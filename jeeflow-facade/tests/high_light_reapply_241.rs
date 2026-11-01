//! issues/241 · `highLight` 重放腿在决策节点必须按**任务行佐证**选边，不得按重提后的
//! 实例变量重算（直返契约：来源节点之前的条件分支不重算）。
//!
//! 夹具：start → apply → fork →(taskA / taskB)→ join → decision1{amount>1000 → taskHigh(直返) |
//! amount<=1000 → taskLow} → end。走单：6666 走 taskHigh → 退回发起人 → 改为 666 重提 →
//! 新待办仍落 taskHigh。高亮必须只点亮实际走过的 e7 侧，不得把 taskLow/e8/e10 算进历史。
//!
//! 决策节点的入边来源刻意放成 join（网关不产任务行）：任务行快照实例变量的栈
//! 在「任务行前置」夹具上会被旧的最老行合并偶然救活，join 入边让那条通道失效。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::spi::{ProcessExtRepository, ProcessRepository};
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::Arc;

const FLOW: &str = r#"{
  "name": "i241-return-highlight-join", "displayName": "直返重提高亮（汇合后分支）", "type": "approval",
  "nodes": [
    {"id": "start", "type": "snaker:start", "text": {"value": "开始"}},
    {"id": "apply", "type": "snaker:task", "text": {"value": "发起申请"},
     "properties": {"assignee": "applicant", "taskType": 0, "performType": 0}},
    {"id": "fork1", "type": "snaker:fork", "text": {"value": "分叉"}, "properties": {}},
    {"id": "taskA", "type": "snaker:task", "text": {"value": "分支A"},
     "properties": {"assignee": "userA", "taskType": 0, "performType": 0}},
    {"id": "taskB", "type": "snaker:task", "text": {"value": "分支B"},
     "properties": {"assignee": "userB", "taskType": 0, "performType": 0}},
    {"id": "join1", "type": "snaker:join", "text": {"value": "汇合"}, "properties": {}},
    {"id": "decision1", "type": "snaker:decision", "text": {"value": "金额分支"},
     "properties": {"expr": "f_amount > 1000"}},
    {"id": "taskHigh", "type": "snaker:task", "text": {"value": "高额审批"},
     "properties": {"assignee": "director", "taskType": 0, "performType": 0, "reapplyMode": "RETURN_TO_SOURCE"}},
    {"id": "taskLow", "type": "snaker:task", "text": {"value": "普通审批"},
     "properties": {"assignee": "manager", "taskType": 0, "performType": 0}},
    {"id": "end", "type": "snaker:end", "text": {"value": "结束"}}
  ],
  "edges": [
    {"id": "e0", "sourceNodeId": "start", "targetNodeId": "apply", "properties": {}},
    {"id": "e1", "sourceNodeId": "apply", "targetNodeId": "fork1", "properties": {}},
    {"id": "e2", "sourceNodeId": "fork1", "targetNodeId": "taskA", "properties": {}},
    {"id": "e3", "sourceNodeId": "fork1", "targetNodeId": "taskB", "properties": {}},
    {"id": "e4", "sourceNodeId": "taskA", "targetNodeId": "join1", "properties": {}},
    {"id": "e5", "sourceNodeId": "taskB", "targetNodeId": "join1", "properties": {}},
    {"id": "e6", "sourceNodeId": "join1", "targetNodeId": "decision1", "properties": {}},
    {"id": "e7", "sourceNodeId": "decision1", "targetNodeId": "taskHigh", "properties": {"expr": "f_amount > 1000"}},
    {"id": "e8", "sourceNodeId": "decision1", "targetNodeId": "taskLow", "properties": {"expr": "f_amount <= 1000"}},
    {"id": "e9", "sourceNodeId": "taskHigh", "targetNodeId": "end", "properties": {}},
    {"id": "e10", "sourceNodeId": "taskLow", "targetNodeId": "end", "properties": {}}
  ]
}"#;

fn setup() -> (JeeflowFacade, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
    (JeeflowFacade::new(ctx), repo)
}

fn args(pairs: &[(&str, Json)]) -> HashMap<String, Json> {
    pairs.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect()
}

async fn doing_task(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> i64 {
    repo.find_doing_tasks(iid, &[])
        .unwrap()
        .into_iter()
        .find(|t| t.task_name == name)
        .unwrap_or_else(|| panic!("任务 {name} 应进行中"))
        .task_id
}

fn list_of(hl: &Json, key: &str) -> Vec<String> {
    hl["data"][key]
        .as_array()
        .unwrap_or_else(|| panic!("{key} 应为数组: {hl}"))
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect()
}

#[tokio::test]
async fn i241_reapply_does_not_light_unvisited_branch() {
    let (facade, repo) = setup();
    let mut define = jeeflow_core::model::ProcessDefine {
        id: 0,
        name: "i241-return-highlight-join".into(),
        display_name: "直返重提高亮（汇合后分支）".into(),
        define_type: "approval".into(),
        state: 1,
        content: FLOW.as_bytes().to_vec(),
        version: 1,
        create_time: None,
        create_user: Some("zhangsan".into()),
        update_time: None,
        update_user: None,
    };
    repo.save_define(&mut define).unwrap();
    let started = facade
        .flow(
            "processDefine/startAndExecute",
            &args(&[
                ("processDefineId", json!(define.id)),
                ("operator", json!("zhangsan")),
                ("f_amount", json!(6666)),
            ]),
        )
        .await;
    assert_eq!(started["code"], 0, "发起失败: {started}");
    let iid = started["data"]["processInstanceId"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();

    for (name, actor) in [("taskA", "userA"), ("taskB", "userB")] {
        let tid = doing_task(&repo, iid, name).await;
        let resp = facade
            .flow(
                "processTask/execute",
                &args(&[
                    ("processTaskId", json!(tid)),
                    ("operator", json!(actor)),
                    ("submitType", json!(1)),
                ]),
            )
            .await;
        assert_eq!(resp["code"], 0, "办理 {name} 失败: {resp}");
    }
    let high = doing_task(&repo, iid, "taskHigh").await;
    let ret = facade
        .flow(
            "processTask/execute",
            &args(&[
                ("processTaskId", json!(high)),
                ("operator", json!("director")),
                ("submitType", json!(6)),
            ]),
        )
        .await;
    assert_eq!(ret["code"], 0, "退回发起人失败: {ret}");
    let apply = doing_task(&repo, iid, "apply").await;
    let reapply = facade
        .flow(
            "processTask/execute",
            &args(&[
                ("processTaskId", json!(apply)),
                ("operator", json!("zhangsan")),
                ("submitType", json!(5)),
                ("f_amount", json!(666)),
            ]),
        )
        .await;
    assert_eq!(reapply["code"], 0, "重提失败: {reapply}");
    let _ = doing_task(&repo, iid, "taskHigh").await;

    let hl = facade
        .flow("processInstance/highLight", &args(&[("id", json!(iid))]))
        .await;
    assert_eq!(hl["code"], 0, "highLight 失败: {hl}");
    let edges = list_of(&hl, "historyEdgeNames");
    let nodes = list_of(&hl, "historyNodeNames");
    assert!(
        edges.contains(&"e7".to_string()),
        "汇合后的实际分支 e7 必须点亮（实际 {edges:?}）"
    );
    assert!(!edges.contains(&"e8".to_string()), "未走分支 e8 不得点亮（实际 {edges:?}）");
    assert!(!edges.contains(&"e10".to_string()), "未走分支 e10 不得点亮（实际 {edges:?}）");
    assert!(!nodes.contains(&"taskLow".to_string()), "未走节点 taskLow 不得进历史（实际 {nodes:?}）");
    assert!(!nodes.contains(&"end".to_string()), "未走节点 end 不得进历史（实际 {nodes:?}）");
}
