//! issues/257 门面定向 T0：顶层 startAndExecute 与子流程自办复用同一核心腿。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::{ProcessDefine, TaskState};
use jeeflow_core::spi::{ProcessExtRepository, ProcessRepository};
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

fn node(id: &str, kind: &str, props: Value) -> Value {
    json!({"id": id, "type": format!("snaker:{kind}"), "properties": props,
        "text": {"value": id}})
}

fn edge(id: &str, source: &str, target: &str) -> Value {
    json!({"id": id, "sourceNodeId": source, "targetNodeId": target, "properties": {}})
}

fn linear(name: &str, specs: &[(&str, &str, Value)]) -> String {
    let nodes: Vec<Value> = specs.iter()
        .map(|(id, kind, props)| node(id, kind, props.clone())).collect();
    let edges: Vec<Value> = specs.windows(2).enumerate()
        .map(|(i, pair)| edge(&format!("e{i}"), pair[0].0, pair[1].0)).collect();
    json!({"name": name, "displayName": name, "type": "approval",
        "nodes": nodes, "edges": edges}).to_string()
}

fn save_define(repo: &Arc<MemoryRepository>, name: &str, content: String) -> i64 {
    let mut define = ProcessDefine {
        id: 0,
        name: name.into(),
        display_name: name.into(),
        define_type: "approval".into(),
        state: 1,
        content: content.into_bytes(),
        version: 1,
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    repo.save_define(&mut define).unwrap();
    define.id
}

fn as_id(value: &Value) -> i64 {
    value.as_i64().or_else(|| value.as_str().and_then(|s| s.parse().ok())).unwrap()
}

#[tokio::test]
async fn i257_facade_start_and_execute_auto_applies_child_once() {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    let facade = JeeflowFacade::new(ctx);

    let child = save_define(&repo, "childFacade257", linear("childFacade257", &[
        ("start", "start", json!({})),
        ("apply", "task", json!({"assignee": "applicant"})),
        ("childReview", "task", json!({"assignee": "leader"})),
        ("end", "end", json!({})),
    ]));
    let parent = save_define(&repo, "parentFacade257", linear("parentFacade257", &[
        ("start", "start", json!({})),
        ("apply", "task", json!({"assignee": "applicant"})),
        ("sub", "subProcess", json!({"processName": "childFacade257", "version": 1, "form": "wrong"})),
        ("parentAfter", "task", json!({"assignee": "manager"})),
        ("end", "end", json!({})),
    ]));

    let mut start = HashMap::new();
    start.insert("processDefineId".into(), json!(parent));
    start.insert("operator".into(), json!("applicant"));
    let response = facade.flow("processDefine/startAndExecute", &start).await;
    assert_eq!(response["code"], 0, "startAndExecute 失败: {response}");
    let parent_id = as_id(&response["data"]["processInstanceId"]);
    let children: Vec<_> = repo.get_all_instances().unwrap().into_iter()
        .filter(|i| i.parent_id == Some(parent_id)).collect();
    assert_eq!(children.len(), 1);
    let child_id = children[0].instance_id;
    assert_eq!(children[0].parent_node_name.as_deref(), Some("sub"));
    assert_eq!(children[0].define_id, child);

    let child_apply = repo.find_done_tasks(child_id, &["apply".to_string()]).unwrap();
    assert_eq!(child_apply.len(), 1);
    assert_eq!(child_apply[0].task_state, TaskState::Finished.code());
    assert_eq!(child_apply[0].actor_id.as_deref(), Some("flow.auto"));
    assert!(repo.find_task_actors(child_apply[0].task_id).unwrap().contains(&"applicant".into()));
    let child_review = repo.find_doing_tasks(child_id, &[]).unwrap();
    assert_eq!(child_review.len(), 1);
    assert_eq!(child_review[0].task_name, "childReview");
    assert!(repo.find_doing_tasks(parent_id, &["parentAfter".to_string()]).unwrap().is_empty());

    let mut execute = HashMap::new();
    execute.insert("processTaskId".into(), json!(child_review[0].task_id));
    execute.insert("operator".into(), json!("leader"));
    execute.insert("submitType".into(), json!(1));
    let response = facade.flow("processTask/execute", &execute).await;
    assert_eq!(response["code"], 0, "子任务办理失败: {response}");
    let parent_after = repo.find_doing_tasks(parent_id, &["parentAfter".to_string()]).unwrap();
    assert_eq!(parent_after.len(), 1, "子办结应回父推进一次");
}
