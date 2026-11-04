//! 「动态会签（征询模式）」＋「已办撤回」**门面侧**端到端格——移植 jeeflow-java
//! `test/DynamicCountersignTest.java` 的八格语义（发起分流／闸门／撤回接口／detail 投影
//! 这四件落在门面，只有经 `facade.flow` 才照得出来）。
//!
//! 引擎侧的承重柱（成员行身份判据、99 跳过、推进闸、解析期禁配）在
//! `jeeflow-core/tests/dynamic_countersign.rs`；已办撤回的九格在
//! `jeeflow-core/tests/recall.rs`；本文件补的是**门面出口那一层**：信封形状、逐字 msg、
//! 参数硬必填、detail 投影的 ext 键。
//!
//! 契约正文：spec/04「动态会签（征询模式）」「已办撤回」、spec/06 §2.7＋
//! §processTask/execute／§processTask/recall／§processTask/revokeDynamicCountersign。
//! 参考实现基准（逐字权威）：`jeeflow-java@09c2afe` 的
//! `facade/JeeflowFacade.tryDynamicCountersign / gateDynamicCountersign /
//! taskRevokeCountersign / taskRecall` ＋ detail 投影进 `tExt` 那一刀。
//!
//! 夹具纪律：全部内联 JSON，不碰共享 `flows/`。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::{ProcessDefine, ProcessTask, TaskState};
use jeeflow_core::spi::{ProcessExtRepository, ProcessRepository};
use jeeflow_core::{add_sign, collect, FlowData, JsonValue};
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::Arc;

const SUBMIT_APPLY: i64 = 0;
const SUBMIT_AGREE: i64 = 1;
const SUBMIT_REJECT: i64 = 2;

const COLLECT_ON: &str = r#""dynamicCountersign":true"#;

// ═══ 工装 ═══

fn make_facade() -> (JeeflowFacade, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    (JeeflowFacade::new(ctx), repo)
}

fn chain(review_props: &str) -> String {
    let r = if review_props.is_empty() {
        String::new()
    } else {
        format!(",{}", review_props)
    };
    format!(
        r#"{{
            "name": "collect-facade",
            "displayName": "动态会签门面夹具",
            "type": "approval",
            "nodes": [
                {{"id":"start","type":"snaker:start","properties":{{}}}},
                {{"id":"apply","type":"snaker:task","properties":{{"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"}}}},
                {{"id":"review","type":"snaker:task","properties":{{"form":"form-r","assignee":"userA","taskType":0,"performType":0{r}}}}},
                {{"id":"final","type":"snaker:task","properties":{{"form":"form-f","assignee":"userF","taskType":0,"performType":0}}}},
                {{"id":"end","type":"snaker:end","properties":{{}}}}
            ],
            "edges": [
                {{"id":"e1","type":"snaker:transition","sourceNodeId":"start","targetNodeId":"apply","properties":{{}}}},
                {{"id":"e2","type":"snaker:transition","sourceNodeId":"apply","targetNodeId":"review","properties":{{}}}},
                {{"id":"e3","type":"snaker:transition","sourceNodeId":"review","targetNodeId":"final","properties":{{}}}},
                {{"id":"e4","type":"snaker:transition","sourceNodeId":"final","targetNodeId":"end","properties":{{}}}}
            ]
        }}"#
    )
}

fn save_define(repo: &Arc<MemoryRepository>, name: &str, content: &str) -> i64 {
    let mut define = ProcessDefine {
        id: 0,
        name: name.into(),
        display_name: name.into(),
        define_type: "approval".into(),
        state: 1,
        content: content.as_bytes().to_vec(),
        version: 1,
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    repo.save_define(&mut define).unwrap();
    define.id
}

fn doing_named(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> Option<ProcessTask> {
    repo.find_doing_tasks(iid, &[])
        .unwrap()
        .into_iter()
        .find(|t| t.task_name == name)
}

fn doing_count(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> usize {
    repo.find_doing_tasks(iid, &[])
        .unwrap()
        .into_iter()
        .filter(|t| t.task_name == name)
        .count()
}

fn task_row(repo: &Arc<MemoryRepository>, tid: i64) -> ProcessTask {
    repo.find_task_by_id(tid).unwrap().unwrap()
}

fn rows(repo: &Arc<MemoryRepository>, iid: i64) -> Vec<ProcessTask> {
    repo.find_history_tasks(iid).unwrap()
}

/// 发起并办结申请节点（引擎原路径，与 java 测试夹具同位），返回停在 review 的实例 id。
async fn start_to_review(
    facade: &JeeflowFacade,
    repo: &Arc<MemoryRepository>,
    define_id: i64,
) -> i64 {
    let inst = facade
        .engine()
        .start_sync(define_id, "applicant", &FlowData::new())
        .unwrap();
    for t in repo
        .find_doing_tasks(inst.instance_id, &[])
        .unwrap()
        .into_iter()
        .filter(|t| t.task_name == "apply")
    {
        let mut a = HashMap::new();
        a.insert("processTaskId".to_string(), json!(t.task_id.to_string()));
        a.insert("operator".to_string(), json!("applicant"));
        a.insert("submitType".to_string(), json!(SUBMIT_APPLY));
        let resp = facade.flow("processTask/execute", &a).await;
        assert_eq!(resp["code"], 0, "申请办结应成功：{}", resp);
    }
    inst.instance_id
}

fn exec_args(task_id: i64, operator: &str, submit_type: i64) -> HashMap<String, Json> {
    let mut a = HashMap::new();
    a.insert("processTaskId".to_string(), json!(task_id.to_string()));
    a.insert("operator".to_string(), json!(operator));
    a.insert("submitType".to_string(), json!(submit_type));
    a
}

/// 经门面发起动态会签（＝execute 带 `tf_addSignOperators`，两形归一走数组档）。
fn collect_args(
    task_id: i64,
    operator: &str,
    operators: &[&str],
    reason: Option<&str>,
) -> HashMap<String, Json> {
    let mut a = exec_args(task_id, operator, SUBMIT_AGREE);
    a.insert(
        add_sign::ADD_SIGN_OPERATORS.to_string(),
        json!(operators.iter().map(|s| s.to_string()).collect::<Vec<_>>()),
    );
    if let Some(r) = reason {
        a.insert("tf_addSignReason".to_string(), json!(r));
    }
    a
}

/// 带拉人参数的任意 submitType（成员行禁拉人那一格用）。
fn exec_args_with_pull(
    task_id: i64,
    operator: &str,
    submit_type: i64,
    operators: &[&str],
) -> HashMap<String, Json> {
    let mut a = exec_args(task_id, operator, submit_type);
    a.insert(
        add_sign::ADD_SIGN_OPERATORS.to_string(),
        json!(operators.iter().map(|s| s.to_string()).collect::<Vec<_>>()),
    );
    a
}

fn revoke_args(task_id: i64, operator: &str, reason: Option<&str>) -> HashMap<String, Json> {
    let mut a = HashMap::new();
    a.insert("processTaskId".to_string(), json!(task_id.to_string()));
    a.insert("operator".to_string(), json!(operator));
    if let Some(r) = reason {
        a.insert("reason".to_string(), json!(r));
    }
    a
}

fn recall_args(task_id: Option<i64>, operator: Option<&str>) -> HashMap<String, Json> {
    let mut a = HashMap::new();
    if let Some(id) = task_id {
        a.insert("processTaskId".to_string(), json!(id.to_string()));
    }
    if let Some(op) = operator {
        a.insert("operator".to_string(), json!(op));
    }
    a
}

fn ledger(row: &ProcessTask) -> Vec<JsonValue> {
    match row.variables.get(add_sign::ADD_SIGN_HISTORY) {
        Some(JsonValue::Array(items)) => items.clone(),
        _ => Vec::new(),
    }
}

fn entry(hist: &JsonValue, key: &str) -> JsonValue {
    hist.as_object()
        .and_then(|entries| entries.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()))
        .unwrap_or_else(|| panic!("账本条目缺键 {}: {:?}", key, hist))
}

/// 引擎原路径办结（成员行提交意见走这条，与 java 测试同位：门面闸门只管主办行）。
fn submit_opinion(facade: &JeeflowFacade, member: &ProcessTask, actor: &str, comment: Option<&str>) {
    let mut fd = FlowData::new();
    fd.insert_i64("submitType", SUBMIT_AGREE);
    if let Some(c) = comment {
        fd.insert_str("tf_approvalComment", c);
    }
    facade
        .engine()
        .execute_task_sync(member.task_id, actor, &fd)
        .unwrap();
}

// ═══ 用例 ═══

/// 格 1（正向）：发起不办结主办行、成员行三戳齐、账本含 reason、响应 marker。
#[tokio::test]
async fn collect01_submit_keeps_host_doing_and_stamps_member_rows() {
    let (facade, repo) = make_facade();
    let define_id = save_define(&repo, "collect-01", &chain(COLLECT_ON));
    let iid = start_to_review(&facade, &repo, define_id).await;
    let host = doing_named(&repo, iid, "review").unwrap();

    let resp = facade
        .flow(
            "processTask/execute",
            &collect_args(host.task_id, "userA", &["userX", "userY"], Some("涉及地规划需两部门意见")),
        )
        .await;
    assert_eq!(resp["code"], 0, "{}", resp);
    let dc = &resp["data"]["dynamicCountersign"];
    assert_eq!(dc["round"], 1, "首轮");
    assert_eq!(dc["total"], 2, "两条成员行在册");
    assert_eq!(dc["submitted"], 0, "一条都还没答");

    // 主办行仍 DOING（征询模式拉人不办结主办）
    let host_row = task_row(&repo, host.task_id);
    assert!(host_row.is_doing(), "主办行必须仍 DOING：拍板权留在主办");
    assert_eq!(3, doing_count(&repo, iid, "review"), "主办＋两条成员行");
    assert!(
        doing_named(&repo, iid, "final").is_none(),
        "拉人这一步不流转令牌"
    );

    // 成员行三戳：同节点同名、parent＝主办行、轮次＋reason
    let members = collect::member_rows(&rows(&repo, iid), &host_row);
    assert_eq!(2, members.len());
    for m in &members {
        assert!(m.is_doing());
        assert_eq!(Some(host.task_id), m.parent_task_id);
        assert_eq!(
            "review",
            m.task_name,
            "成员行与主办行同节点（承重判据 1 的身份锚）"
        );
        assert_eq!(
            Some(1),
            m.variables.get_i64(&add_sign::round_key("review")),
            "轮次戳 addSignRound_review"
        );
        assert_eq!(
            Some("涉及地规划需两部门意见"),
            m.variables.get_str(collect::KEY_ADD_SIGN_REASON),
            "reason 盖到每条成员行（单行出证自足）"
        );
        assert_eq!(0, m.perform_type, "成员行是普通任务行，不是会签");
    }

    // 账本含 reason、只一条，且落在**主办行**上
    let lg = ledger(&task_row(&repo, host.task_id));
    assert_eq!(1, lg.len(), "只追加一条");
    assert_eq!("涉及地规划需两部门意见", entry(&lg[0], "reason").as_str().unwrap());
    assert_eq!("userA", entry(&lg[0], "fromActor").as_str().unwrap());
    assert_eq!("userA", entry(&lg[0], "operator").as_str().unwrap());
    assert_eq!(1.0, entry(&lg[0], "round").as_f64().unwrap());
    assert_eq!(
        vec!["userX", "userY"],
        entry(&lg[0], "toActors")
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>()
    );
    // 主办行同步盖轮次戳（java `host.getVariables().put(ADD_SIGN_ROUND_…, round)`）
    assert_eq!(
        Some(1),
        task_row(&repo, host.task_id)
            .variables
            .get_i64(&add_sign::round_key("review"))
    );
}

/// 格 2（闸门）：未收齐时主办按任意 submitType 办结 ⇒ 逐字文案且**零写入三面**。
#[tokio::test]
async fn collect02_gate_blocks_all_submit_types_until_collected() {
    let (facade, repo) = make_facade();
    let define_id = save_define(&repo, "collect-02", &chain(COLLECT_ON));
    let iid = start_to_review(&facade, &repo, define_id).await;
    let host = doing_named(&repo, iid, "review").unwrap();
    facade
        .flow(
            "processTask/execute",
            &collect_args(host.task_id, "userA", &["userX", "userY"], None),
        )
        .await;

    let host_row = task_row(&repo, host.task_id);
    let members = collect::member_rows(&rows(&repo, iid), &host_row)
        .iter()
        .map(|m| m.task_id)
        .collect::<Vec<_>>();
    let rows_before = rows(&repo, iid).len();

    for submit_type in [SUBMIT_AGREE, SUBMIT_REJECT, 3, 4, 6, 20] {
        let resp = facade
            .flow("processTask/execute", &exec_args(host.task_id, "userA", submit_type))
            .await;
        assert_eq!(resp["code"], 99999999, "submitType={} 应被挡：{}", submit_type, resp);
        assert_eq!(
            "节点[review]动态会签意见未收齐（已提交 0/2），不能办理",
            resp["msg"].as_str().unwrap(),
            "挡全部 submitType（含驳回），逐字文案"
        );
    }

    // 零写入三面：目标行状态不变、任务行总数不变、成员行一条都没被办结
    assert!(task_row(&repo, host.task_id).is_doing(), "面①：主办仍 DOING");
    assert_eq!(rows_before, rows(&repo, iid).len(), "面②：没有多出行");
    assert_eq!(3, doing_count(&repo, iid, "review"), "面③：两条成员行仍待答");
    for mid in &members {
        assert!(
            task_row(&repo, *mid).is_doing(),
            "六次报错不得有任何一次把成员行办结"
        );
    }
    assert!(doing_named(&repo, iid, "final").is_none(), "令牌仍停留本节点");
}

/// 格 3（收齐后拍板＋投影）：`ext.dynamicCountersign` 的 active 随提交翻转。
#[tokio::test]
async fn collect03_host_proceeds_after_all_submitted_with_projection() {
    let (facade, repo) = make_facade();
    let define_id = save_define(&repo, "collect-03", &chain(COLLECT_ON));
    let iid = start_to_review(&facade, &repo, define_id).await;
    let host = doing_named(&repo, iid, "review").unwrap();
    facade
        .flow(
            "processTask/execute",
            &collect_args(host.task_id, "userA", &["userX", "userY"], None),
        )
        .await;

    let host_row = task_row(&repo, host.task_id);
    let members = collect::member_rows(&rows(&repo, iid), &host_row);
    assert_eq!(2, members.len());

    // 只交第一条 ⇒ active=true、1/2
    submit_opinion(&facade, &members[0], members[0].actor_ids[0].as_str(), Some("甲部门意见"));
    let detail = facade
        .flow(
            "processTask/detail",
            &{
                let mut a = HashMap::new();
                a.insert("id".to_string(), json!(host.task_id.to_string()));
                a
            },
        )
        .await;
    assert_eq!(detail["code"], 0, "{}", detail);
    let dc = &detail["data"]["ext"]["dynamicCountersign"];
    assert_eq!(dc["active"], true, "还有未答 ⇒ active=true（前端据此置灰）");
    assert_eq!(dc["total"], 2);
    assert_eq!(dc["submitted"], 1);
    assert_eq!(dc["actors"].as_array().unwrap().len(), 2, "actors 逐行给状态");
    let actor_states: Vec<i64> = dc["actors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["state"].as_i64().unwrap())
        .collect();
    assert!(
        actor_states.contains(&(TaskState::Finished.code() as i64))
            && actor_states.contains(&(TaskState::Doing.code() as i64)),
        "一条已交一条待答：{:?}",
        actor_states
    );

    // 交第二条 ⇒ active=false、2/2，闸门开
    submit_opinion(&facade, &members[1], members[1].actor_ids[0].as_str(), Some("乙部门意见"));
    let detail = facade
        .flow(
            "processTask/detail",
            &{
                let mut a = HashMap::new();
                a.insert("id".to_string(), json!(host.task_id.to_string()));
                a
            },
        )
        .await;
    let dc = &detail["data"]["ext"]["dynamicCountersign"];
    assert_eq!(dc["active"], false);
    assert_eq!(dc["total"], 2);
    assert_eq!(dc["submitted"], 2);
    assert_eq!(dc["round"], 1, "投影的 round 读主办行账本末条");

    // 主办拍板 ⇒ 推进
    let resp = facade
        .flow("processTask/execute", &exec_args(host.task_id, "userA", SUBMIT_AGREE))
        .await;
    assert_eq!(resp["code"], 0, "{}", resp);
    assert!(doing_named(&repo, iid, "final").is_some(), "令牌流出本节点");
}

/// 格 4（三拦）：成员行携带拉人参数、重复拉未答的人、加给自己。
#[tokio::test]
async fn collect04_member_pull_duplicate_and_self_guards() {
    let (facade, repo) = make_facade();
    let define_id = save_define(&repo, "collect-04", &chain(COLLECT_ON));
    let iid = start_to_review(&facade, &repo, define_id).await;
    let host = doing_named(&repo, iid, "review").unwrap();
    facade
        .flow(
            "processTask/execute",
            &collect_args(host.task_id, "userA", &["userX"], None),
        )
        .await;

    let host_row = task_row(&repo, host.task_id);
    let member = collect::member_rows(&rows(&repo, iid), &host_row)
        .into_iter()
        .next()
        .unwrap();

    // ① 成员行拉人 ⇒ 闸门报它的逐字文案（分流腿对成员行不接管）
    let resp = facade
        .flow(
            "processTask/execute",
            &exec_args_with_pull(member.task_id, "userX", SUBMIT_AGREE, &["userZ"]),
        )
        .await;
    assert_eq!("动态会签成员任务不能发起加签", resp["msg"].as_str().unwrap());
    assert_eq!(resp["code"], 99999999);
    assert!(task_row(&repo, member.task_id).is_doing(), "报错即零写入");

    // ② 重复拉仍在待答的那位
    let resp = facade
        .flow(
            "processTask/execute",
            &collect_args(host.task_id, "userA", &["userX"], None),
        )
        .await;
    assert_eq!(
        "操作人[userX]已在节点[review]的动态会签中",
        resp["msg"].as_str().unwrap()
    );
    assert_eq!(2, doing_count(&repo, iid, "review"), "重复拉不得多建一行");

    // ③ 加给自己
    let resp = facade
        .flow(
            "processTask/execute",
            &collect_args(host.task_id, "userA", &["userA"], None),
        )
        .await;
    assert_eq!(
        "不能把任务加签给自己",
        resp["msg"].as_str().unwrap()
    );

    // ④ 互斥：与 tf_nextNodeOperator 同传＝死参数，报错不静默
    let mut a = collect_args(host.task_id, "userA", &["userW"], None);
    a.insert("tf_nextNodeOperator".to_string(), json!("userQ"));
    let resp = facade.flow("processTask/execute", &a).await;
    assert_eq!(
        "tf_addSignOperators 与 tf_nextNodeOperator 互斥，不能同时提交",
        resp["msg"].as_str().unwrap()
    );

    // ⑤ 非主办行参与人拉人 ⇒ java 抛 WfErrEnum.NOT_ALLOWED_EXECUTE 的逐字串
    let resp = facade
        .flow(
            "processTask/execute",
            &collect_args(host.task_id, "userNobody", &["userW"], None),
        )
        .await;
    assert_eq!(
        "当前参与者不能执行该流程任务",
        resp["msg"].as_str().unwrap()
    );
    assert_eq!(2, doing_count(&repo, iid, "review"), "五道拦全零写入");
}

/// 格 5（撤回接口）：未答废弃、已交保留、账本记撤回、闸门恢复；重复撤回与非参与者各归各文案。
#[tokio::test]
async fn collect05_revoke_restores_gate_and_keeps_submitted_opinions() {
    let (facade, repo) = make_facade();
    let define_id = save_define(&repo, "collect-05", &chain(COLLECT_ON));
    let iid = start_to_review(&facade, &repo, define_id).await;
    let host = doing_named(&repo, iid, "review").unwrap();
    facade
        .flow(
            "processTask/execute",
            &collect_args(host.task_id, "userA", &["userX", "userY"], None),
        )
        .await;

    let host_row = task_row(&repo, host.task_id);
    let members = collect::member_rows(&rows(&repo, iid), &host_row);
    let x_row = members
        .iter()
        .find(|m| m.actor_ids.contains(&"userX".to_string()))
        .unwrap()
        .clone();
    let y_row = members
        .iter()
        .find(|m| m.actor_ids.contains(&"userY".to_string()))
        .unwrap()
        .clone();
    submit_opinion(&facade, &x_row, "userX", Some("甲部门意见"));

    // 非参与者先拦一次（承重判据 3：按名册判，不是按 DOING 行的 is_allowed）
    let resp = facade
        .flow(
            "processTask/revokeDynamicCountersign",
            &revoke_args(host.task_id, "userNobody", None),
        )
        .await;
    assert_eq!(
        "无权限撤回该动态会签",
        resp["msg"].as_str().unwrap()
    );
    assert!(task_row(&repo, y_row.task_id).is_doing(), "报错即零写入");

    let resp = facade
        .flow(
            "processTask/revokeDynamicCountersign",
            &revoke_args(host.task_id, "userA", Some("部门选错重新拉")),
        )
        .await;
    assert_eq!(resp["code"], 0, "{}", resp);
    assert_eq!(1, resp["data"]["revoked"].as_i64().unwrap(), "只撤未答那一条");
    assert_eq!(
        vec!["userY"],
        resp["data"]["actors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect::<Vec<_>>()
    );
    assert!(
        task_row(&repo, x_row.task_id).is_finished(),
        "已提交意见行原样保留（审计凭证不动）"
    );
    assert_eq!(
        TaskState::Abandon.code(),
        task_row(&repo, y_row.task_id).task_state,
        "未答行废弃 99"
    );

    // 账本两条（发起＋撤回），撤回条带 revoked 标记；轮次不消耗
    let lg = ledger(&task_row(&repo, host.task_id));
    assert_eq!(2, lg.len());
    assert_eq!(JsonValue::Bool(true), entry(&lg[1], "revoked"));
    assert_eq!("部门选错重新拉", entry(&lg[1], "reason").as_str().unwrap());
    assert_eq!(1.0, entry(&lg[1], "round").as_f64().unwrap(), "撤回不推进轮次");
    assert_eq!(
        vec!["userY"],
        entry(&lg[1], "toActors")
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>()
    );

    // 闸门自然恢复 ⇒ 主办直接拍板推进（99 行不在册、也不算未答）
    let resp = facade
        .flow("processTask/execute", &exec_args(host.task_id, "userA", SUBMIT_AGREE))
        .await;
    assert_eq!(resp["code"], 0, "{}", resp);
    assert!(doing_named(&repo, iid, "final").is_some());

    // 收齐／已撤干净 ⇒ 本 action 报错；投影里 99 行不进分母
    let again = facade
        .flow(
            "processTask/revokeDynamicCountersign",
            &revoke_args(host.task_id, "userA", None),
        )
        .await;
    assert_eq!(
        "节点[review]没有进行中的动态会签",
        again["msg"].as_str().unwrap()
    );
    let detail = facade
        .flow(
            "processTask/detail",
            &{
                let mut a = HashMap::new();
                a.insert("id".to_string(), json!(host.task_id.to_string()));
                a
            },
        )
        .await;
    let dc = &detail["data"]["ext"]["dynamicCountersign"];
    assert_eq!(dc["total"], 1, "撤回清场的那行不进分母");
    assert_eq!(dc["submitted"], 1);
    assert_eq!(dc["active"], false);
}

/// 格 6（多轮征询）：跨轮意见互不覆盖、轮次递增、收齐判据跨轮计数。
#[tokio::test]
async fn collect06_multi_round_collect_counts_across_rounds() {
    let (facade, repo) = make_facade();
    let define_id = save_define(&repo, "collect-06", &chain(COLLECT_ON));
    let iid = start_to_review(&facade, &repo, define_id).await;
    let host = doing_named(&repo, iid, "review").unwrap();

    let r1 = facade
        .flow(
            "processTask/execute",
            &collect_args(host.task_id, "userA", &["userX"], Some("第一轮")),
        )
        .await;
    assert_eq!(r1["data"]["dynamicCountersign"]["round"], 1);
    assert_eq!(r1["data"]["dynamicCountersign"]["total"], 1);

    let host_row = task_row(&repo, host.task_id);
    let x_row = collect::member_rows(&rows(&repo, iid), &host_row)
        .into_iter()
        .next()
        .unwrap();
    submit_opinion(&facade, &x_row, "userX", Some("甲意见"));

    // 闸门已开时仍可再拉一轮
    let r2 = facade
        .flow(
            "processTask/execute",
            &collect_args(host.task_id, "userA", &["userY"], Some("第二轮")),
        )
        .await;
    assert_eq!(r2["data"]["dynamicCountersign"]["round"], 2, "轮次递增");
    assert_eq!(r2["data"]["dynamicCountersign"]["total"], 2, "跨轮一起在册");
    assert_eq!(r2["data"]["dynamicCountersign"]["submitted"], 1);

    // 跨轮收齐判据：x 已交、y 未交 ⇒ 1/2
    let resp = facade
        .flow("processTask/execute", &exec_args(host.task_id, "userA", SUBMIT_AGREE))
        .await;
    assert_eq!(
        "节点[review]动态会签意见未收齐（已提交 1/2），不能办理",
        resp["msg"].as_str().unwrap()
    );

    // 只办结仍待答的那条（第 1 轮已提交的行不重复办结）
    let host_row = task_row(&repo, host.task_id);
    for m in collect::member_rows(&rows(&repo, iid), &host_row) {
        if m.is_doing() {
            let actor = m.actor_ids.first().cloned().unwrap();
            submit_opinion(&facade, &m, &actor, None);
        }
    }
    let resp = facade
        .flow("processTask/execute", &exec_args(host.task_id, "userA", SUBMIT_AGREE))
        .await;
    assert_eq!(resp["code"], 0, "{}", resp);
    assert!(doing_named(&repo, iid, "final").is_some());

    // 跨轮意见互不覆盖：两行各自留自己的 reason
    let host_row = task_row(&repo, host.task_id);
    let mut reasons: Vec<Option<String>> = collect::member_rows(&rows(&repo, iid), &host_row)
        .iter()
        .map(|m| {
            m.variables
                .get_str(collect::KEY_ADD_SIGN_REASON)
                .map(|s| s.to_string())
        })
        .collect();
    reasons.sort();
    assert_eq!(
        vec![Some("第一轮".to_string()), Some("第二轮".to_string())],
        reasons
    );
    assert_eq!(2, ledger(&host_row).len(), "两轮各一条");
}

/// 格 7（`processTask/recall` 端到端）：信封两键＋参数硬必填＋逐字 msg 经门面出口。
#[tokio::test]
async fn collect07_recall_action_envelope_and_required_args() {
    let (facade, repo) = make_facade();
    let define_id = save_define(&repo, "recall-facade", &chain(r#""allowRecall":true"#));
    let iid = start_to_review(&facade, &repo, define_id).await;

    // operator 硬必填（缺省不得回落 user1，否则撤回人静默记成别人）
    let resp = facade
        .flow("processTask/recall", &recall_args(None, None))
        .await;
    assert_eq!("operator 必填", resp["msg"].as_str().unwrap(), "位序：operator 空判在前");
    assert_eq!(resp["code"], 99999999);

    // 主键档缺失响亮报错，绝不拿 ''/0 当 id 落库
    let resp = facade
        .flow("processTask/recall", &recall_args(None, Some("userA")))
        .await;
    assert_eq!("processTaskId 缺失", resp["msg"].as_str().unwrap());

    let review = doing_named(&repo, iid, "review").unwrap();
    facade
        .flow("processTask/execute", &exec_args(review.task_id, "userA", SUBMIT_AGREE))
        .await;
    let old_review = finished_row_named(&repo, iid, "review").unwrap();

    let resp = facade
        .flow("processTask/recall", &recall_args(Some(old_review.task_id), Some("userA")))
        .await;
    assert_eq!(resp["code"], 0, "{}", resp);
    assert_eq!(
        "review",
        task_row(&repo, old_review.task_id).task_name
    );
    // 本栈出口对 `*Id` 键一律 stringify（C1/C14 全栈约定，防雪花 id 过 JS 精度丢失）
    let resurrected_id: i64 = resp["data"]["processTaskId"]
        .as_str()
        .unwrap_or_else(|| panic!("processTaskId 应为字符串档：{}", resp["data"]))
        .parse::<i64>()
        .unwrap();
    assert_eq!(
        TaskState::Doing.code() as i64,
        resp["data"]["taskState"].as_i64().unwrap(),
        "信封第二键给复活行状态"
    );
    assert_eq!(1, doing_count(&repo, iid, "review"));
    assert_eq!(Some(resurrected_id), doing_named(&repo, iid, "review").map(|t| t.task_id));
    assert_eq!(
        TaskState::Abandon.code(),
        any_row_named(&repo, iid, "final").unwrap().task_state
    );

    // 目标行非 FINISHED（复活行仍待办）⇒ 逐字 msg 经门面透出
    let resp = facade
        .flow("processTask/recall", &recall_args(Some(resurrected_id), Some("userA")))
        .await;
    assert_eq!(
        "任务非已完成状态，不可撤回",
        resp["msg"].as_str().unwrap()
    );
}

fn finished_row_named(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> Option<ProcessTask> {
    rows(repo, iid).into_iter().find(|t| t.task_name == name && t.is_finished())
}

fn any_row_named(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> Option<ProcessTask> {
    rows(repo, iid).into_iter().find(|t| t.task_name == name)
}

/// 格 8（`processTask/revokeDynamicCountersign` 参数档）：硬必填三档与逐字 msg。
#[tokio::test]
async fn collect08_revoke_action_required_args() {
    let (facade, repo) = make_facade();
    let define_id = save_define(&repo, "collect-08", &chain(COLLECT_ON));
    let iid = start_to_review(&facade, &repo, define_id).await;
    let host = doing_named(&repo, iid, "review").unwrap();

    let resp = facade
        .flow("processTask/revokeDynamicCountersign", &revoke_args(host.task_id, "", None))
        .await;
    assert_eq!("operator 必填", resp["msg"].as_str().unwrap(), "空串也算没填");

    let resp = facade
        .flow(
            "processTask/revokeDynamicCountersign",
            &recall_args(Some(host.task_id), Some("userA")),
        )
        .await;
    assert_eq!(
        "节点[review]没有进行中的动态会签",
        resp["msg"].as_str().unwrap(),
        "还没拉过人 ⇒ 无待答成员行；操作人就是主办自己，报的必须是这一档"
    );

    facade
        .flow(
            "processTask/execute",
            &collect_args(host.task_id, "userA", &["userX"], None),
        )
        .await;
    let resp = facade
        .flow(
            "processTask/revokeDynamicCountersign",
            &revoke_args(999_999, "userA", None),
        )
        .await;
    assert_eq!("任务不存在", resp["msg"].as_str().unwrap());
    assert!(
        task_row(&repo, collect::member_rows(&rows(&repo, iid), &task_row(&repo, host.task_id))
            .first()
            .unwrap()
            .task_id)
            .is_doing(),
        "三档参数报错全部零写入：成员行仍待答"
    );
}
