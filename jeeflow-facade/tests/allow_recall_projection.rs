//! issues/251「已办撤回 allowRecall 快照投影」——**门面出口侧**端到端验收。
//!
//! 引擎盖章侧在 `jeeflow-core/tests/allow_recall_stamp.rs`；本文件补的是**查询出口那一层**：
//! 办理时盖进任务行的 `tf_allowRecall` 快照，经 `processTask/doneList`／`processTask/todoList`／
//! `processInstance/detail` 三个出口搬运成顶层契约键 `allowRecall`（布尔）。
//!
//! 定案形状（出口投影）：对**非 DOING** 行、且行变量含 `tf_allowRecall` 时投顶层布尔 `allowRecall`；
//! 缺键＝不出字段；DOING 行永不投。detail 的 tasks 列表同形。布尔口径对齐 java
//! `Boolean.parseBoolean(String.valueOf(v))`。撤回校验本身不动（仍以现读定义为权威）。
//!
//! 夹具纪律（照 `tests/dynamic_countersign.rs`）：全部内联 JSON，不碰共享 `flows/`；
//! 用例各自用独立 define 名段（`allow-recall-proj-NN`）。
//!
//! 参考实现基准：jeeflow-java `facade/JeeflowFacade.taskRowToMap`（已办行 `ext.get(ALLOW_RECALL)`
//! 在 hasOnlyControlKeys 回退之前搬顶层）＋ detail 投影同形。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::{ProcessDefine, ProcessTask};
use jeeflow_core::recall;
use jeeflow_core::spi::{ProcessExtRepository, ProcessRepository};
use jeeflow_core::FlowData;
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::Arc;

const SUBMIT_APPLY: i64 = 0;
const SUBMIT_AGREE: i64 = 1;

// ═══ 工装（照 tests/dynamic_countersign.rs make_facade）═══

fn make_facade() -> (JeeflowFacade, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    (JeeflowFacade::new(ctx), repo)
}

/// 五节点链 start→apply→review→final→end；参数是 review 的 properties 片段。
fn chain(review_props: &str) -> String {
    let r = if review_props.is_empty() {
        String::new()
    } else {
        format!(",{}", review_props)
    };
    format!(
        r#"{{
            "name": "allow-recall-proj-inline",
            "displayName": "allowRecall 投影门面夹具",
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

/// 发起并办结申请节点（经门面 execute），返回停在 review 的实例 id。
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
        let resp = facade
            .flow("processTask/execute", &exec_args(t.task_id, "applicant", SUBMIT_APPLY))
            .await;
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

async fn done_rows(facade: &JeeflowFacade, who: &str) -> Vec<Json> {
    let mut m = HashMap::new();
    m.insert("operator".to_string(), json!(who));
    m.insert("pageSize".to_string(), json!(100));
    let resp = facade.flow("processTask/doneList", &m).await;
    assert_eq!(resp["code"], 0, "doneList({who}) 应成功: {resp:?}");
    resp["data"]["rows"].as_array().cloned().unwrap_or_default()
}

async fn todo_rows(facade: &JeeflowFacade, who: &str) -> Vec<Json> {
    let mut m = HashMap::new();
    m.insert("operator".to_string(), json!(who));
    m.insert("pageSize".to_string(), json!(100));
    let resp = facade.flow("processTask/todoList", &m).await;
    assert_eq!(resp["code"], 0, "todoList({who}) 应成功: {resp:?}");
    resp["data"]["rows"].as_array().cloned().unwrap_or_default()
}

async fn detail_tasks(facade: &JeeflowFacade, iid: i64) -> Vec<Json> {
    let mut m = HashMap::new();
    m.insert("id".to_string(), json!(iid));
    let resp = facade.flow("processInstance/detail", &m).await;
    assert_eq!(resp["code"], 0, "processInstance/detail 应成功: {resp:?}");
    resp["data"]["tasks"].as_array().cloned().unwrap_or_default()
}

fn row_named<'a>(rows: &'a [Json], name: &str) -> Option<&'a Json> {
    rows.iter().find(|r| r["taskName"].as_str() == Some(name))
}

/// 顶层契约键 allowRecall（出口 to_camel 已把 snake allow_recall 落成 camel）。
fn allow_recall_of(row: &Json) -> Option<bool> {
    row.get("allowRecall").and_then(|v| v.as_bool())
}

// ═══ 用例 ═══

/// 主档（四合一）：doneList 已办行顶层 allowRecall=true（review 配）/false（final 不配）；
/// todoList DOING 行无键；detail tasks 已办有键、DOING 无键；apply（首任务节点恒 false）已办出 false。
#[tokio::test]
async fn projection01_done_todo_and_detail_carry_allow_recall_snapshot() {
    let (facade, repo) = make_facade();
    let define_id = save_define(
        &repo,
        "allow-recall-proj-01",
        &chain(r#""allowRecall":true"#),
    );
    let iid = start_to_review(&facade, &repo, define_id).await;

    // review 仍 DOING：todoList 行**无** allowRecall 键（DOING 永不投）
    let todo = todo_rows(&facade, "userA").await;
    let review_todo = row_named(&todo, "review").expect("review 应在 userA 待办里");
    assert!(
        review_todo.get("allowRecall").is_none(),
        "DOING 行永不投 allowRecall：{review_todo}"
    );

    // detail：apply 已办（首任务节点恒 false）有键 false；review DOING 无键
    let tasks = detail_tasks(&facade, iid).await;
    assert_eq!(
        allow_recall_of(row_named(&tasks, "apply").expect("detail 应含 apply 行")),
        Some(false),
        "apply 已办应投 allowRecall=false（首任务节点恒 false 也盖键）"
    );
    assert!(
        row_named(&tasks, "review").unwrap().get("allowRecall").is_none(),
        "detail 里 review 仍 DOING ⇒ 无键"
    );

    // 办结 review（配 allowRecall:true）
    let review = doing_named(&repo, iid, "review").unwrap();
    let resp = facade
        .flow("processTask/execute", &exec_args(review.task_id, "userA", SUBMIT_AGREE))
        .await;
    assert_eq!(resp["code"], 0, "review 办结应成功：{resp}");

    // doneList(userA)：review 已办行顶层 allowRecall=true
    let done = done_rows(&facade, "userA").await;
    assert_eq!(
        allow_recall_of(row_named(&done, "review").expect("doneList 应含已办 review")),
        Some(true),
        "review 配 true ⇒ 已办行出口 allowRecall=true"
    );

    // todoList(userF)：final DOING 行无键
    let todo_f = todo_rows(&facade, "userF").await;
    assert!(
        row_named(&todo_f, "final").expect("final 应在 userF 待办里").get("allowRecall").is_none(),
        "final DOING ⇒ todoList 无 allowRecall 键"
    );

    // detail：review 已办 true；final DOING 无键
    let tasks = detail_tasks(&facade, iid).await;
    assert_eq!(
        allow_recall_of(row_named(&tasks, "review").unwrap()),
        Some(true),
        "detail 里 review 已办 ⇒ allowRecall=true"
    );
    assert!(
        row_named(&tasks, "final").unwrap().get("allowRecall").is_none(),
        "detail 里 final 仍 DOING ⇒ 无键"
    );

    // 办结 final（不配 allowRecall ⇒ 默认 false 也盖键）
    let final_task = doing_named(&repo, iid, "final").unwrap();
    let resp = facade
        .flow("processTask/execute", &exec_args(final_task.task_id, "userF", SUBMIT_AGREE))
        .await;
    assert_eq!(resp["code"], 0, "final 办结应成功：{resp}");

    // doneList(userF)：final 已办行顶层 allowRecall=false
    let done_f = done_rows(&facade, "userF").await;
    assert_eq!(
        allow_recall_of(row_named(&done_f, "final").expect("doneList 应含已办 final")),
        Some(false),
        "final 不配 ⇒ 已办行出口 allowRecall=false（默认 false 也有键）"
    );

    // detail：final 已办 false
    let tasks = detail_tasks(&facade, iid).await;
    assert_eq!(
        allow_recall_of(row_named(&tasks, "final").unwrap()),
        Some(false),
        "detail 里 final 已办 ⇒ allowRecall=false"
    );
}

/// 缺键档（存量脏行）：已办行抹掉 `tf_allowRecall` ⇒ doneList 与 detail 都**不得造** allowRecall 字段
/// （缺键与 false 是两档）。证明出口只搬运、不现算定义。
#[tokio::test]
async fn projection02_missing_key_row_projects_no_field() {
    let (facade, repo) = make_facade();
    let define_id = save_define(
        &repo,
        "allow-recall-proj-02",
        &chain(r#""allowRecall":true"#),
    );
    let iid = start_to_review(&facade, &repo, define_id).await;
    let review = doing_named(&repo, iid, "review").unwrap();
    let resp = facade
        .flow("processTask/execute", &exec_args(review.task_id, "userA", SUBMIT_AGREE))
        .await;
    assert_eq!(resp["code"], 0, "{}", resp);

    // 前提：已办 review 行本有 allowRecall=true
    let done = done_rows(&facade, "userA").await;
    assert_eq!(allow_recall_of(row_named(&done, "review").unwrap()), Some(true));

    // 抹掉行上快照键（模拟没有快照的存量行）
    let mut row = repo
        .find_history_tasks(iid)
        .unwrap()
        .into_iter()
        .find(|t| t.task_name == "review" && t.is_finished())
        .unwrap();
    row.variables.remove(recall::KEY_ALLOW_RECALL);
    repo.update_task(&row).unwrap();

    // doneList：缺键 ⇒ 不出字段
    let done = done_rows(&facade, "userA").await;
    let r = row_named(&done, "review").unwrap();
    assert!(
        r.get("allowRecall").is_none(),
        "缺键的存量已办行不得造 allowRecall 字段：{r}"
    );

    // detail 同形：缺键 ⇒ 不出字段
    let tasks = detail_tasks(&facade, iid).await;
    assert!(
        row_named(&tasks, "review").unwrap().get("allowRecall").is_none(),
        "detail 缺键也不投 allowRecall"
    );
}