//! 「动态协办（aidant）」门面侧端到端验收（issues/253）——逐字移植 jeeflow-java
//! `test/AidantTest.java`（34 例）。
//!
//! 契约正文：spec/04「动态协办（aidant）」（派生规则／推进闸与等待闸／能力边界／留痕与投影）、
//! spec/06 §2.7（协议键 `tf_aidantHistory`／`tf_aidantReason`／`tf_aidantWait` ＋ 投影 `ext.aidant`）
//! ＋ §processTask/aidant（协办六行逐字失败文案）。
//! 参考实现基准（逐字权威）：jeeflow-java issues/253 实施轮的 `core/AidantSupport.java`、
//! `core/JeeflowEngineImpl.aidantTask / completeAidantWithoutDrive`、`facade/JeeflowFacade.taskAidant`。
//!
//! 覆盖：派生形状（task_type=1／parent＝主办行／同节点同名／performType=Normal／expire_time 同尺／
//! 委托并入／码 3 既有腿）、推进闸（协办行办结不驱动，execute 与 jump 两路同闸）、等待闸两档
//! （waitCompletion=true 逐字文案＋零写入／缺省 false 随时推进＋离开节点清场记账）、两跳账本存活、
//! detail 投影形状、协办行能力边界（可加/摘人、可被转办、禁递归）、负向六文案＋越权＋非 DOING、
//! 回归（会签票齐即推进不被协办挡／与征询并存两闸各数／撤回清场／transfer·surrogate·addCandidate 不变）。
//!
//! 夹具纪律：全部内联 JSON，**不碰共享 `flows/`**（同 `dynamic_countersign.rs`／`recall.rs`）；
//! 既有测试期望值一字未改（仅 action 名单与 OR 表条数断言按 253 +1，落在各自原文件）。
//!
//! ⚠️ 投影键序：本栈 serde_json 未开 preserve_order，门面响应的 object 键按字母序排列，
//! 故键序断言落在核心 [`jeeflow_core::aidant::build_projection`]（JsonValue::Object 保序）那一支，
//! 门面响应只断言键集＋值（语义与 java LinkedHashMap 逐字同，排序是 serde 结构层差异）。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::event::ProcessEvent;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::{
    InstanceState, PerformType, ProcessDefine, ProcessSurrogate, ProcessTask, TaskState, TaskType,
};
use jeeflow_core::spi::{ProcessEventListener, ProcessExtRepository, ProcessRepository};
use jeeflow_core::{add_sign, aidant, collect, FlowData, JsonValue};
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const SUBMIT_APPLY: i64 = 0;
const SUBMIT_AGREE: i64 = 1;
const SUBMIT_REJECT: i64 = 2;
const SUBMIT_ROLLBACK: i64 = 3;
const SUBMIT_JUMP: i64 = 4;
const SUBMIT_TO_OPERATOR: i64 = 6;

// ═══ 工装 ═══

fn make_facade() -> (JeeflowFacade, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    (JeeflowFacade::new(ctx), repo)
}

/// 带事件录音机的门面（码 3/5 既有腿取证；不新增事件码）。
fn make_facade_recording() -> (JeeflowFacade, Arc<MemoryRepository>, Arc<Recorder>) {
    let repo = Arc::new(MemoryRepository::new());
    let rec = Arc::new(Recorder::default());
    let mut ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    ctx.register_event_listener(rec.clone() as Arc<dyn ProcessEventListener>);
    (JeeflowFacade::new(ctx), repo, rec)
}

#[derive(Default)]
struct Recorder(Mutex<Vec<i32>>);

impl ProcessEventListener for Recorder {
    fn on_event(&self, event: &ProcessEvent) {
        self.0.lock().unwrap().push(event.event_type.code());
    }
}

impl Recorder {
    fn codes(&self) -> Vec<i32> {
        self.0.lock().unwrap().clone()
    }
    fn reset(&self) {
        self.0.lock().unwrap().clear();
    }
}

/// 通用五节点链 start→apply→review→final→end；review_props 是追加到 review 的 properties 片段。
fn chain(review_props: &str) -> String {
    let r = if review_props.is_empty() {
        String::new()
    } else {
        format!(",{}", review_props)
    };
    format!(
        r#"{{
            "name": "aidant-inline",
            "displayName": "动态协办夹具",
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

/// 并行会签链：start→apply→cs（userA,userB）→end。
fn countersign_chain() -> String {
    r#"{
        "name": "aidant-cs-inline",
        "displayName": "协办×会签夹具",
        "type": "approval",
        "nodes": [
            {"id":"start","type":"snaker:start","properties":{}},
            {"id":"apply","type":"snaker:task","properties":{"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"}},
            {"id":"cs","type":"snaker:task","properties":{"form":"form-c","taskType":0,"performType":1,"assignee":"userA,userB","countersignType":"PARALLEL"}},
            {"id":"end","type":"snaker:end","properties":{}}
        ],
        "edges": [
            {"id":"e1","type":"snaker:transition","sourceNodeId":"start","targetNodeId":"apply","properties":{}},
            {"id":"e2","type":"snaker:transition","sourceNodeId":"apply","targetNodeId":"cs","properties":{}},
            {"id":"e3","type":"snaker:transition","sourceNodeId":"cs","targetNodeId":"end","properties":{}}
        ]
    }"#
    .to_string()
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

fn instance_state(repo: &Arc<MemoryRepository>, iid: i64) -> i32 {
    repo.find_instance_by_id(iid).unwrap().unwrap().state
}

fn aidant_rows(repo: &Arc<MemoryRepository>, iid: i64, host: &ProcessTask) -> Vec<ProcessTask> {
    aidant::aidant_rows(&rows(repo, iid), &task_row(repo, host.task_id))
}

fn ledger(repo: &Arc<MemoryRepository>, row: &ProcessTask) -> Vec<JsonValue> {
    aidant::history_of(&task_row(repo, row.task_id))
}

fn entry(hist: &JsonValue, key: &str) -> JsonValue {
    hist.as_object()
        .and_then(|entries| entries.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()))
        .unwrap_or_else(|| panic!("账本条目缺键 {}: {:?}", key, hist))
}

fn keys(hist: &JsonValue) -> Vec<String> {
    hist.as_object()
        .map(|entries| entries.iter().map(|(k, _)| k.clone()).collect())
        .unwrap_or_default()
}

/// 发起并办结申请节点，返回停在第一个审批节点的实例 id（chain 与 countersign_chain 通用）。
async fn start(facade: &JeeflowFacade, repo: &Arc<MemoryRepository>, define_id: i64) -> i64 {
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

async fn start_to_review(facade: &JeeflowFacade, repo: &Arc<MemoryRepository>, review_props: &str) -> i64 {
    let did = save_define(repo, "aidant-chain", &chain(review_props));
    start(facade, repo, did).await
}

fn exec_args(task_id: i64, operator: &str, submit_type: i64) -> HashMap<String, Json> {
    let mut a = HashMap::new();
    a.insert("processTaskId".to_string(), json!(task_id.to_string()));
    a.insert("operator".to_string(), json!(operator));
    a.insert("submitType".to_string(), json!(submit_type));
    a
}

/// 派生协办入参：operator/actorIds/reason/wait 各档可缺（None＝不传键）。
fn aidant_args(
    task_id: i64,
    operator: Option<&str>,
    actor_ids: Option<Json>,
    reason: Option<&str>,
    wait: Option<Json>,
) -> HashMap<String, Json> {
    let mut a = HashMap::new();
    a.insert("processTaskId".to_string(), json!(task_id.to_string()));
    if let Some(op) = operator {
        a.insert("operator".to_string(), json!(op));
    }
    if let Some(ids) = actor_ids {
        a.insert("actorIds".to_string(), ids);
    }
    if let Some(r) = reason {
        a.insert("reason".to_string(), json!(r));
    }
    if let Some(w) = wait {
        a.insert("waitCompletion".to_string(), w);
    }
    a
}

async fn aidant(
    facade: &JeeflowFacade,
    host: &ProcessTask,
    operator: Option<&str>,
    actor_ids: Option<Json>,
    reason: Option<&str>,
    wait: Option<Json>,
) -> Json {
    facade
        .flow("processTask/aidant", &aidant_args(host.task_id, operator, actor_ids, reason, wait))
        .await
}

async fn execute(facade: &JeeflowFacade, row: &ProcessTask, operator: &str, submit_type: i64) -> Json {
    facade
        .flow("processTask/execute", &exec_args(row.task_id, operator, submit_type))
        .await
}

fn assert_msg(resp: &Json, msg: &str) {
    assert_eq!(resp["code"], 99999999, "应失败：{}", resp);
    assert_eq!(resp["msg"], msg, "逐字文案：{}", resp);
}

fn actors_json(ids: &[&str]) -> Json {
    json!(ids.iter().map(|s| s.to_string()).collect::<Vec<_>>())
}

// ═══ 正向：派生形状 ═══

/// 派生：每人一行、task_type=1、parent＝主办行、同节点同名、performType=Normal、主办保留 DOING、账本键序。
#[tokio::test]
async fn aidant01_derive_shapes_secondary_rows_and_keeps_host_doing() {
    let (facade, repo, rec) = make_facade_recording();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    rec.reset();

    let resp = aidant(&facade, &host, Some("userA"), Some(actors_json(&["userX", "userY"])),
        Some("需两部门协办"), None).await;
    assert_eq!(resp["code"], 0, "{}", resp);
    assert!(resp["data"].is_null(), "成功 data → null（spec/06）：{}", resp);

    // 主办行原样保留 DOING，名册不动
    let host_row = task_row(&repo, host.task_id);
    assert!(host_row.is_doing(), "主办行保留 DOING");
    assert_eq!(vec!["userA".to_string()], host_row.actor_ids);
    assert!(host_row.actor_id.is_none(), "严禁覆写任务 actor_id 列（转办那条 ⚠️ 同尺）");

    let rows = aidant_rows(&repo, iid, &host);
    assert_eq!(2, rows.len(), "多人每人一行");
    let mut actors: Vec<String> = Vec::new();
    for r in &rows {
        assert!(r.is_doing());
        assert_eq!(TaskType::Assistant.code(), r.task_type, "task_type=1");
        assert_eq!(Some(host.task_id), r.parent_task_id);
        assert_eq!(host.task_name, r.task_name, "同节点");
        assert_eq!(host.display_name, r.display_name, "同名");
        assert_eq!(PerformType::Normal.code(), r.perform_type, "performType=Normal");
        assert_eq!(Some("需两部门协办"), r.variables.get_str(aidant::AIDANT_REASON));
        assert!(r.variables.get("tf_approvalComment").is_none(), "协办行不占 tf_approvalComment");
        assert!(r.actor_id.is_none(), "进行中任务 actor_id 列恒无值");
        actors.push(r.actor_ids[0].clone());
    }
    assert_eq!(vec!["userX".to_string(), "userY".to_string()], actors);

    // 账本：一条、六键锁定键序、time 为 §2.4 字符串
    let lg = ledger(&repo, &host);
    assert_eq!(1, lg.len());
    assert_eq!(
        vec!["round", "fromActor", "toActors", "reason", "time", "operator"],
        keys(&lg[0]),
        "六键原序（核心 JsonValue 保序）"
    );
    assert_eq!(1.0, entry(&lg[0], "round").as_f64().unwrap());
    assert_eq!("userA", entry(&lg[0], "fromActor").as_str().unwrap());
    assert_eq!("需两部门协办", entry(&lg[0], "reason").as_str().unwrap());
    assert_eq!("userA", entry(&lg[0], "operator").as_str().unwrap());
    let time = entry(&lg[0], "time").as_str().unwrap().to_string();
    assert!(time.len() == 19 && time.as_bytes()[4] == b'-' && time.as_bytes()[13] == b':',
        "time 一律 yyyy-MM-dd HH:mm:ss：{}", time);

    // 事件：只走既有码 3（逐协办行一次），不新增事件码
    assert_eq!(vec![3, 3], rec.codes());
}

/// reason 选填：空串兜底进账本，协办行不盖空戳。
#[tokio::test]
async fn aidant02_reason_optional_falls_back_to_empty_string() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    let resp = aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await;
    assert_eq!(resp["code"], 0, "{}", resp);
    assert_eq!("", entry(&ledger(&repo, &host)[0], "reason").as_str().unwrap());
    assert!(aidant_rows(&repo, iid, &host)[0].variables.get(aidant::AIDANT_REASON).is_none());
    assert!(task_row(&repo, host.task_id).variables.get("tf_approvalComment").is_none(),
        "主办行也不占审批意见");
}

/// actorIds 逗号串与数组两形同归一（§2.11 单点）；空白元素丢弃。
#[tokio::test]
async fn aidant03_actor_ids_accepts_comma_string_and_array() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    let resp = aidant(&facade, &host, Some("userA"), Some(json!(" userX , userY ,")), None, None).await;
    assert_eq!(resp["code"], 0, "{}", resp);
    assert_eq!(2, aidant_rows(&repo, iid, &host).len());
}

/// expire_time 同尺重算（节点表达式），委托照既有腿并入协办行名册。
#[tokio::test]
async fn aidant04_expire_time_recomputed_and_surrogate_merged() {
    let (facade, repo) = make_facade();
    let mut s = ProcessSurrogate {
        id: 0,
        process_name: "".into(),
        operator: "userX".into(),
        surrogate: "userS".into(),
        start_time: Some("2020-01-01 00:00:00".into()),
        end_time: Some("2099-12-31 23:59:59".into()),
        enabled: 1,
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    repo.save_surrogate(&mut s).unwrap();
    let iid = start_to_review(&facade, &repo, r#""expireTime":"2d""#).await;
    let host = doing_named(&repo, iid, "review").unwrap();
    let resp = aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await;
    assert_eq!(resp["code"], 0, "{}", resp);
    let child = aidant_rows(&repo, iid, &host)[0].clone();
    assert!(child.expire_time.is_some(), "expire_time 按节点表达式同尺重算");
    assert!(child.actor_ids.contains(&"userX".to_string()), "委托并入不摘原人");
    assert!(child.actor_ids.contains(&"userS".to_string()), "委托照既有腿并入");
}

// ═══ 正向：推进闸 ═══

/// 协办行办结不产生执行对象、不跑输出变迁；主办行仍 DOING；只 fire 既有码 5。
#[tokio::test]
async fn aidant05_aidant_completion_does_not_drive() {
    let (facade, repo, rec) = make_facade_recording();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await;
    let child = aidant_rows(&repo, iid, &host)[0].clone();
    rec.reset();

    let mut fd = FlowData::new();
    fd.insert_i64("submitType", SUBMIT_AGREE);
    let created = facade.engine().execute_task_sync(child.task_id, "userX", &fd).unwrap();
    assert!(created.is_empty(), "协办办结不产生执行对象（无新任务）");
    assert!(task_row(&repo, child.task_id).is_finished());
    assert!(task_row(&repo, host.task_id).is_doing(), "主办行保留 DOING");
    assert!(doing_named(&repo, iid, "final").is_none(), "不跑输出变迁：下游节点无待办");
    assert_eq!(InstanceState::Doing.code(), instance_state(&repo, iid));
    assert_eq!(vec![5], rec.codes(), "只走既有码 5，不新增事件码");
}

/// 两路同闸：executeAndJumpTask（JUMP／ROLLBACK）对协办行同样不驱动。
#[tokio::test]
async fn aidant06_aidant_jump_path_also_gated() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(actors_json(&["userX", "userY"])), None, None).await;
    let rows = aidant_rows(&repo, iid, &host);

    // JUMP 路（门面 submitType=4）
    let mut a = exec_args(rows[0].task_id, "userX", SUBMIT_JUMP);
    a.insert("taskName".to_string(), json!("final"));
    let resp = facade.flow("processTask/execute", &a).await;
    assert_eq!(resp["code"], 0, "{}", resp);
    assert!(task_row(&repo, rows[0].task_id).is_finished());
    assert!(doing_named(&repo, iid, "final").is_none(), "跳转路同闸：不得凭空建出 final 待办");

    // ROLLBACK 路（引擎直调，nodeName=None）
    let mut fd = FlowData::new();
    fd.insert_i64("submitType", SUBMIT_ROLLBACK);
    let created = facade.engine().execute_and_jump_sync(rows[1].task_id, "userY", &fd, None).unwrap();
    assert!(created.is_empty());
    assert!(task_row(&repo, rows[1].task_id).is_finished());
    assert!(task_row(&repo, host.task_id).is_doing(), "主办行仍在");
    assert_eq!(1, doing_count(&repo, iid, "review"));
}

/// 协办行拒绝／退回发起人同样不驱动（任何 submitType 都只办结自己那一行）。
#[tokio::test]
async fn aidant07_aidant_reject_and_return_do_not_drive() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(actors_json(&["userX", "userY"])), None, None).await;
    let rows = aidant_rows(&repo, iid, &host);

    assert_eq!(0, execute(&facade, &rows[0], "userX", SUBMIT_REJECT).await["code"]);
    assert_eq!(InstanceState::Doing.code(), instance_state(&repo, iid), "拒绝不得结束整单");
    assert_eq!(0, execute(&facade, &rows[1], "userY", SUBMIT_TO_OPERATOR).await["code"]);
    assert_eq!(InstanceState::Doing.code(), instance_state(&repo, iid));
    assert!(task_row(&repo, host.task_id).is_doing());
    assert!(doing_named(&repo, iid, "apply").is_none());
}

/// 主办再办结照常驱动（协办已办结）。
#[tokio::test]
async fn aidant08_host_completion_after_aidant_drives() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await;
    let child = aidant_rows(&repo, iid, &host)[0].clone();
    execute(&facade, &child, "userX", SUBMIT_AGREE).await;
    assert_eq!(0, execute(&facade, &host, "userA", SUBMIT_AGREE).await["code"]);
    assert!(doing_named(&repo, iid, "final").is_some(), "主办办结才推进");
    assert!(task_row(&repo, host.task_id).is_finished());
}

// ═══ 正向／负向：等待闸两档 ═══

/// waitCompletion=true：未齐 ⇒ 逐字文案＋零写入；齐了 ⇒ 放行推进。
#[tokio::test]
async fn aidant09_wait_completion_true_blocks_host_until_aidant_done() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    let resp = aidant(&facade, &host, Some("userA"), Some(actors_json(&["userX", "userY"])),
        None, Some(json!(true))).await;
    assert_eq!(resp["code"], 0, "{}", resp);
    let rows = aidant_rows(&repo, iid, &host);
    let ledger_before = ledger(&repo, &host).len();

    let blocked = execute(&facade, &host, "userA", SUBMIT_AGREE).await;
    assert_msg(&blocked, "节点[review]协办未办结（已办结 0/2），不能办理");
    // 零写入：主办仍 DOING、无下游待办、账本不增、协办行不动
    assert!(task_row(&repo, host.task_id).is_doing());
    assert!(doing_named(&repo, iid, "final").is_none());
    assert_eq!(ledger_before, ledger(&repo, &host).len());
    assert_eq!(2, aidant_rows(&repo, iid, &host).len());
    for r in aidant_rows(&repo, iid, &host) {
        assert!(r.is_doing());
    }

    // 办结一条 ⇒ 计数 1/2，仍拦
    execute(&facade, &rows[0], "userX", SUBMIT_AGREE).await;
    assert_msg(&execute(&facade, &host, "userA", SUBMIT_AGREE).await,
        "节点[review]协办未办结（已办结 1/2），不能办理");

    // 全办结 ⇒ 放行
    execute(&facade, &rows[1], "userY", SUBMIT_AGREE).await;
    assert_eq!(0, execute(&facade, &host, "userA", SUBMIT_AGREE).await["code"]);
    assert!(doing_named(&repo, iid, "final").is_some());
}

/// 等待闸挡全部 submitType（含拒绝／退回），不只挡同意。
#[tokio::test]
async fn aidant10_wait_gate_blocks_every_submit_type() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, Some(json!(true))).await;
    for st in [SUBMIT_AGREE, SUBMIT_REJECT, SUBMIT_TO_OPERATOR] {
        assert_msg(&execute(&facade, &host, "userA", st).await,
            "节点[review]协办未办结（已办结 0/1），不能办理");
        assert!(task_row(&repo, host.task_id).is_doing());
    }
    assert_eq!(InstanceState::Doing.code(), instance_state(&repo, iid));
}

/// waitCompletion 缺省 false：主办可随时推进，未办结协办行随废弃腿清场＋账本记废弃。
#[tokio::test]
async fn aidant11_wait_default_false_cleans_pending_aidant_on_leave() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(actors_json(&["userX", "userY"])), None, None).await;
    let rows = aidant_rows(&repo, iid, &host);
    // 只办结一条，另一条仍 DOING
    execute(&facade, &rows[0], "userX", SUBMIT_AGREE).await;

    assert_eq!(0, execute(&facade, &host, "userA", SUBMIT_AGREE).await["code"], "缺省 false 主办可随时推进");
    assert!(doing_named(&repo, iid, "final").is_some());
    assert_eq!(TaskState::Abandon.code(), task_row(&repo, rows[1].task_id).task_state,
        "未办结协办行清场为废弃(99)");
    assert_eq!(TaskState::Finished.code(), task_row(&repo, rows[0].task_id).task_state, "已办结那条不改写");
    // 账本记废弃：末条带 abandoned 标记、toActors＝被废弃的人
    let lg = ledger(&repo, &host);
    assert_eq!(2, lg.len());
    assert_eq!(Some(true), entry(&lg[1], "abandoned").as_bool());
    assert_eq!(vec!["userY".to_string()],
        entry(&lg[1], "toActors").as_array().unwrap().iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string())).collect::<Vec<_>>());
    assert_eq!(1.0, entry(&lg[1], "round").as_f64().unwrap(), "清场不消耗轮次");
    assert_eq!(0, doing_count(&repo, iid, "review"), "不留孤儿待办");
}

/// waitCompletion=false 显式传值同缺省；字符串 "true" 两形也认。
#[tokio::test]
async fn aidant12_wait_completion_accepts_string_form() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    let resp = aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, Some(json!("true"))).await;
    assert_eq!(resp["code"], 0, "{}", resp);
    assert_msg(&execute(&facade, &host, "userA", SUBMIT_AGREE).await,
        "节点[review]协办未办结（已办结 0/1），不能办理");
}

/// 拒绝／退回发起人离开节点：协办行同样清场（废弃腿覆盖）。
#[tokio::test]
async fn aidant13_reject_and_return_to_operator_clean_aidant() {
    let (facade, repo) = make_facade();
    let iid1 = start_to_review(&facade, &repo, "").await;
    let h1 = doing_named(&repo, iid1, "review").unwrap();
    aidant(&facade, &h1, Some("userA"), Some(json!("userX")), None, None).await;
    let c1 = aidant_rows(&repo, iid1, &h1)[0].clone();
    assert_eq!(0, execute(&facade, &h1, "userA", SUBMIT_REJECT).await["code"]);
    assert_eq!(TaskState::Abandon.code(), task_row(&repo, c1.task_id).task_state);
    assert_eq!(Some(true), entry(&ledger(&repo, &h1)[1], "abandoned").as_bool());

    let iid2 = start_to_review(&facade, &repo, "").await;
    let h2 = doing_named(&repo, iid2, "review").unwrap();
    aidant(&facade, &h2, Some("userA"), Some(json!("userX")), None, None).await;
    let c2 = aidant_rows(&repo, iid2, &h2)[0].clone();
    assert_eq!(0, execute(&facade, &h2, "userA", SUBMIT_TO_OPERATOR).await["code"]);
    assert_eq!(TaskState::Abandon.code(), task_row(&repo, c2.task_id).task_state);
    assert!(doing_named(&repo, iid2, "apply").is_some(), "退回发起人后申请节点复活");
}

// ═══ 正向：账本与投影 ═══

/// 两跳账本＝2，且主办办结后仍在（variables 合并序保证存活）。
#[tokio::test]
async fn aidant14_two_rounds_ledger_survives_host_finish() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(json!("userX")), Some("第一轮"), Some(json!(false))).await;
    let first = aidant_rows(&repo, iid, &host)[0].clone();
    execute(&facade, &first, "userX", SUBMIT_AGREE).await;
    aidant(&facade, &host, Some("userA"), Some(json!("userY")), Some("第二轮"), Some(json!(false))).await;

    let lg = ledger(&repo, &host);
    assert_eq!(2, lg.len(), "两跳账本＝2");
    assert_eq!(1.0, entry(&lg[0], "round").as_f64().unwrap());
    assert_eq!(2.0, entry(&lg[1], "round").as_f64().unwrap());
    assert_eq!("第一轮", entry(&lg[0], "reason").as_str().unwrap());
    assert_eq!("第二轮", entry(&lg[1], "reason").as_str().unwrap());

    let second = aidant_rows(&repo, iid, &host)[1].clone();
    execute(&facade, &second, "userY", SUBMIT_AGREE).await;
    assert_eq!(0, execute(&facade, &host, "userA", SUBMIT_AGREE).await["code"]);
    assert!(task_row(&repo, host.task_id).is_finished());
    assert_eq!(2, ledger(&repo, &host).len(), "办结后账本仍在");
}

/// 投影 ext.aidant＝{active, round, total, submitted, actors, wait}，从 parent＋task_type 现算。
#[tokio::test]
async fn aidant15_detail_projection_shape() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(actors_json(&["userX", "userY"])),
        Some("协办原因"), Some(json!(true))).await;
    let ad_rows = aidant_rows(&repo, iid, &host);

    // 核心投影保序：六键原序（serde_json 响应侧按键排序，故键序断言落这一支）
    let core = aidant::build_projection(&rows(&repo, iid), &task_row(&repo, host.task_id)).unwrap();
    assert_eq!(vec!["active", "round", "total", "submitted", "actors", "wait"], keys(&core));

    let mut a = HashMap::new();
    a.insert("id".to_string(), json!(host.task_id.to_string()));
    let detail = facade.flow("processTask/detail", &a).await;
    assert_eq!(detail["code"], 0, "{}", detail);
    let p = &detail["data"]["ext"]["aidant"];
    assert!(!p.is_null(), "主办行输出 ext.aidant");
    assert_eq!(p["active"], true);
    assert_eq!(p["round"], 1);
    assert_eq!(p["total"], 2);
    assert_eq!(p["submitted"], 0);
    assert_eq!(p["wait"], true);
    let actors = p["actors"].as_array().unwrap();
    assert_eq!(2, actors.len());
    assert_eq!("userX", actors[0]["actor"]);
    assert_eq!(ad_rows[0].task_id.to_string(), actors[0]["taskId"].as_str().unwrap());
    assert_eq!(TaskState::Doing.code() as i64, actors[0]["state"]);

    // 办结一条 ⇒ submitted 现算推进
    execute(&facade, &ad_rows[0], "userX", SUBMIT_AGREE).await;
    let d2 = facade.flow("processTask/detail", &a).await;
    let p2 = &d2["data"]["ext"]["aidant"];
    assert_eq!(p2["submitted"], 1);
    assert_eq!(p2["active"], true);

    // 无协办行的普通任务不输出该投影
    let other = start_to_review(&facade, &repo, "").await;
    let plain = doing_named(&repo, other, "review").unwrap();
    let mut a3 = HashMap::new();
    a3.insert("id".to_string(), json!(plain.task_id.to_string()));
    let d3 = facade.flow("processTask/detail", &a3).await;
    assert!(d3["data"]["ext"]["aidant"].is_null(), "无协办行不输出空壳");
}

// ═══ 负向：六文案＋越权＋非 DOING ═══

#[tokio::test]
async fn aidant16_negative_actor_ids_required() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    assert_msg(&aidant(&facade, &host, Some("userA"), None, None, None).await, "actorIds 必填");
    assert_msg(&aidant(&facade, &host, Some("userA"), Some(json!([])), None, None).await, "actorIds 必填");
    assert_msg(&aidant(&facade, &host, Some("userA"), Some(json!("  ,  ")), None, None).await, "actorIds 必填");
    assert_eq!(0, aidant_rows(&repo, iid, &host).len(), "零写入");
}

#[tokio::test]
async fn aidant17_negative_operator_required() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    assert_msg(&aidant(&facade, &host, None, Some(json!("userX")), None, None).await, "operator 必填");
    assert_msg(&aidant(&facade, &host, Some("   "), Some(json!("userX")), None, None).await, "operator 必填");
}

#[tokio::test]
async fn aidant18_negative_task_not_found() {
    let (facade, repo) = make_facade();
    let _iid = start_to_review(&facade, &repo, "").await;
    let mut a = HashMap::new();
    a.insert("processTaskId".to_string(), json!("999999"));
    a.insert("operator".to_string(), json!("userA"));
    a.insert("actorIds".to_string(), json!("userX"));
    assert_msg(&facade.flow("processTask/aidant", &a).await, "任务不存在");
}

#[tokio::test]
async fn aidant19_negative_unauthorized_operator() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    assert_msg(&aidant(&facade, &host, Some("userZ"), Some(json!("userX")), None, None).await,
        "无权限派生协办任务");
    assert_eq!(0, aidant_rows(&repo, iid, &host).len(), "零写入");
    assert!(task_row(&repo, host.task_id).is_doing());
}

/// 名册内参与人（含加签进来的）可派生；flow.auto／flow.admin 例外放行。
#[tokio::test]
async fn aidant20_roster_member_and_privileged_allowed() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    // 加签进来的第二参与人同样在名册内
    let mut ac = HashMap::new();
    ac.insert("processTaskId".to_string(), json!(host.task_id.to_string()));
    ac.insert("actorIds".to_string(), actors_json(&["userB"]));
    facade.flow("processTask/addCandidate", &ac).await;
    assert_eq!(0, aidant(&facade, &host, Some("userB"), Some(json!("userX")), None, None).await["code"]);
    assert_eq!(0, aidant(&facade, &host, Some("flow.auto"), Some(json!("userY")), None, None).await["code"]);
    assert_eq!(0, aidant(&facade, &host, Some("flow.admin"), Some(json!("userZ")), None, None).await["code"]);
    assert_eq!(3, aidant_rows(&repo, iid, &host).len());
}

#[tokio::test]
async fn aidant21_negative_host_not_doing() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    execute(&facade, &host, "userA", SUBMIT_AGREE).await;
    assert!(task_row(&repo, host.task_id).is_finished());
    assert_msg(&aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await,
        "任务非进行中，不可派生协办");
}

#[tokio::test]
async fn aidant22_negative_duplicate_actor() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await;
    assert_msg(&aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await,
        "该人已有未办结的协办任务");
    assert_eq!(1, aidant_rows(&repo, iid, &host).len(), "零写入：不重复建行");
    assert_eq!(1, ledger(&repo, &host).len());

    // 已办结的那位可以再派生（去重只认未办结）
    let child = aidant_rows(&repo, iid, &host)[0].clone();
    execute(&facade, &child, "userX", SUBMIT_AGREE).await;
    assert_eq!(0, aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await["code"]);
    assert_eq!(2, aidant_rows(&repo, iid, &host).len());
}

#[tokio::test]
async fn aidant23_negative_recursion_forbidden() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await;
    let child = aidant_rows(&repo, iid, &host)[0].clone();
    assert_msg(&aidant(&facade, &child, Some("userX"), Some(json!("userY")), None, None).await,
        "协办任务不能再派生协办");
    assert_eq!(1, aidant_rows(&repo, iid, &host).len(), "零写入");
    assert_eq!(0, aidant::aidant_rows(&rows(&repo, iid), &child).len());
}

// ═══ 协办行能力边界 ═══

/// 协办行可加/摘参与人、可被转办（走既有 actor 写腿／transfer，不造特例）。
#[tokio::test]
async fn aidant24_aidant_row_supports_actor_writes_and_transfer() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await;
    let child = aidant_rows(&repo, iid, &host)[0].clone();
    let cid = child.task_id;

    // 加参与人（surrogate / addCandidate 同一腿）
    let mut s = HashMap::new();
    s.insert("processTaskId".to_string(), json!(cid.to_string()));
    s.insert("actorIds".to_string(), actors_json(&["userK"]));
    assert_eq!(0, facade.flow("processTask/surrogate", &s).await["code"]);
    assert!(task_row(&repo, cid).actor_ids.contains(&"userK".to_string()));
    let mut ac = HashMap::new();
    ac.insert("processTaskId".to_string(), json!(cid.to_string()));
    ac.insert("actorIds".to_string(), actors_json(&["userL"]));
    assert_eq!(0, facade.flow("processTask/addCandidate", &ac).await["code"]);
    assert!(task_row(&repo, cid).actor_ids.contains(&"userL".to_string()));

    // 摘参与人（只能摘自己那一票）
    let mut rm = HashMap::new();
    rm.insert("processTaskId".to_string(), json!(cid.to_string()));
    rm.insert("operator".to_string(), json!("userK"));
    rm.insert("actorIds".to_string(), actors_json(&["userK"]));
    assert_eq!(0, facade.flow("processTask/removeTaskActor", &rm).await["code"]);
    assert!(!task_row(&repo, cid).actor_ids.contains(&"userK".to_string()));

    // 转办（摘原人＋换新参与人，同一行不新建）
    let mut t = HashMap::new();
    t.insert("processTaskId".to_string(), json!(cid.to_string()));
    t.insert("operator".to_string(), json!("userX"));
    t.insert("fromActor".to_string(), json!("userX"));
    t.insert("toActor".to_string(), json!("userT"));
    t.insert("reason".to_string(), json!("协办转办"));
    assert_eq!(0, facade.flow("processTask/transfer", &t).await["code"]);
    let after = task_row(&repo, cid);
    assert!(!after.actor_ids.contains(&"userX".to_string()));
    assert!(after.actor_ids.contains(&"userT".to_string()));
    assert_eq!(cid, after.task_id, "转办不新建任务行");
    assert!(after.is_doing());
    assert_eq!(TaskType::Assistant.code(), after.task_type);
    // 转办后新办理人可办结，且仍不驱动流程
    assert_eq!(0, execute(&facade, &after, "userT", SUBMIT_AGREE).await["code"]);
    assert!(task_row(&repo, host.task_id).is_doing());
    assert!(doing_named(&repo, iid, "final").is_none());
}

// ═══ 回归：会签合并 ═══

/// 会签票齐即推进，不被协办行挡（合并判据显式排除 task_type=1）。
#[tokio::test]
async fn aidant25_countersign_merge_not_blocked_by_aidant() {
    let (facade, repo) = make_facade();
    let did = save_define(&repo, "aidant-cs", &countersign_chain());
    let iid = start(&facade, &repo, did).await;
    let members = repo.find_doing_tasks(iid, &[]).unwrap();
    assert_eq!(2, members.len());
    let row_a = members.iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap().clone();
    let row_b = members.iter().find(|t| t.actor_ids.contains(&"userB".to_string())).unwrap().clone();
    // 在会签成员行上派生协办（协办行同节点同名，但 task_type=1）
    assert_eq!(0, aidant(&facade, &row_a, Some("userA"), Some(json!("userX")), None, None).await["code"]);
    let child = aidant::aidant_rows(&rows(&repo, iid), &task_row(&repo, row_a.task_id))[0].clone();
    assert!(child.is_doing());
    assert_eq!("cs", child.task_name);

    // 两票齐 ⇒ 推进（改前协办行会被当第三张票，永远合并不了）
    assert_eq!(0, execute(&facade, &row_a, "userA", SUBMIT_AGREE).await["code"]);
    assert_eq!(0, execute(&facade, &row_b, "userB", SUBMIT_AGREE).await["code"]);
    assert_eq!(InstanceState::Finished.code(), instance_state(&repo, iid), "票齐即推进：实例办结");
    assert_eq!(TaskState::Abandon.code(), task_row(&repo, child.task_id).task_state, "协办行随离开节点清场");
    assert_eq!(0, repo.find_doing_tasks(iid, &[]).unwrap().len());
}

/// 会签未齐时协办行办结也不得推进（推进闸＋合并判据两头都守住）。
#[tokio::test]
async fn aidant26_aidant_on_countersign_does_not_advance_early() {
    let (facade, repo) = make_facade();
    let did = save_define(&repo, "aidant-cs2", &countersign_chain());
    let iid = start(&facade, &repo, did).await;
    let members = repo.find_doing_tasks(iid, &[]).unwrap();
    let row_a = members.iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap().clone();
    aidant(&facade, &row_a, Some("userA"), Some(json!("userX")), None, None).await;
    let child = aidant::aidant_rows(&rows(&repo, iid), &task_row(&repo, row_a.task_id))[0].clone();
    execute(&facade, &child, "userX", SUBMIT_AGREE).await;
    assert_eq!(InstanceState::Doing.code(), instance_state(&repo, iid), "协办办结不推进");
    assert_eq!(2, doing_count(&repo, iid, "cs"));
}

// ═══ 回归：与征询并存 ═══

/// 与征询同节点并存：两族子行靠 task_type 分辨，等待闸与收齐闸各数各的行。
#[tokio::test]
async fn aidant27_coexist_with_collect_two_gates_count_own_rows() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, r#""dynamicCountersign":true"#).await;
    let host = doing_named(&repo, iid, "review").unwrap();
    // 征询拉一人（成员行 task_type=0）
    let mut c = exec_args(host.task_id, "userA", SUBMIT_AGREE);
    c.insert(add_sign::ADD_SIGN_OPERATORS.to_string(), actors_json(&["userC"]));
    assert_eq!(0, facade.flow("processTask/execute", &c).await["code"]);
    // 协办派生一人（协办行 task_type=1），等待闸开
    assert_eq!(0, aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, Some(json!(true))).await["code"]);

    let host_row = task_row(&repo, host.task_id);
    let all = rows(&repo, iid);
    assert_eq!(1, collect::count_members(&all, &host_row).0, "征询收齐闸只数成员行");
    assert_eq!(1, aidant::count_aidant(&all, &host_row).0, "协办等待闸只数协办行");
    assert_eq!(1, collect::member_rows(&all, &host_row).len());
    assert_eq!(1, aidant::aidant_rows(&all, &host_row).len());
    assert!(!collect::is_member_row(&all, &aidant::aidant_rows(&all, &host_row)[0]),
        "协办行不是征询成员行");

    // 协办先办结 ⇒ 征询闸仍拦（各数各的行），文案是征询那一族
    let aidant_row = aidant::aidant_rows(&all, &host_row)[0].clone();
    execute(&facade, &aidant_row, "userX", SUBMIT_AGREE).await;
    assert_msg(&execute(&facade, &host_row, "userA", SUBMIT_AGREE).await,
        "节点[review]动态会签意见未收齐（已提交 0/1），不能办理");

    // 征询成员交意见 ⇒ 两闸齐过，主办推进
    let member = collect::member_rows(&rows(&repo, iid), &task_row(&repo, host.task_id))[0].clone();
    assert_eq!(0, execute(&facade, &member, "userC", SUBMIT_AGREE).await["code"]);
    assert_eq!(0, execute(&facade, &task_row(&repo, host.task_id), "userA", SUBMIT_AGREE).await["code"]);
    assert!(doing_named(&repo, iid, "final").is_some());
}

/// 反向：征询已收齐、协办未办结 ⇒ 报协办那一族文案（证明两闸不串味）。
#[tokio::test]
async fn aidant28_collect_done_but_aidant_pending_reports_aidant_message() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, r#""dynamicCountersign":true"#).await;
    let host = doing_named(&repo, iid, "review").unwrap();
    let mut c = exec_args(host.task_id, "userA", SUBMIT_AGREE);
    c.insert(add_sign::ADD_SIGN_OPERATORS.to_string(), actors_json(&["userC"]));
    facade.flow("processTask/execute", &c).await;
    aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, Some(json!(true))).await;
    let member = collect::member_rows(&rows(&repo, iid), &task_row(&repo, host.task_id))[0].clone();
    execute(&facade, &member, "userC", SUBMIT_AGREE).await;
    assert_msg(&execute(&facade, &task_row(&repo, host.task_id), "userA", SUBMIT_AGREE).await,
        "节点[review]协办未办结（已办结 0/1），不能办理");
}

/// 征询节点上 waitCompletion=false：主办收齐后可推进，未办结协办行清场（加签闸不被协办行挡）。
#[tokio::test]
async fn aidant29_collect_node_wait_false_not_blocked_by_aidant() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, r#""dynamicCountersign":true"#).await;
    let host = doing_named(&repo, iid, "review").unwrap();
    let mut c = exec_args(host.task_id, "userA", SUBMIT_AGREE);
    c.insert(add_sign::ADD_SIGN_OPERATORS.to_string(), actors_json(&["userC"]));
    facade.flow("processTask/execute", &c).await;
    aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, Some(json!(false))).await;
    let member = collect::member_rows(&rows(&repo, iid), &task_row(&repo, host.task_id))[0].clone();
    execute(&facade, &member, "userC", SUBMIT_AGREE).await;
    assert_eq!(0, execute(&facade, &task_row(&repo, host.task_id), "userA", SUBMIT_AGREE).await["code"],
        "缺省不等协办：收齐即推进");
    assert!(doing_named(&repo, iid, "final").is_some());
    let aidant_row = aidant::aidant_rows(&rows(&repo, iid), &task_row(&repo, host.task_id))[0].clone();
    assert_eq!(TaskState::Abandon.code(), task_row(&repo, aidant_row.task_id).task_state);
}

// ═══ 回归：撤回清场 ═══

/// 已办撤回：下游 DOING 协办行一并废弃（血缘自然覆盖），复活行是唯一活待办。
#[tokio::test]
async fn aidant30_recall_cleans_aidant_rows() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, r#""allowRecall":true"#).await;
    let host = doing_named(&repo, iid, "review").unwrap();
    assert_eq!(0, execute(&facade, &host, "userA", SUBMIT_AGREE).await["code"]);
    let final_row = doing_named(&repo, iid, "final").unwrap();
    // 在下游 final 行上派生协办 ⇒ 它是 review 行的血缘后代
    assert_eq!(0, aidant(&facade, &final_row, Some("userF"), Some(json!("userX")), None, None).await["code"]);
    let child = aidant::aidant_rows(&rows(&repo, iid), &task_row(&repo, final_row.task_id))[0].clone();
    assert!(child.is_doing());

    let resurrected = facade.engine().recall_task_sync(host.task_id, "userA").unwrap();
    assert!(resurrected.is_doing());
    assert_eq!("review", resurrected.task_name);
    assert_eq!(TaskState::Abandon.code(), task_row(&repo, child.task_id).task_state, "下游协办行废弃");
    assert_eq!(TaskState::Abandon.code(), task_row(&repo, final_row.task_id).task_state, "下游主办行废弃");
    assert_eq!(1, repo.find_doing_tasks(iid, &[]).unwrap().len(), "唯一活待办");
}

/// 实例撤回：DOING 协办行随整单置 WITHDRAW(30)，不留孤儿待办。
#[tokio::test]
async fn aidant31_instance_withdraw_cleans_aidant_rows() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await;
    let child = aidant_rows(&repo, iid, &host)[0].clone();
    let mut w = HashMap::new();
    w.insert("id".to_string(), json!(iid.to_string()));
    w.insert("operator".to_string(), json!("applicant"));
    assert_eq!(0, facade.flow("processInstance/withdraw", &w).await["code"]);
    assert_eq!(TaskState::Withdraw.code(), task_row(&repo, child.task_id).task_state);
    assert_eq!(0, repo.find_doing_tasks(iid, &[]).unwrap().len());
}

// ═══ 回归：既有 actor 写腿不变 ═══

/// transfer／surrogate／addCandidate 在主办行上行为不变（协办存在也不影响）。
#[tokio::test]
async fn aidant32_transfer_surrogate_add_candidate_unchanged() {
    let (facade, repo) = make_facade();
    let iid = start_to_review(&facade, &repo, "").await;
    let host = doing_named(&repo, iid, "review").unwrap();
    let hid = host.task_id;
    aidant(&facade, &host, Some("userA"), Some(json!("userX")), None, None).await;

    // surrogate：只追加，原人保留
    let mut s = HashMap::new();
    s.insert("processTaskId".to_string(), json!(hid.to_string()));
    s.insert("actorIds".to_string(), actors_json(&["userB"]));
    assert_eq!(0, facade.flow("processTask/surrogate", &s).await["code"]);
    assert_eq!(vec!["userA".to_string(), "userB".to_string()], task_row(&repo, hid).actor_ids);
    // addCandidate：同一腿
    let mut ac = HashMap::new();
    ac.insert("processTaskId".to_string(), json!(hid.to_string()));
    ac.insert("actorIds".to_string(), actors_json(&["userC"]));
    assert_eq!(0, facade.flow("processTask/addCandidate", &ac).await["code"]);
    assert_eq!(vec!["userA".to_string(), "userB".to_string(), "userC".to_string()], task_row(&repo, hid).actor_ids);
    // transfer：摘原人换新参与人，同一行不新建，留痕三件照旧
    let mut t = HashMap::new();
    t.insert("processTaskId".to_string(), json!(hid.to_string()));
    t.insert("operator".to_string(), json!("userA"));
    t.insert("fromActor".to_string(), json!("userA"));
    t.insert("toActor".to_string(), json!("userD"));
    t.insert("reason".to_string(), json!("转办原因"));
    assert_eq!(0, facade.flow("processTask/transfer", &t).await["code"]);
    let after = task_row(&repo, hid);
    assert!(!after.actor_ids.contains(&"userA".to_string()));
    assert!(after.actor_ids.contains(&"userD".to_string()));
    assert_eq!(hid, after.task_id);
    assert_eq!(Some(7), after.variables.get_i64("submitType"));
    assert!(after.variables.get("tf_transferHistory").is_some());
    assert!(after.actor_id.is_none(), "转办不写 actor_id 列");
    // 协办行不受主办转办影响，仍 DOING 且不驱动
    assert_eq!(1, aidant_rows(&repo, iid, &after).len());
    assert!(aidant_rows(&repo, iid, &after)[0].is_doing());
    // 新办理人办结照常推进
    assert_eq!(0, execute(&facade, &after, "userD", SUBMIT_AGREE).await["code"]);
    assert!(doing_named(&repo, iid, "final").is_some());
}

/// 权限码复用 wf:processTask:execute（OR 规则，照 246/247 B 案惯例，不建按钮种子）。
#[test]
fn aidant33_permission_code_rides_execute() {
    use jeeflow_core::spi::{ActionPermissionProvider, DefaultActionPermissionProvider};
    let provider = DefaultActionPermissionProvider;
    assert_eq!(vec!["wf:processTask:execute".to_string()],
        provider.permission_codes("processTask/aidant"));
    // 哨兵：没进表的 action 仍走默认派生
    assert_eq!(vec!["wf:processTask:transfer".to_string()],
        provider.permission_codes("processTask/transfer"));
}

/// 节点级 taskType=1 仍解析期禁配（issues/252 不因本轮运行期派生而放宽）。
#[test]
fn aidant34_node_level_secondary_still_banned() {
    use jeeflow_core::parser::ModelParser;
    let res = ModelParser::parse(&chain(r#""taskType":1"#));
    assert!(res.is_err(), "节点级协办配置必须解析期报错");
    let msg = res.unwrap_err().message();
    assert!(msg.contains("不允许配置协办任务类型"), "{}", msg);
}