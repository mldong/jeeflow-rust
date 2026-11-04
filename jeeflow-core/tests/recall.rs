//! 「已办撤回（`allowRecall`）」T0 验收——移植 jeeflow-java `test/RecallTest.java` 的 9 格语义
//! （＋一枚本栈自加的第十格：事件码 10 与复活行的码 3）。
//!
//! 契约正文：spec/02 §4.4（开关＋两条解析期禁配）、spec/04「已办撤回」（前置与窗口判据／
//! 动作三件／事件／语义边界）、spec/06 §2.7＋§processTask/recall（五条逐字失败文案表）、
//! spec/11 §11.3 码 10 `TASK_RECALL`。
//! 参考实现基准（逐字权威）：`jeeflow-java@09c2afe` 的
//! `core/RecallSupport.java`、`core/JeeflowEngineImpl.recallTask`（判据位序＋落库序，
//! 含 `14674d7` 补的"复活行另 fire 码 3"那一刀）、`domain/ProcessInstance.recallTask`。
//!
//! 夹具纪律（照 `add_sign_after.rs`）：**全部走内联 JSON**，不碰共享 `flows/` 目录
//! （那份目录是七语言仓镜像＋漂移门禁的核对对象，多一个文件就把十仓同步拖进这一轮）；
//! 用例各自用独立 define 名段（`recall-NN`），互不共用夹具。
//!
//! 「报错即零写入」三 faces（spec 04 判据位序的验收面）：每一格负向都真断言
//! ①当前行状态不变、②任务行总数不变、③实例状态不变——不写这三面的负向格等于没测。

use jeeflow_core::add_sign;
use jeeflow_core::event::{ProcessEvent, ProcessEventType};
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::{InstanceState, ProcessDefine, ProcessTask, TaskState};
use jeeflow_core::recall;
use jeeflow_core::spi::{ProcessEventListener, ProcessRepository};
use jeeflow_core::{FlowData, JeeflowEngineImpl, JsonValue, ServiceContext};
use std::sync::{Arc, Mutex};

const SUBMIT_APPLY: i64 = 0;
const SUBMIT_AGREE: i64 = 1;

// ═══ 工装 ═══

fn as_engine() -> (JeeflowEngineImpl, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    (JeeflowEngineImpl::new(ctx), repo)
}

/// 带事件录音机的引擎（第十格：码 10 与复活行码 3 的派发面）。
fn as_engine_recording() -> (JeeflowEngineImpl, Arc<MemoryRepository>, Arc<Recorder>) {
    let repo = Arc::new(MemoryRepository::new());
    let rec = Arc::new(Recorder::default());
    let mut ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    ctx.register_event_listener(rec.clone() as Arc<dyn ProcessEventListener>);
    (JeeflowEngineImpl::new(ctx), repo, rec)
}

#[derive(Default)]
struct Recorder(Mutex<Vec<String>>);

impl ProcessEventListener for Recorder {
    fn on_event(&self, event: &ProcessEvent) {
        self.0
            .lock()
            .unwrap()
            .push(format!("{}/{}", event.event_type.spec_name(), event.source_id));
    }
}

impl Recorder {
    fn labels(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
    fn reset(&self) {
        self.0.lock().unwrap().clear();
    }
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

/// 通用五节点链 start→apply→review→final→end；参数是**审批节点**的 properties 片段
/// （逐字对齐 java `RecallTest.chain` 的夹具形状：`assignee=userA` 恒定，追加片段只动 review）。
fn chain(review_props: &str) -> String {
    let r = if review_props.is_empty() {
        String::new()
    } else {
        format!(",{}", review_props)
    };
    format!(
        r#"{{
            "name": "recall-inline",
            "displayName": "已办撤回夹具",
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

/// 六节点链（多一级 pay），供"下游已办结"窗口档。
fn chain_deep(review_props: &str) -> String {
    let r = if review_props.is_empty() {
        String::new()
    } else {
        format!(",{}", review_props)
    };
    format!(
        r#"{{
            "name": "recalldeep-inline",
            "displayName": "已办撤回深链",
            "type": "approval",
            "nodes": [
                {{"id":"start","type":"snaker:start","properties":{{}}}},
                {{"id":"apply","type":"snaker:task","properties":{{"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"}}}},
                {{"id":"review","type":"snaker:task","properties":{{"form":"form-r","assignee":"userA","taskType":0,"performType":0{r}}}}},
                {{"id":"final","type":"snaker:task","properties":{{"form":"form-f","assignee":"userF","taskType":0,"performType":0}}}},
                {{"id":"pay","type":"snaker:task","properties":{{"form":"form-p","assignee":"userP","taskType":0,"performType":0}}}},
                {{"id":"end","type":"snaker:end","properties":{{}}}}
            ],
            "edges": [
                {{"id":"e1","type":"snaker:transition","sourceNodeId":"start","targetNodeId":"apply","properties":{{}}}},
                {{"id":"e2","type":"snaker:transition","sourceNodeId":"apply","targetNodeId":"review","properties":{{}}}},
                {{"id":"e3","type":"snaker:transition","sourceNodeId":"review","targetNodeId":"final","properties":{{}}}},
                {{"id":"e4","type":"snaker:transition","sourceNodeId":"final","targetNodeId":"pay","properties":{{}}}},
                {{"id":"e5","type":"snaker:transition","sourceNodeId":"pay","targetNodeId":"end","properties":{{}}}}
            ]
        }}"#
    )
}

/// 并行双支：review 出两条边到 payA/payB，各自汇入 end（java `chainFork` 同形）。
fn chain_fork(review_props: &str) -> String {
    let r = if review_props.is_empty() {
        String::new()
    } else {
        format!(",{}", review_props)
    };
    format!(
        r#"{{
            "name": "recallfork-inline",
            "displayName": "已办撤回并行",
            "type": "approval",
            "nodes": [
                {{"id":"start","type":"snaker:start","properties":{{}}}},
                {{"id":"apply","type":"snaker:task","properties":{{"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"}}}},
                {{"id":"review","type":"snaker:task","properties":{{"form":"form-r","assignee":"userA","taskType":0,"performType":0{r}}}}},
                {{"id":"payA","type":"snaker:task","properties":{{"form":"form-a","assignee":"userPA","taskType":0,"performType":0}}}},
                {{"id":"payB","type":"snaker:task","properties":{{"form":"form-b","assignee":"userPB","taskType":0,"performType":0}}}},
                {{"id":"end","type":"snaker:end","properties":{{}}}}
            ],
            "edges": [
                {{"id":"e1","type":"snaker:transition","sourceNodeId":"start","targetNodeId":"apply","properties":{{}}}},
                {{"id":"e2","type":"snaker:transition","sourceNodeId":"apply","targetNodeId":"review","properties":{{}}}},
                {{"id":"e3","type":"snaker:transition","sourceNodeId":"review","targetNodeId":"payA","properties":{{}}}},
                {{"id":"e4","type":"snaker:transition","sourceNodeId":"review","targetNodeId":"payB","properties":{{}}}},
                {{"id":"e5","type":"snaker:transition","sourceNodeId":"payA","targetNodeId":"end","properties":{{}}}},
                {{"id":"e6","type":"snaker:transition","sourceNodeId":"payB","targetNodeId":"end","properties":{{}}}}
            ]
        }}"#
    )
}

/// 审批节点 properties 任写；首任务节点那一格的夹具要改 apply 节点，故另开一支
/// （java `RecallTest` 里那一处 `replace` 的同位）。
fn chain_with_apply(apply_props: &str, review_props: &str) -> String {
    chain(review_props).replace(
        r#"{"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"}"#,
        &format!(
            r#"{{"form":"apply-form","taskType":0,"performType":0,{}}}"#,
            apply_props
        ),
    )
}

fn submit(type_: i64) -> FlowData {
    let mut args = FlowData::new();
    args.insert_i64("submitType", type_);
    args
}

fn agree_with_add_sign(operators: &str) -> FlowData {
    let mut args = submit(SUBMIT_AGREE);
    args.insert_str(add_sign::ADD_SIGN_OPERATORS, operators);
    args
}

fn doing_all_named(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> Vec<ProcessTask> {
    repo.find_doing_tasks(iid, &[])
        .unwrap()
        .into_iter()
        .filter(|t| t.task_name == name)
        .collect()
}

fn doing_named(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> Option<ProcessTask> {
    doing_all_named(repo, iid, name).into_iter().next()
}

fn doing_count(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> usize {
    doing_all_named(repo, iid, name).len()
}

/// 不按状态滤——撤回后的下游行是 ABANDON(99)，不走 FINISHED（java `anyRow` 同位）。
fn any_row(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> Option<ProcessTask> {
    repo.find_history_tasks(iid)
        .unwrap()
        .into_iter()
        .find(|t| t.task_name == name)
}

fn finished_row(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> Option<ProcessTask> {
    repo.find_history_tasks(iid)
        .unwrap()
        .into_iter()
        .find(|t| t.task_name == name && t.is_finished())
}

fn rows_named(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> usize {
    repo.find_history_tasks(iid)
        .unwrap()
        .iter()
        .filter(|t| t.task_name == name)
        .count()
}

fn rows_total(repo: &Arc<MemoryRepository>, iid: i64) -> usize {
    repo.find_history_tasks(iid).unwrap().len()
}

fn task_row(repo: &Arc<MemoryRepository>, tid: i64) -> ProcessTask {
    repo.find_task_by_id(tid).unwrap().unwrap()
}

fn instance_state(repo: &Arc<MemoryRepository>, iid: i64) -> i32 {
    repo.find_instance_by_id(iid).unwrap().unwrap().state
}

/// 发起并办结申请节点，返回停在 review 的实例 id。
fn start_to_review(engine: &JeeflowEngineImpl, repo: &Arc<MemoryRepository>, define_id: i64) -> i64 {
    let inst = engine
        .start_sync(define_id, "applicant", &FlowData::new())
        .unwrap();
    for t in doing_all_named(repo, inst.instance_id, "apply") {
        engine
            .execute_task_sync(t.task_id, "applicant", &submit(SUBMIT_APPLY))
            .unwrap();
    }
    inst.instance_id
}

fn entry<'a>(hist: &'a JsonValue, key: &str) -> &'a JsonValue {
    hist.as_object()
        .and_then(|entries| entries.iter().find(|(k, _)| k == key).map(|(_, v)| v))
        .unwrap_or_else(|| panic!("账本条目缺键 {}: {:?}", key, hist))
}

/// 报错即零写入的**三个面**（spec 04 判据位序；缺任何一面都等于没测）。
fn assert_zero_write(
    repo: &Arc<MemoryRepository>,
    iid: i64,
    target_before: &ProcessTask,
    rows_before: usize,
    state_before: i32,
) {
    let target_now = task_row(repo, target_before.task_id);
    assert_eq!(
        target_before.task_state, target_now.task_state,
        "零写入面①：目标行状态被改动（{} → {}）",
        target_before.task_state, target_now.task_state
    );
    assert_eq!(
        rows_before,
        rows_total(repo, iid),
        "零写入面②：任务行总数变了 ⇒ 复活行/废弃行已落库"
    );
    assert_eq!(
        state_before,
        instance_state(repo, iid),
        "零写入面③：实例状态被改动"
    );
    assert!(
        recall::recall_history_of(&target_now).is_empty(),
        "零写入面①的补充：目标行不得已经盖上 tf_recallHistory"
    );
}

// ═══ 用例 ═══

/// 格 1（正向）：办结→撤回→原行留账本、复活行名册照旧、下游废弃、重办后再推进。
#[test]
fn recall01_revives_abandons_downstream_and_allows_redo() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "recall-01", &chain(r#""allowRecall":true"#));
    let iid = start_to_review(&engine, &repo, define_id);

    let review = doing_named(&repo, iid, "review").expect("申请办结后应在审批节点有待办");
    engine
        .execute_task_sync(review.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    let old_review = finished_row(&repo, iid, "review").unwrap();
    assert_eq!(1, doing_count(&repo, iid, "final"), "下游 final 应有一条待办");

    let resurrected = engine
        .recall_task_sync(old_review.task_id, "userA")
        .expect("撤回应当放行");

    // 动作三件·第 3 件：同节点新行 DOING、参与者＝原行名册
    assert_eq!("review", resurrected.task_name);
    assert!(resurrected.is_doing());
    assert!(
        resurrected.actor_ids.contains(&"userA".to_string()),
        "复活行名册照旧：{:?}",
        resurrected.actor_ids
    );
    assert_eq!(
        old_review.parent_task_id, resurrected.parent_task_id,
        "复活行的 parent＝原行的 parent（原地顶替，不是往下走一步）"
    );
    // 动作三件·第 1 件：下游 DOING 行废弃
    let final_row = any_row(&repo, iid, "final").unwrap();
    assert_eq!(TaskState::Abandon.code(), final_row.task_state);
    assert_eq!(0, doing_count(&repo, iid, "final"));
    assert_eq!(
        1,
        doing_count(&repo, iid, "review"),
        "复活行是本节点唯一活待办"
    );
    // 动作三件·第 2 件：原 FINISHED 行不改写，只盖追加账本
    let old_now = task_row(&repo, old_review.task_id);
    assert!(old_now.is_finished(), "原行必须仍是 FINISHED（历史即任务行）");
    let ledger = recall::recall_history_of(&old_now);
    assert_eq!(1, ledger.len());
    assert_eq!("userA", entry(&ledger[0], "operator").as_str().unwrap());
    assert_eq!(
        Some(resurrected.task_id),
        entry(&ledger[0], "recalledTaskId").as_i64(),
        "recalledTaskId 指向复活行（从历史一步跳到唯一活待办）"
    );
    assert!(
        entry(&ledger[0], "time")
            .as_str()
            .map(|s| s.len() == 19)
            .unwrap_or(false),
        "time 走 §2.4 全生态格式 yyyy-MM-dd HH:mm:ss：{:?}",
        entry(&ledger[0], "time")
    );
    // 实例仍进行中；重办后照常推进
    assert_eq!(InstanceState::Doing.code(), instance_state(&repo, iid));
    engine
        .execute_task_sync(resurrected.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(doing_named(&repo, iid, "final").is_some());
    assert_eq!(
        2,
        rows_named(&repo, iid, "final"),
        "重办产出第二条 final 行（废弃的那条留作出证）"
    );
}

/// 格 2（窗口·宽口径）：下游已办结 ⇒ 逐字文案报错**且零写入三面**。
#[test]
fn recall02_blocked_when_downstream_finished_with_zero_write() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "recall-02", &chain_deep(r#""allowRecall":true"#));
    let iid = start_to_review(&engine, &repo, define_id);

    let review = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(review.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    let final_task = doing_named(&repo, iid, "final").unwrap();
    engine
        .execute_task_sync(final_task.task_id, "userF", &submit(SUBMIT_AGREE))
        .unwrap();
    let old_review = finished_row(&repo, iid, "review").unwrap();
    assert!(
        doing_named(&repo, iid, "pay").is_some(),
        "下游 final 已办结、令牌停在 pay"
    );

    let before = task_row(&repo, old_review.task_id);
    let rows_before = rows_total(&repo, iid);
    let state_before = instance_state(&repo, iid);

    let err = engine
        .recall_task_sync(old_review.task_id, "userA")
        .expect_err("应拦：下游 final 已办结");
    assert_eq!("任务[review]的后续节点已办理，不能撤回", err.message());

    assert_zero_write(&repo, iid, &before, rows_before, state_before);
    assert!(
        finished_row(&repo, iid, "final").is_some(),
        "下游 final 仍 FINISHED（判据没顺手改它）"
    );
    assert!(doing_named(&repo, iid, "pay").is_some(), "pay 待办原样保留");
    assert_eq!(0, doing_count(&repo, iid, "review"), "不得建出复活行");
}

/// 格 3（开关未开）：逐字文案。
#[test]
fn recall03_blocked_when_switch_off() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "recall-03", &chain(""));
    let iid = start_to_review(&engine, &repo, define_id);

    let review = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(review.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    let old_review = finished_row(&repo, iid, "review").unwrap();

    let before = task_row(&repo, old_review.task_id);
    let rows_before = rows_total(&repo, iid);
    let state_before = instance_state(&repo, iid);

    let err = engine
        .recall_task_sync(old_review.task_id, "userA")
        .expect_err("应拦：开关未开");
    assert_eq!("节点[review]未开启 allowRecall，不能撤回", err.message());
    assert_zero_write(&repo, iid, &before, rows_before, state_before);
}

/// 格 4（权限档＋未办结档）：非参与者拦、`flow.admin` 特权放行、DOING 行拦。
#[test]
fn recall04_permission_and_not_finished_guards() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "recall-04", &chain(r#""allowRecall":true"#));
    let iid = start_to_review(&engine, &repo, define_id);

    let review = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(review.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    let old_review = finished_row(&repo, iid, "review").unwrap();

    // ⚠️ 承重判据 3 的同位：参与人判断**按名册**（FINISHED 行走不了只认 DOING 的 is_allowed）
    let before = task_row(&repo, old_review.task_id);
    let rows_before = rows_total(&repo, iid);
    let state_before = instance_state(&repo, iid);
    let err = engine
        .recall_task_sync(old_review.task_id, "userZ")
        .expect_err("应拦：非参与者");
    assert_eq!("无权限撤回该任务", err.message());
    assert_zero_write(&repo, iid, &before, rows_before, state_before);

    // flow.admin 特权放行（与转办/摘人同口径）
    let resurrected = engine
        .recall_task_sync(old_review.task_id, "flow.admin")
        .expect("flow.admin 应放行");
    assert!(resurrected.is_doing());
    assert_eq!(1, doing_count(&repo, iid, "review"));
    let ledger = recall::recall_history_of(&task_row(&repo, old_review.task_id));
    assert_eq!(
        "flow.admin",
        entry(&ledger[0], "operator").as_str().unwrap(),
        "账本记的是实际操作人，不是缺省账号"
    );

    // 进行中行拦（复活行还没办结）
    let err = engine
        .recall_task_sync(resurrected.task_id, "userA")
        .expect_err("应拦：目标行未办结");
    assert_eq!("任务非已完成状态，不可撤回", err.message());
}

/// 格 5（实例非 DOING）：实例已走完 ⇒ 逐字文案。
#[test]
fn recall05_blocked_when_instance_finished() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "recall-05", &chain(r#""allowRecall":true"#));
    let iid = start_to_review(&engine, &repo, define_id);

    let review = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(review.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    let final_task = doing_named(&repo, iid, "final").unwrap();
    engine
        .execute_task_sync(final_task.task_id, "userF", &submit(SUBMIT_AGREE))
        .unwrap();
    assert_eq!(
        InstanceState::Finished.code(),
        instance_state(&repo, iid),
        "夹具应把实例推到 FINISHED"
    );
    let old_review = finished_row(&repo, iid, "review").unwrap();

    let before = task_row(&repo, old_review.task_id);
    let rows_before = rows_total(&repo, iid);
    let state_before = instance_state(&repo, iid);
    let err = engine
        .recall_task_sync(old_review.task_id, "userA")
        .expect_err("应拦：实例已结束");
    assert_eq!("流程实例非进行中状态，不可撤回", err.message());
    assert_zero_write(&repo, iid, &before, rows_before, state_before);
}

/// 格 6（后加签行属下游 DOING）：连带废弃、不留孤儿待办；复活后可正常推进。
#[test]
fn recall06_abandons_add_sign_rows_too() {
    let (engine, repo) = as_engine();
    let define_id = save_define(
        &repo,
        "recall-06",
        &chain(r#""allowRecall":true,"allowAddSign":true"#),
    );
    let iid = start_to_review(&engine, &repo, define_id);

    let review = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(review.task_id, "userA", &agree_with_add_sign("userX"))
        .unwrap();
    let add_sign_row = doing_named(&repo, iid, "review").expect("被加签人应在同一节点有待办");
    assert!(add_sign_row.actor_ids.contains(&"userX".to_string()));
    let old_review = finished_row(&repo, iid, "review").unwrap();

    engine
        .recall_task_sync(old_review.task_id, "userA")
        .expect("加签行属下游 DOING，天然落进窗口闭包");

    assert_eq!(
        TaskState::Abandon.code(),
        task_row(&repo, add_sign_row.task_id).task_state,
        "加签行连带废弃（宽口径血缘闭包，不为其另开一档判据）"
    );
    assert_eq!(
        1,
        doing_count(&repo, iid, "review"),
        "复活行是节点上唯一 DOING"
    );

    // 复活行照常推进（无兄弟待办 ⇒ merged 出边）
    let resurrected = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(resurrected.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(doing_named(&repo, iid, "final").is_some());
}

/// 格 7（并行分支）：撤回后两支全部废弃。
#[test]
fn recall07_abandons_both_fork_branches() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "recall-07", &chain_fork(r#""allowRecall":true"#));
    let iid = start_to_review(&engine, &repo, define_id);

    let review = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(review.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(doing_named(&repo, iid, "payA").is_some());
    assert!(doing_named(&repo, iid, "payB").is_some());
    let old_review = finished_row(&repo, iid, "review").unwrap();

    engine
        .recall_task_sync(old_review.task_id, "userA")
        .expect("并行两支都属下游 DOING，应全部废弃");

    assert_eq!(
        TaskState::Abandon.code(),
        any_row(&repo, iid, "payA").unwrap().task_state
    );
    assert_eq!(
        TaskState::Abandon.code(),
        any_row(&repo, iid, "payB").unwrap().task_state
    );
    assert_eq!(1, doing_count(&repo, iid, "review"));
    assert_eq!(0, doing_count(&repo, iid, "payA"));
    assert_eq!(0, doing_count(&repo, iid, "payB"));
}

/// 格 8（解析期禁配）：会签节点／申请节点配 `allowRecall` 直接报错（不做运行时降级）。
#[test]
fn recall08_parse_bans_allow_recall_on_countersign_and_apply_nodes() {
    // 会签节点：夹具里 performType 后写的 1 覆盖前写的 0（properties 收进 HashMap，末值生效）
    let (engine, repo) = as_engine();
    let define_id = save_define(
        &repo,
        "recall-08a",
        &chain(r#""allowRecall":true,"performType":1,"countersignType":"PARALLEL""#),
    );
    let err = engine
        .start_sync(define_id, "applicant", &FlowData::new())
        .expect_err("应拦：会签节点禁配");
    assert!(
        err.message().contains("不允许配置 allowRecall"),
        "逐字文案：{}",
        err.message()
    );
    assert!(err.message().contains("会签节点"), "逐字文案：{}", err.message());
    assert_eq!(
        "会签节点[review]不允许配置 allowRecall：撤\"谁的一票\"语义不清",
        err.message()
    );

    // 申请节点＝start 直接后继
    let (engine2, repo2) = as_engine();
    let define_id2 = save_define(
        &repo2,
        "recall-08b",
        &chain_with_apply(r#""assignee":"applicant","allowRecall":true"#, ""),
    );
    let err2 = engine2
        .start_sync(define_id2, "applicant", &FlowData::new())
        .expect_err("应拦：申请节点禁配");
    assert!(err2.message().contains("首任务节点"), "{}", err2.message());
    assert!(err2.message().contains("allowRecall"), "{}", err2.message());
    assert_eq!(
        "首任务节点[apply]不允许配置 allowRecall：申请节点走实例撤回＋重新发起，不需要同实例复活",
        err2.message()
    );

    // 非法值同样在解析期拦下（三档同一把尺子 `parse_allow_keyed`，不得静默当缺省）
    let (engine3, repo3) = as_engine();
    let define_id3 = save_define(&repo3, "recall-08c", &chain(r#""allowRecall":"maybe""#));
    let err3 = engine3
        .start_sync(define_id3, "applicant", &FlowData::new())
        .expect_err("应拦：allowRecall 值非法");
    assert_eq!("节点[review]的 allowRecall 配置非法: maybe", err3.message());
}

/// 格 9（逐字文案常量自证）：防常量被改后测试仍绿（spec 06 §processTask/recall 失败文案表）。
#[test]
fn recall09_pinned_messages_match_spec() {
    assert_eq!(
        "任务[review]的后续节点已办理，不能撤回",
        recall::msg_window("review")
    );
    assert_eq!(
        "节点[review]未开启 allowRecall，不能撤回",
        recall::msg_switch_off("review")
    );
    assert_eq!("任务非已完成状态，不可撤回", recall::MSG_NOT_FINISHED);
    assert_eq!("无权限撤回该任务", recall::MSG_NO_PERMISSION);
    assert_eq!("流程实例非进行中状态，不可撤回", recall::MSG_INSTANCE_NOT_DOING);
    assert_eq!("流程实例不存在", recall::MSG_INSTANCE_NOT_FOUND);
}

/// 格 10（本栈自加·事件面）：撤回 fire **一次**码 10（sourceId＝被撤回的旧行），
/// 复活行的待办另走码 3（java `14674d7` 补的那一刀）；**不补发 5/6**。
#[test]
fn recall10_fires_code_10_once_and_code_3_for_resurrected_row() {
    let (engine, repo, rec) = as_engine_recording();
    let define_id = save_define(&repo, "recall-10", &chain(r#""allowRecall":true"#));
    let iid = start_to_review(&engine, &repo, define_id);

    let review = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(review.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    let old_review = finished_row(&repo, iid, "review").unwrap();
    rec.reset();

    let resurrected = engine
        .recall_task_sync(old_review.task_id, "userA")
        .unwrap();
    let labels = rec.labels();

    let recall_code = format!("TASK_RECALL/{}", old_review.task_id);
    assert_eq!(
        1,
        labels.iter().filter(|l| **l == recall_code).count(),
        "码 10 每轮撤回只 fire 一次，sourceId＝被撤回的旧行：{:?}",
        labels
    );
    assert_eq!(
        ProcessEventType::TaskRecall.code(),
        10,
        "spec 11 §11.3 码表 A 套：10 号位即 TASK_RECALL"
    );
    assert!(
        labels
            .iter()
            .any(|l| *l == format!("PROCESS_TASK_START/{}", resurrected.task_id)),
        "复活行自己的待办另走码 3：{:?}",
        labels
    );
    for forbidden in ["TASK_COMPLETE", "TASK_REJECT"] {
        assert!(
            !labels.iter().any(|l| l.starts_with(forbidden)),
            "撤回不是办结也不是拒绝 ⇒ 只发 10 不补发 5/6：{:?}",
            labels
        );
    }
}
