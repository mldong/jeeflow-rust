//! issues/251「已办撤回 allowRecall 快照投影」——**引擎盖章侧**验收。
//!
//! 定案形状（办理时盖章）：执行任务时把**当前节点**的 `allowRecall` 开关值以键 `tf_allowRecall`
//! 写进执行 args（过滤字段权限之后、`complete_task` 之前）。守卫＝"当前节点是 Task 节点才盖，
//! 值＝开关 true/false"——**默认 false 也要盖键**。`tf_` 前缀使这枚键只落任务行变量、不传播进
//! 实例变量（`model::complete_task` 只并 `f_`）。出口投影那一层在 `jeeflow-facade`（见
//! `jeeflow-facade/tests/allow_recall_projection.rs` 与门面内联测试）。
//!
//! 夹具纪律（照 `tests/recall.rs`）：**全部走内联 JSON**，不碰共享 `flows/` 目录；
//! 用例各自用独立 define 名段（`allow-recall-stamp-NN`），互不共用夹具。
//!
//! 参考实现基准：jeeflow-java `core/RecallSupport.stampAllowRecall`
//! （`if (node instanceof TaskModel) args.put(FlowConst.ALLOW_RECALL, ((TaskModel) node).isAllowRecall())`）。

use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::{ProcessDefine, ProcessTask, TaskState};
use jeeflow_core::recall;
use jeeflow_core::spi::ProcessRepository;
use jeeflow_core::{FlowData, JeeflowEngineImpl, JsonValue, ServiceContext};
use std::sync::Arc;

const SUBMIT_APPLY: i64 = 0;
const SUBMIT_AGREE: i64 = 1;
const SUBMIT_REJECT: i64 = 2;

// ═══ 工装（照 tests/recall.rs）═══

fn as_engine() -> (JeeflowEngineImpl, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    (JeeflowEngineImpl::new(ctx), repo)
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

/// 通用五节点链 start→apply→review→final→end；参数是**审批节点 review** 的 properties 片段
/// （逐字对齐 `tests/recall.rs::chain` 的夹具形状：`assignee=userA` 恒定，追加片段只动 review）。
fn chain(review_props: &str) -> String {
    let r = if review_props.is_empty() {
        String::new()
    } else {
        format!(",{}", review_props)
    };
    format!(
        r#"{{
            "name": "allow-recall-stamp-inline",
            "displayName": "allowRecall 快照盖章夹具",
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

fn submit(type_: i64) -> FlowData {
    let mut args = FlowData::new();
    args.insert_i64("submitType", type_);
    args
}

fn doing_named(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> Option<ProcessTask> {
    repo.find_doing_tasks(iid, &[])
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

/// 发起并办结申请节点，返回停在 review 的实例 id。
fn start_to_review(engine: &JeeflowEngineImpl, repo: &Arc<MemoryRepository>, define_id: i64) -> i64 {
    let inst = engine
        .start_sync(define_id, "applicant", &FlowData::new())
        .unwrap();
    for t in repo
        .find_doing_tasks(inst.instance_id, &[])
        .unwrap()
        .into_iter()
        .filter(|t| t.task_name == "apply")
    {
        engine
            .execute_task_sync(t.task_id, "applicant", &submit(SUBMIT_APPLY))
            .unwrap();
    }
    inst.instance_id
}

/// 承重判据：`tf_` 键（含 `tf_allowRecall`）不得传播进实例变量——快照只落任务行。
fn assert_instance_has_no_tf_keys(repo: &Arc<MemoryRepository>, iid: i64) {
    let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
    let tf_keys: Vec<String> = inst
        .variables
        .iter()
        .filter(|(k, _)| k.starts_with("tf_"))
        .map(|(k, _)| k.clone())
        .collect();
    assert!(
        tf_keys.is_empty(),
        "tf_ 键不得传播进实例变量（快照只落任务行），实得 {:?}",
        tf_keys
    );
    assert!(
        inst.variables.get(recall::KEY_ALLOW_RECALL).is_none(),
        "实例变量不得含 tf_allowRecall"
    );
}

/// 读已办行上的快照键（恒为 bool；盖章写入的就是 JsonValue::Bool）。
fn stamp_of(row: &ProcessTask) -> Option<bool> {
    row.variables.get(recall::KEY_ALLOW_RECALL).and_then(|v| v.as_bool())
}

// ═══ 用例 ═══

/// 格 1：review 配 `allowRecall:true` ⇒ 正常办结后，已办 review 行变量盖 `tf_allowRecall=true`；
/// 实例变量零 `tf_` 键（不传播）。
#[test]
fn stamp01_review_on_writes_true_snapshot_to_finished_row() {
    let (engine, repo) = as_engine();
    let define_id = save_define(
        &repo,
        "allow-recall-stamp-01",
        &chain(r#""allowRecall":true"#),
    );
    let iid = start_to_review(&engine, &repo, define_id);

    let review = doing_named(&repo, iid, "review").expect("申请办结后应在 review 有待办");
    // DOING 行还没盖章（盖章发生在办结那一拍）
    assert_eq!(
        stamp_of(&review),
        None,
        "待办行尚未办理，不应有快照键"
    );

    engine
        .execute_task_sync(review.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();

    let old_review = finished_row(&repo, iid, "review").expect("review 应已办结");
    assert_eq!(
        old_review.variables.get(recall::KEY_ALLOW_RECALL),
        Some(&JsonValue::Bool(true)),
        "review 配 true ⇒ 已办行必须盖 tf_allowRecall=true（恒为 bool）"
    );
    assert_eq!(stamp_of(&old_review), Some(true));
    assert_instance_has_no_tf_keys(&repo, iid);
}

/// 格 2（承重判据·默认 false 也盖键）：review 不配 `allowRecall` ⇒ 办结后行变量盖
/// `tf_allowRecall=false`（缺键与 false 是两档，默认 false 也要有键）；申请节点（首任务节点，
/// 结构性恒 false）同样盖 false 键；实例变量零 `tf_` 键。
#[test]
fn stamp02_review_off_still_writes_false_key() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "allow-recall-stamp-02", &chain(""));
    let iid = start_to_review(&engine, &repo, define_id);

    // 申请节点＝首任务节点，解析期禁配 allowRecall ⇒ 恒 false，但**仍要盖 false 键**。
    let apply = finished_row(&repo, iid, "apply").expect("apply 应已办结");
    assert_eq!(
        apply.variables.get(recall::KEY_ALLOW_RECALL),
        Some(&JsonValue::Bool(false)),
        "首任务节点恒 false 也必须盖键（默认 false 也有键是承重判据）"
    );

    let review = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(review.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();

    let old_review = finished_row(&repo, iid, "review").unwrap();
    assert_eq!(
        old_review.variables.get(recall::KEY_ALLOW_RECALL),
        Some(&JsonValue::Bool(false)),
        "review 不配 ⇒ 已办行必须盖 tf_allowRecall=false（不是缺键）"
    );
    assert_eq!(stamp_of(&old_review), Some(false));
    assert_instance_has_no_tf_keys(&repo, iid);
}

/// 格 3（跳转腿也盖到键）：review 配 true，经 `execute_and_jump_sync` 跳到 final
/// （走 `execute_jump_inner` 编排）⇒ 被办结的 review 行同样盖 `tf_allowRecall=true`。
#[test]
fn stamp03_jump_leg_stamps_key_too() {
    let (engine, repo) = as_engine();
    let define_id = save_define(
        &repo,
        "allow-recall-stamp-03",
        &chain(r#""allowRecall":true"#),
    );
    let iid = start_to_review(&engine, &repo, define_id);

    let review = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_and_jump_sync(review.task_id, "userA", &submit(SUBMIT_AGREE), Some("final"))
        .expect("跳转到 final 应放行");

    let old_review = finished_row(&repo, iid, "review").expect("跳转腿也应办结当前 review 行");
    assert_eq!(
        old_review.variables.get(recall::KEY_ALLOW_RECALL),
        Some(&JsonValue::Bool(true)),
        "跳转腿（execute_jump_inner）同样盖当前节点快照"
    );
    assert!(
        doing_named(&repo, iid, "final").is_some(),
        "跳转后令牌应停在 final"
    );
    assert_instance_has_no_tf_keys(&repo, iid);
}

/// 格 4（驳回腿也盖到键）：review 配 true，经 `execute_and_jump_to_end_sync` 拒绝直推到结束
/// （走 `execute_and_jump_to_end_sync` 编排）⇒ 被办结的 review 行同样盖 `tf_allowRecall=true`。
#[test]
fn stamp04_reject_to_end_leg_stamps_key_too() {
    let (engine, repo) = as_engine();
    let define_id = save_define(
        &repo,
        "allow-recall-stamp-04",
        &chain(r#""allowRecall":true"#),
    );
    let iid = start_to_review(&engine, &repo, define_id);

    let review = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_and_jump_to_end_sync(review.task_id, "userA", &submit(SUBMIT_REJECT))
        .expect("拒绝直推结束应放行");

    let old_review = finished_row(&repo, iid, "review").expect("驳回腿也应办结当前 review 行");
    assert_eq!(
        old_review.variables.get(recall::KEY_ALLOW_RECALL),
        Some(&JsonValue::Bool(true)),
        "驳回腿（execute_and_jump_to_end_sync）同样盖当前节点快照"
    );
    assert_eq!(
        old_review.task_state,
        TaskState::Finished.code(),
        "被驳回的当前行是 FINISHED（不是废弃）"
    );
    assert_instance_has_no_tf_keys(&repo, iid);
}