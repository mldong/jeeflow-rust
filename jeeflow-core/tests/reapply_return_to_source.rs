//! 「退回修改、重提直返」验收（对齐 jeeflow-java `ReturnToSourceTest`）。
//!
//! 夹具：`flows/14|15|16-return-to-source*.json`（与 java 共享目录逐字同源）；
//! 非法配置夹具单独放 `jeeflow-core/tests/flows-invalid/`——**不进共享 `flows/`**，
//! 因为别的套件会解析 `flows/` 里的每一份定义。

use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::ProcessDefine;
use jeeflow_core::parser::ModelParser;
use jeeflow_core::reapply::{self, ReapplyMode};
use jeeflow_core::spi::ProcessRepository;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::{FlowData, JeeflowEngineImpl, JeeflowResult, ProcessTask, ServiceContext};
use std::sync::Arc;

const SUBMIT_APPLY: i64 = 0;
const SUBMIT_AGREE: i64 = 1;
const SUBMIT_REJECT: i64 = 2;
const SUBMIT_ROLLBACK: i64 = 3;
const SUBMIT_JUMP: i64 = 4;
const SUBMIT_REAPPLY: i64 = 5;
const SUBMIT_RETURN: i64 = 6;

fn rts_engine() -> (JeeflowEngineImpl, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    (JeeflowEngineImpl::new(ctx), repo)
}

/// 共享夹具目录＝workspace 根的 `flows/`（引擎用例与 java 源逐字同源）。
fn load_flow(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../flows")
        .join(format!("{}.json", name));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {}", path.display(), e))
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

fn submit(type_: i64) -> FlowData {
    let mut args = FlowData::new();
    args.insert_i64("submitType", type_);
    args
}

/// 发起并办掉申请节点（operator=applicant，与 java 测试同形）。
fn start_flow(
    engine: &JeeflowEngineImpl,
    repo: &Arc<MemoryRepository>,
    define_id: i64,
    args: &FlowData,
) -> i64 {
    let inst = engine.start_sync(define_id, "applicant", args).unwrap();
    let apply = doing_named(repo, inst.instance_id, "apply").expect("发起后应有申请待办");
    engine
        .execute_task_sync(apply.task_id, "applicant", &submit(SUBMIT_APPLY))
        .unwrap();
    inst.instance_id
}

fn doing_named(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> Option<jeeflow_core::ProcessTask> {
    repo.find_doing_tasks(iid, &[])
        .unwrap()
        .into_iter()
        .find(|t| t.task_name == name)
}

fn doing_all(repo: &Arc<MemoryRepository>, iid: i64) -> Vec<jeeflow_core::ProcessTask> {
    repo.find_doing_tasks(iid, &[]).unwrap()
}

fn rows(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> usize {
    repo.find_history_tasks(iid)
        .unwrap()
        .iter()
        .filter(|t| t.task_name == name)
        .count()
}

fn instance_state(repo: &Arc<MemoryRepository>, iid: i64) -> i32 {
    repo.find_instance_by_id(iid).unwrap().unwrap().state
}

fn task_row(repo: &Arc<MemoryRepository>, tid: i64) -> jeeflow_core::ProcessTask {
    repo.find_task_by_id(tid).unwrap().unwrap()
}

fn define_content(repo: &Arc<MemoryRepository>, define_id: i64) -> String {
    repo.find_define_by_id(define_id).unwrap().unwrap().content_str()
}

// ═══ 10.1 基本直返 ═══

#[test]
fn test_rts_simple_return_then_reapply() {
    let (engine, repo) = rts_engine();
    let define_id = save_define(&repo, "return-to-source", &load_flow("14-return-to-source"));
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let task_a = doing_named(&repo, iid, "taskA").expect("申请后进入 taskA");

    engine
        .execute_and_jump_to_first_sync(task_a.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();

    // 来源任务完成留痕，实例仍是同一个且进行中
    let finished_a = task_row(&repo, task_a.task_id);
    assert!(finished_a.is_finished(), "来源任务应已完成");
    assert_eq!(finished_a.process_instance_id, iid);
    assert_eq!(instance_state(&repo, iid), 10);

    // 生成新的申请待办，携带直返上下文；不生成来源之后的任务
    let apply = doing_named(&repo, iid, "apply").expect("退回后应有申请待办");
    let ctx = reapply::reapply_of(&apply.variables).expect("申请待办应带直返上下文");
    assert_eq!(ctx.mode, "RETURN_TO_SOURCE");
    assert_eq!(ctx.source_task_id, task_a.task_id);
    assert_eq!(ctx.source_node, "taskA");
    assert_eq!(
        reapply::scopes_of(&apply.variables),
        Vec::new(),
        "申请待办不在任何分支内"
    );
    assert_eq!(doing_all(&repo, iid).len(), 1);
    assert!(
        doing_named(&repo, iid, "taskB").is_none(),
        "退回不生成后续节点任务"
    );

    // 重提：直接回到来源节点，不经过来源之前的节点
    engine
        .execute_task_sync(apply.task_id, "applicant", &submit(SUBMIT_REAPPLY))
        .unwrap();
    assert!(task_row(&repo, apply.task_id).is_finished());
    let new_a = doing_named(&repo, iid, "taskA").expect("重提应在来源节点新建待办");
    assert_eq!(
        new_a.parent_task_id,
        Some(apply.task_id),
        "新任务血缘指向本轮申请任务"
    );
    assert_eq!(doing_all(&repo, iid).len(), 1);
    assert!(
        doing_named(&repo, iid, "taskB").is_none(),
        "重提不得生成来源之后的节点"
    );
    assert_eq!(rows(&repo, iid, "taskA"), 2, "旧来源任务保留为历史行");

    // 新 taskA 通过后继续后续流程
    engine
        .execute_task_sync(new_a.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    let task_b = doing_named(&repo, iid, "taskB").expect("taskA 通过后进入 taskB");
    engine
        .execute_task_sync(task_b.task_id, "userB", &submit(SUBMIT_AGREE))
        .unwrap();
    assert_eq!(instance_state(&repo, iid), 20, "后续流程正常办结");
}

#[test]
fn test_rts_reapply_rejects_other_submit_types() {
    let (engine, repo) = rts_engine();
    let define_id = save_define(&repo, "return-to-source", &load_flow("14-return-to-source"));
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let task_a = doing_named(&repo, iid, "taskA").unwrap();
    engine
        .execute_and_jump_to_first_sync(task_a.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();
    let apply = doing_named(&repo, iid, "apply").unwrap();

    let err = engine
        .execute_task_sync(apply.task_id, "applicant", &submit(SUBMIT_AGREE))
        .unwrap_err();
    assert!(
        err.message().contains("重新提交"),
        "拒绝文案须点名只接受重新提交：{}",
        err.message()
    );
    assert!(
        doing_named(&repo, iid, "apply").is_some(),
        "拒绝后申请待办仍可处理"
    );
    assert!(
        doing_named(&repo, iid, "taskA").is_none(),
        "拒绝即不建来源任务"
    );

    // 非办理人重提同样被拦
    let err = engine
        .execute_task_sync(apply.task_id, "userB", &submit(SUBMIT_REAPPLY))
        .unwrap_err();
    assert!(
        err.message().contains("not allowed"),
        "非办理人不得重提：{}",
        err.message()
    );
    assert!(doing_named(&repo, iid, "apply").is_some());
}

// ═══ 10.2 条件分支不重算 ═══

#[test]
fn test_rts_condition_not_reevaluated() {
    let (engine, repo) = rts_engine();
    let define_id = save_define(
        &repo,
        "return-to-source-decision",
        &load_flow("16-return-to-source-decision"),
    );
    let mut args = FlowData::new();
    args.insert_i64("amount", 5000);
    let iid = start_flow(&engine, &repo, define_id, &args);
    let high = doing_named(&repo, iid, "taskHigh").expect("金额>1000 应走 taskHigh");
    assert!(doing_named(&repo, iid, "taskLow").is_none());

    engine
        .execute_and_jump_to_first_sync(high.task_id, "userH", &submit(SUBMIT_RETURN))
        .unwrap();
    // 申请人把条件字段改成 100，重提仍回 taskHigh（不重算来源前的条件）
    let mut reapply_args = submit(SUBMIT_REAPPLY);
    reapply_args.insert_i64("amount", 100);
    let apply = doing_named(&repo, iid, "apply").unwrap();
    engine
        .execute_task_sync(apply.task_id, "applicant", &reapply_args)
        .unwrap();
    let again = doing_named(&repo, iid, "taskHigh").expect("重提必须回原来源节点");
    assert!(
        doing_named(&repo, iid, "taskLow").is_none(),
        "不得按修改后的条件改走 taskLow"
    );

    // 来源之后的二级条件按正常数据求值：level>1 走 taskX
    let mut agree = submit(SUBMIT_AGREE);
    agree.insert_i64("level", 2);
    engine
        .execute_task_sync(again.task_id, "userH", &agree)
        .unwrap();
    assert!(
        doing_named(&repo, iid, "taskX").is_some(),
        "来源后的条件节点正常执行"
    );
    assert!(doing_named(&repo, iid, "taskY").is_none());
}

// ═══ 10.3 并行分支 ═══

#[test]
fn test_rts_join_waits_while_applicant_editing() {
    let (engine, repo) = rts_engine();
    let define_id = save_define(
        &repo,
        "return-to-source-fork",
        &load_flow("15-return-to-source-fork"),
    );
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let task_a = doing_named(&repo, iid, "taskA").unwrap();
    let task_b = doing_named(&repo, iid, "taskB").unwrap();
    assert_ne!(task_a.task_id, task_b.task_id);

    engine
        .execute_and_jump_to_first_sync(task_a.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();
    assert!(doing_named(&repo, iid, "apply").is_some());
    assert!(
        doing_named(&repo, iid, "taskB").is_some(),
        "另一分支可继续"
    );

    // taskB 办完到汇合点：申请人还在修改，不得放行汇合
    engine
        .execute_task_sync(task_b.task_id, "userB", &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(
        doing_named(&repo, iid, "taskC").is_none(),
        "来源分支仍在申请人手里，汇合不得放行"
    );
    assert_eq!(instance_state(&repo, iid), 10);

    // 重提只创建新 taskA，不重建 taskB、不重触发分叉
    let apply = doing_named(&repo, iid, "apply").unwrap();
    engine
        .execute_task_sync(apply.task_id, "applicant", &submit(SUBMIT_REAPPLY))
        .unwrap();
    let new_a = doing_named(&repo, iid, "taskA").expect("重提回到来源节点");
    assert!(doing_named(&repo, iid, "taskB").is_none(), "不重建 taskB");
    assert_eq!(rows(&repo, iid, "taskB"), 1, "taskB 已办记录不变");
    assert!(
        doing_named(&repo, iid, "taskC").is_none(),
        "重提本身不放行汇合"
    );

    // 新 taskA 办完才汇合，且只放行一次
    engine
        .execute_task_sync(new_a.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    let doing = doing_all(&repo, iid);
    assert_eq!(
        doing.len(),
        1,
        "汇合后只建一条负责人待办：{:?}",
        doing.iter().map(|t| t.task_name.clone()).collect::<Vec<_>>()
    );
    assert_eq!(doing[0].task_name, "taskC");
    assert_eq!(rows(&repo, iid, "taskC"), 1);
}

#[test]
fn test_rts_join_after_source_completes_first() {
    let (engine, repo) = rts_engine();
    let define_id = save_define(
        &repo,
        "return-to-source-fork",
        &load_flow("15-return-to-source-fork"),
    );
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let task_a = doing_named(&repo, iid, "taskA").unwrap();
    let task_b = doing_named(&repo, iid, "taskB").unwrap();
    engine
        .execute_and_jump_to_first_sync(task_a.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();
    let apply = doing_named(&repo, iid, "apply").unwrap();
    engine
        .execute_task_sync(apply.task_id, "applicant", &submit(SUBMIT_REAPPLY))
        .unwrap();
    let new_a = doing_named(&repo, iid, "taskA").unwrap();
    engine
        .execute_task_sync(new_a.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(
        doing_named(&repo, iid, "taskC").is_none(),
        "另一支未办完，不得先放行"
    );
    engine
        .execute_task_sync(task_b.task_id, "userB", &submit(SUBMIT_AGREE))
        .unwrap();
    assert_eq!(rows(&repo, iid, "taskC"), 1, "另一种完成顺序同样只放行一次");
}

#[test]
fn test_rts_two_branches_return_independently() {
    let (engine, repo) = rts_engine();
    let define_id = save_define(
        &repo,
        "return-to-source-fork",
        &load_flow("15-return-to-source-fork"),
    );
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let task_a = doing_named(&repo, iid, "taskA").unwrap();
    engine
        .execute_and_jump_to_first_sync(task_a.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();
    let task_b = doing_named(&repo, iid, "taskB").unwrap();
    engine
        .execute_and_jump_to_first_sync(task_b.task_id, "userB", &submit(SUBMIT_RETURN))
        .unwrap();

    let applies = repo
        .find_doing_tasks(iid, &["apply".to_string()])
        .unwrap();
    assert_eq!(applies.len(), 2, "两条分支分别退回产生两条申请待办");
    let apply_a = applies
        .iter()
        .find(|t| {
            reapply::reapply_of(&t.variables)
                .map(|c| c.source_node == "taskA")
                .unwrap_or(false)
        })
        .expect("应有绑定 taskA 的申请待办");
    let apply_b = applies
        .iter()
        .find(|t| {
            reapply::reapply_of(&t.variables)
                .map(|c| c.source_node == "taskB")
                .unwrap_or(false)
        })
        .expect("应有绑定 taskB 的申请待办");

    // 只恢复 A 的来源：B 仍在等待
    engine
        .execute_task_sync(apply_a.task_id, "applicant", &submit(SUBMIT_REAPPLY))
        .unwrap();
    assert_eq!(
        repo.find_doing_tasks(iid, &["apply".to_string()])
            .unwrap()
            .len(),
        1,
        "B 的申请待办不受影响"
    );
    let new_a = doing_named(&repo, iid, "taskA").unwrap();
    engine
        .execute_task_sync(new_a.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(
        doing_named(&repo, iid, "taskC").is_none(),
        "B 仍在修改，汇合不得放行"
    );

    engine
        .execute_task_sync(apply_b.task_id, "applicant", &submit(SUBMIT_REAPPLY))
        .unwrap();
    let new_b = doing_named(&repo, iid, "taskB").expect("重提回到 B 的来源节点");
    engine
        .execute_task_sync(new_b.task_id, "userB", &submit(SUBMIT_AGREE))
        .unwrap();
    assert_eq!(rows(&repo, iid, "taskC"), 1, "两支都恢复后汇合只放行一次");
}

// ═══ 重复退回 / 回归 / 非法配置 / 分支归属 ═══

#[test]
fn test_rts_repeated_return_binds_its_own_source_task() {
    let (engine, repo) = rts_engine();
    let define_id = save_define(&repo, "return-to-source", &load_flow("14-return-to-source"));
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let task_a = doing_named(&repo, iid, "taskA").unwrap();
    engine
        .execute_and_jump_to_first_sync(task_a.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();
    let apply1 = doing_named(&repo, iid, "apply").unwrap();
    // 缺省 submitType ⇒ 按 5（重新提交）处理
    engine
        .execute_task_sync(apply1.task_id, "applicant", &FlowData::new())
        .unwrap();
    let round2 = doing_named(&repo, iid, "taskA").expect("缺省 submitType 也按重提处理");

    // 第二轮再退回，绑定本轮来源
    engine
        .execute_and_jump_to_first_sync(round2.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();
    let apply2 = doing_named(&repo, iid, "apply").unwrap();
    let ctx = reapply::reapply_of(&apply2.variables).unwrap();
    assert_eq!(ctx.source_task_id, round2.task_id, "重复退回绑定本轮来源");
    assert!(
        !task_row(&repo, apply2.task_id).is_finished(),
        "新申请待办仍待办"
    );
    assert_eq!(rows(&repo, iid, "taskA"), 2);
}

#[test]
fn test_rts_legacy_flow_unchanged() {
    // 回归：未配置 reapplyMode 的旧流程，退回/重提仍按原路（重新分叉）
    let (engine, repo) = rts_engine();
    let define_id = save_define(&repo, "fork-join", &load_flow("04-fork-join"));
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let task_a = doing_named(&repo, iid, "taskA").unwrap();
    engine
        .execute_and_jump_to_first_sync(task_a.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();
    let apply = doing_named(&repo, iid, "apply").unwrap();
    assert!(
        reapply::reapply_of(&apply.variables).is_none(),
        "未配置直返的节点不得生成直返上下文"
    );

    engine
        .execute_task_sync(apply.task_id, "applicant", &submit(SUBMIT_REAPPLY))
        .unwrap();
    // 原 taskB 仍在办（另一分支），普通重提重新分叉再建 A/B ⇒ 3 条进行中
    assert_eq!(
        doing_all(&repo, iid).len(),
        3,
        "普通重提仍按流程重新审批（重新分叉）"
    );
    assert_eq!(rows(&repo, iid, "taskA"), 2);
    assert_eq!(rows(&repo, iid, "taskB"), 2);
}

#[test]
fn test_rts_forged_reserved_keys_ignored() {
    let (engine, repo) = rts_engine();
    let define_id = save_define(&repo, "fork-join", &load_flow("04-fork-join"));
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let task_a = doing_named(&repo, iid, "taskA").unwrap();
    engine
        .execute_and_jump_to_first_sync(task_a.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();
    let apply = doing_named(&repo, iid, "apply").unwrap();

    let mut forged = submit(SUBMIT_REAPPLY);
    forged.insert(
        "_reapplyContext".to_string(),
        jeeflow_core::json::parse_json(
            r#"{"mode":"RETURN_TO_SOURCE","sourceTaskId":999,"sourceNode":"taskX","branch":[]}"#,
        )
        .unwrap(),
    );
    forged.insert(
        "_branchCtx".to_string(),
        jeeflow_core::json::parse_json(r#"[{"join":"join1","round":0,"root":1}]"#).unwrap(),
    );
    engine
        .execute_task_sync(apply.task_id, "applicant", &forged)
        .unwrap();

    assert_eq!(doing_all(&repo, iid).len(), 3, "伪造控制键不得改变路由");
    for t in repo.find_history_tasks(iid).unwrap() {
        assert!(
            reapply::reapply_of(&t.variables).is_none(),
            "控制键不得写入业务任务的直返上下文: {}",
            t.task_name
        );
        for s in reapply::scopes_of(&t.variables) {
            assert_eq!(s.fork, "fork1", "分支归属只能是引擎按模型算出的完整档");
            assert_eq!(s.join, "join1");
        }
    }
}

#[test]
fn test_rts_invalid_reapply_mode_fails_at_parse() {
    let (engine, repo) = rts_engine();
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/flows-invalid/17-return-to-source-bad-config.json");
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {}", path.display(), e));
    let define_id = save_define(&repo, "return-to-source-bad-config", &content);
    let err = engine
        .start_sync(define_id, "applicant", &FlowData::new())
        .unwrap_err();
    assert!(
        err.message().contains("reapplyMode"),
        "非法 reapplyMode 必须在解析期报错：{}",
        err.message()
    );
}

#[test]
fn test_rts_branch_scopes_carry_round_and_root() {    // 契约 3：建单时算分支范围；分支根的 round=父任务 id、root=自身 id（落库后回填）
    let (engine, repo) = rts_engine();
    let define_id = save_define(
        &repo,
        "return-to-source-fork",
        &load_flow("15-return-to-source-fork"),
    );
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let task_a = doing_named(&repo, iid, "taskA").unwrap();
    let task_b = doing_named(&repo, iid, "taskB").unwrap();
    let scopes_a = reapply::scopes_of(&task_a.variables);
    let scopes_b = reapply::scopes_of(&task_b.variables);
    assert_eq!(scopes_a.len(), 1);
    assert_eq!(scopes_a[0].fork, "fork1");
    assert_eq!(scopes_a[0].join, "join1");
    assert_eq!(
        scopes_a[0].round,
        task_a.parent_task_id.unwrap(),
        "分支根 round＝父任务（申请）id"
    );
    assert_eq!(
        scopes_a[0].root, task_a.task_id,
        "分支根 root＝自身 id（落库后回填）"
    );
    assert_eq!(scopes_b[0].root, task_b.task_id);
    assert_eq!(scopes_b[0].round, scopes_a[0].round);

    // 来源节点属性解析：apply=Normal，taskA/taskB=ReturnToSource
    let model = ModelParser::parse(&define_content(&repo, define_id)).unwrap();
    assert_eq!(
        model.get_node("apply").unwrap().reapply_mode,
        ReapplyMode::Normal
    );
    assert_eq!(
        model.get_node("taskA").unwrap().reapply_mode,
        ReapplyMode::ReturnToSource
    );
}

/// 退回准入校验必须排在**任何状态变更之前**（java `validateReturnToSource`）：
/// 申请节点自己配了直返、会签节点配了直返，都只能报错，不能先完成任务。
fn inline_flow(apply_props: &str, task_props: &str) -> String {
    format!(
        r#"{{
            "name": "return-to-source-inline",
            "nodes": [
                {{"id": "start", "type": "snaker:start"}},
                {{"id": "apply", "type": "snaker:task", "properties": {}}},
                {{"id": "taskA", "type": "snaker:task", "properties": {}}},
                {{"id": "end", "type": "snaker:end"}}
            ],
            "edges": [
                {{"sourceNodeId": "start", "targetNodeId": "apply"}},
                {{"sourceNodeId": "apply", "targetNodeId": "taskA"}},
                {{"sourceNodeId": "taskA", "targetNodeId": "end"}}
            ]
        }}"#,
        apply_props, task_props
    )
}

#[test]
fn test_rts_source_as_first_task_rejected_before_mutation() {
    // 申请节点自己配 RETURN_TO_SOURCE ⇒ 退回操作必须被拦，且申请待办仍是 DOING
    let (engine, repo) = rts_engine();
    let define_id = save_define(
        &repo,
        "rts-inline-first",
        &inline_flow(
            r#"{"assignee": "applicant", "reapplyMode": "RETURN_TO_SOURCE"}"#,
            r#"{"assignee": "userA"}"#,
        ),
    );
    let inst = engine.start_sync(define_id, "applicant", &FlowData::new()).unwrap();
    let apply = doing_named(&repo, inst.instance_id, "apply").unwrap();
    let err = engine
        .execute_and_jump_to_first_sync(apply.task_id, "applicant", &submit(SUBMIT_RETURN))
        .unwrap_err();
    assert!(
        err.message().contains("不能配置重提直返"),
        "申请节点不能配直返：{}",
        err.message()
    );
    assert!(
        task_row(&repo, apply.task_id).is_doing(),
        "校验失败不得先完成任务"
    );
    assert_eq!(doing_all(&repo, inst.instance_id).len(), 1, "不得凭空建申请待办");
}

#[test]
fn test_rts_countersign_source_rejected_before_mutation() {    // 会签节点配 RETURN_TO_SOURCE ⇒ 不支持退回修改直返，任何一张会签任务都不得被完成
    let (engine, repo) = rts_engine();
    let define_id = save_define(
        &repo,
        "rts-inline-countersign",
        &inline_flow(
            r#"{"assignee": "applicant"}"#,
            r#"{"assignee": "userA,userB", "performType": 1, "reapplyMode": "RETURN_TO_SOURCE"}"#,
        ),
    );
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let countersign = doing_named(&repo, iid, "taskA").expect("会签首位成员待办");
    let err = engine
        .execute_and_jump_to_first_sync(countersign.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap_err();
    assert!(
        err.message().contains("不支持退回修改直返"),
        "会签节点不支持直返：{}",
        err.message()
    );
    assert!(
        task_row(&repo, countersign.task_id).is_doing(),
        "校验失败不得先完成会签任务"
    );
    assert!(doing_named(&repo, iid, "apply").is_none(), "不得建申请待办");
}

#[test]
fn test_rts_concurrent_reapply_only_once() {
    // 契约 8：同实例互斥 —— 同一申请任务并发重提至多一个成功，只产出一组新待办
    let (engine, repo) = rts_engine();
    let define_id = save_define(&repo, "return-to-source", &load_flow("14-return-to-source"));
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let task_a = doing_named(&repo, iid, "taskA").unwrap();
    engine
        .execute_and_jump_to_first_sync(task_a.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();
    let apply_id = doing_named(&repo, iid, "apply").unwrap().task_id;

    let engine = Arc::new(engine);
    let (ok, failed) = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..2 {
            let engine = Arc::clone(&engine);
            handles.push(scope.spawn(move || {
                engine
                    .execute_task_sync(apply_id, "applicant", &submit(SUBMIT_REAPPLY))
                    .is_ok()
            }));
        }
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .fold((0usize, 0usize), |(ok, bad), success| {
                if success { (ok + 1, bad) } else { (ok, bad + 1) }
            })
    });

    assert_eq!(ok, 1, "并发重提只允许一个成功");
    assert_eq!(failed, 1);
    assert_eq!(doing_all(&repo, iid).len(), 1, "只产生一组新待办");
    assert_eq!(
        repo.find_doing_tasks(iid, &["taskA".to_string()])
            .unwrap()
            .len(),
        1
    );
}

// ═══ 10.8 负向：带标记申请待办不得走其他办理/跳转入口（java `test16` 同款） ═══

#[test]
fn test_rts_marked_applicant_rejects_return_and_jumps() {
    let (engine, repo) = rts_engine();
    let define_id = save_define(&repo, "return-to-source", &load_flow("14-return-to-source"));
    let iid = start_flow(&engine, &repo, define_id, &FlowData::new());
    let task_a = doing_named(&repo, iid, "taskA").unwrap();
    engine
        .execute_and_jump_to_first_sync(task_a.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();
    let apply = doing_named(&repo, iid, "apply").unwrap();
    let doing_before = doing_all(&repo, iid).len();
    let hist_before = repo.find_history_tasks(iid).unwrap().len();

    fn assert_marked(r: JeeflowResult<Vec<ProcessTask>>) {
        let err = r.expect_err("带直返标记的申请待办不应被其他入口接受");
        assert!(
            err.message().contains("只接受重新提交"),
            "守卫文案须点名只接受重新提交：{}",
            err.message()
        );
    }

    assert_marked(engine.execute_and_jump_to_first_sync(
        apply.task_id,
        "applicant",
        &submit(SUBMIT_RETURN),
    ));
    assert_marked(engine.execute_and_jump_sync(
        apply.task_id,
        "applicant",
        &submit(SUBMIT_ROLLBACK),
        None,
    ));
    assert_marked(engine.execute_and_jump_sync(
        apply.task_id,
        "applicant",
        &submit(SUBMIT_JUMP),
        Some("taskB"),
    ));
    assert_marked(engine.execute_and_jump_to_end_sync(
        apply.task_id,
        "applicant",
        &submit(SUBMIT_REJECT),
    ));

    assert_eq!(
        doing_all(&repo, iid).len(),
        doing_before,
        "拒绝后不新增任务"
    );
    assert_eq!(
        repo.find_history_tasks(iid).unwrap().len(),
        hist_before,
        "拒绝后不新增历史行"
    );
    let still = doing_named(&repo, iid, "apply").expect("原申请待办仍在");
    assert_eq!(still.task_id, apply.task_id);
    assert_eq!(
        reapply::reapply_of(&task_row(&repo, apply.task_id).variables)
            .expect("原等待上下文仍在")
            .source_task_id,
        task_a.task_id
    );
    assert_eq!(rows(&repo, iid, "taskA"), 1, "来源任务保持完成态、未被重建");

    // 正常重提仍然可用
    engine
        .execute_task_sync(apply.task_id, "applicant", &submit(SUBMIT_REAPPLY))
        .unwrap();
    assert!(doing_named(&repo, iid, "taskA").is_some());
}
