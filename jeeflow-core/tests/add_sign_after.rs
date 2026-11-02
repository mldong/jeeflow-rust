//! 「后加签（可重复）」T0 验收——移植 jeeflow-java `test/AddSignAfterTest.java` 的 16 格语义。
//!
//! 契约正文：spec/04「后加签（可重复）」、spec/02 §4.3、spec/06 §2.7 ＋ 文末失败 msg 表。
//! 参考实现基准：`jeeflow-java/jeeflow-core/.../core/AddSignSupport.java`、
//! `handler/impl/AddSignHandler.java`、`domain/ProcessInstance.completeTask`。
//!
//! 夹具纪律（两条）：
//! - **正向链走共享夹具** `flows/19-add-sign-after.json`（java 编辑源的入库副本，见 `test16`）；
//! - **需要改节点 properties 的夹具走内联 JSON**（`chain`）——不往共享 `flows/` 里加文件，
//!   那份目录是七语言仓镜像＋漂移门禁的核对对象（本仓 `flowsdir` 会全量镜像，多一个文件
//!   就等于把十仓同步拖进这一轮）。非法配置样本同样只走内联，理由见
//!   `reapply_return_to_source.rs` 的 `tests/flows-invalid/` 那条注释。
//! - 用例各自用**独立 define 名段**（`addsign-NN`）＋ 独立 define，不共用夹具 id，
//!   避免"多例共用 define ＋ 全局清场"那种逐跑漂移的互踩形状。

use jeeflow_core::add_sign;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::ProcessDefine;
use jeeflow_core::parser::ModelParser;
use jeeflow_core::spi::ProcessRepository;
use jeeflow_core::{FlowData, JeeflowEngineImpl, JsonValue, ProcessTask, ServiceContext};
use std::sync::Arc;

const SUBMIT_APPLY: i64 = 0;
const SUBMIT_AGREE: i64 = 1;
const SUBMIT_REJECT: i64 = 2;
const SUBMIT_ROLLBACK: i64 = 3;
const SUBMIT_RETURN: i64 = 6;

const ADD_SIGN_ON: &str = r#""allowAddSign":true"#;

// ═══ 工装 ═══

fn as_engine() -> (JeeflowEngineImpl, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    (JeeflowEngineImpl::new(ctx), repo)
}

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

/// start → apply → review → final → end；参数是**审批节点**的 properties 片段
/// （逐字对齐 java `AddSignAfterTest.chain` 的夹具形状）。
fn chain(review_props: &str) -> String {
    let r = if review_props.is_empty() {
        String::new()
    } else {
        format!(",{}", review_props)
    };
    format!(
        r#"{{
            "name": "addsign-inline",
            "displayName": "后加签夹具",
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

/// 审批节点 properties 任写（首任务节点那一格的夹具要改 apply 节点，故另开一支）。
fn chain_with_apply(apply_props: &str, review_props: &str) -> String {
    chain(review_props).replace(
        r#"{"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"}"#,
        &format!(r#"{{"form":"apply-form","taskType":0,"performType":0,{}}}"#, apply_props),
    )
}

fn submit(type_: i64) -> FlowData {
    let mut args = FlowData::new();
    args.insert_i64("submitType", type_);
    args
}

/// 同意 ＋ 逗号串形态的加签人（spec 06 §2.7「两形同判据」的串档）。
fn agree(operators: &str) -> FlowData {
    let mut args = submit(SUBMIT_AGREE);
    args.insert_str(add_sign::ADD_SIGN_OPERATORS, operators);
    args
}

/// 同意 ＋ 数组形态的加签人（＝前端多选 select 的提交形状）。
fn agree_arr(operators: &[&str]) -> FlowData {
    let mut args = submit(SUBMIT_AGREE);
    args.insert(
        add_sign::ADD_SIGN_OPERATORS.to_string(),
        JsonValue::Array(operators.iter().map(|a| JsonValue::Str(a.to_string())).collect()),
    );
    args
}

fn doing_named(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> Option<ProcessTask> {
    repo.find_doing_tasks(iid, &[])
        .unwrap()
        .into_iter()
        .find(|t| t.task_name == name)
}

fn doing_all_named(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> Vec<ProcessTask> {
    repo.find_doing_tasks(iid, &[])
        .unwrap()
        .into_iter()
        .filter(|t| t.task_name == name)
        .collect()
}

fn doing_count(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> usize {
    doing_all_named(repo, iid, name).len()
}

fn rows(repo: &Arc<MemoryRepository>, iid: i64, name: &str) -> usize {
    repo.find_history_tasks(iid)
        .unwrap()
        .iter()
        .filter(|t| t.task_name == name)
        .count()
}

fn task_row(repo: &Arc<MemoryRepository>, tid: i64) -> ProcessTask {
    repo.find_task_by_id(tid).unwrap().unwrap()
}

fn instance_state(repo: &Arc<MemoryRepository>, iid: i64) -> i32 {
    repo.find_instance_by_id(iid).unwrap().unwrap().state
}

fn instance_vars(repo: &Arc<MemoryRepository>, iid: i64) -> Vec<String> {
    repo.find_instance_by_id(iid)
        .unwrap()
        .unwrap()
        .variables
        .inner()
        .keys()
        .cloned()
        .collect()
}

/// 进行中任务的 `actor_id` 列恒空是家族不变量（办结才写），取办理人要用参与者列表。
fn first_actor(t: &ProcessTask) -> String {
    t.actor_ids.first().cloned().unwrap_or_default()
}

/// 发起并办结申请节点，返回停在 review 的实例 id。
fn start_to_review(
    engine: &JeeflowEngineImpl,
    repo: &Arc<MemoryRepository>,
    define_id: i64,
) -> i64 {
    let inst = engine.start_sync(define_id, "applicant", &FlowData::new()).unwrap();
    for t in doing_all_named(repo, inst.instance_id, "apply") {
        engine
            .execute_task_sync(t.task_id, "applicant", &submit(SUBMIT_APPLY))
            .unwrap();
    }
    inst.instance_id
}

fn hist_entry(hist: &JsonValue, key: &str) -> JsonValue {
    hist.as_object()
        .and_then(|entries| entries.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()))
        .unwrap_or_else(|| panic!("账本条目缺键 {}: {:?}", key, hist))
}

// ═══ 正向 ═══

/// test01 链式续办：A→B→C→不点人即流出；血缘串成链、轮次递增、账本落在已完成行。
#[test]
fn test01_chain_then_stop() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-01", &chain(ADD_SIGN_ON));
    let iid = start_to_review(&engine, &repo, define_id);

    let a = doing_named(&repo, iid, "review").expect("申请办结后应在审批节点有待办");
    engine.execute_task_sync(a.task_id, "userA", &agree("userB")).unwrap();

    let b = doing_named(&repo, iid, "review").expect("被加签人应在同一节点收到新待办");
    assert_eq!(Some(a.task_id), b.parent_task_id, "血缘 parent＝刚办结那条");
    assert_eq!(
        b.variables.get_i64(&add_sign::round_key("review")),
        Some(1),
        "轮次 1"
    );
    assert!(doing_named(&repo, iid, "final").is_none(), "令牌不得流出本节点");
    assert_eq!(1, doing_count(&repo, iid, "review"), "本节点只应有 1 条待办");
    assert_eq!(0, b.perform_type, "加签任务是普通任务，不是会签（wf_process_task_perform_type 0）");
    assert_eq!(Some("form-r"), b.form_key.as_deref(), "form 继承节点配置");

    let hist_a = add_sign::history_of(&task_row(&repo, a.task_id));
    assert_eq!(1, hist_a.len());
    let e0 = &hist_a[0];
    assert_eq!(JsonValue::Number(1.0), hist_entry(e0, "round"));
    assert_eq!("userA", hist_entry(e0, "fromActor").as_str().unwrap());
    assert_eq!(
        vec![JsonValue::Str("userB".into())],
        hist_entry(e0, "toActors").as_array().unwrap().clone()
    );
    assert_eq!("userA", hist_entry(e0, "operator").as_str().unwrap());
    let time = hist_entry(e0, "time").as_str().unwrap().to_string();
    assert!(
        time.len() == 19
            && time.as_bytes()[4] == b'-'
            && time.as_bytes()[10] == b' '
            && time.as_bytes()[13] == b':',
        "时间须是全生态格式串 yyyy-MM-dd HH:mm:ss，实得 {}",
        time
    );

    // 第二圈：B 再点 C（可重复）
    engine.execute_task_sync(b.task_id, "userB", &agree("userC")).unwrap();
    let c = doing_named(&repo, iid, "review").expect("加签链可重复");
    assert_eq!(
        c.variables.get_i64(&add_sign::round_key("review")),
        Some(2),
        "轮次递增"
    );
    assert_eq!(Some(b.task_id), c.parent_task_id, "血缘串成链");
    assert_eq!(1, add_sign::history_of(&task_row(&repo, b.task_id)).len(), "账本落在各自办结的那一行");
    assert!(add_sign::history_of(&task_row(&repo, a.task_id)).len() == 1, "上一圈的账本不被覆盖");

    // 第三圈：C 不点人 ⇒ 流出本节点
    engine
        .execute_task_sync(c.task_id, "userC", &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(doing_named(&repo, iid, "review").is_none(), "不点人后不得再有本节点待办");
    let fin = doing_named(&repo, iid, "final").expect("应流转到下一审批节点");
    assert_eq!(3, rows(&repo, iid, "review"), "review 共三行");

    engine
        .execute_task_sync(fin.task_id, "userF", &submit(SUBMIT_AGREE))
        .unwrap();
    assert_eq!(20, instance_state(&repo, iid), "实例办结");
}

/// test02 一轮多选：同圈两行兄弟都要办完，且只有最后一个办结且没点人才放行。
#[test]
fn test02_multi_select_waits_siblings() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-02", &chain(ADD_SIGN_ON));
    let iid = start_to_review(&engine, &repo, define_id);

    let a = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(a.task_id, "userA", &agree_arr(&["userB", "userC"]))
        .unwrap();

    let siblings = doing_all_named(&repo, iid, "review");
    assert_eq!(2, siblings.len(), "同圈应建两行待办");
    assert!(doing_named(&repo, iid, "final").is_none(), "兄弟待办未完成时不得放行");
    assert_eq!(
        Some(a.task_id),
        siblings[0].parent_task_id,
        "同圈两行的血缘都指向刚办结那条"
    );
    assert_eq!(
        siblings[0].variables.get_i64(&add_sign::round_key("review")),
        siblings[1].variables.get_i64(&add_sign::round_key("review")),
        "同圈＝同一个轮次号"
    );

    engine
        .execute_task_sync(siblings[0].task_id, &first_actor(&siblings[0]), &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(
        doing_named(&repo, iid, "final").is_none(),
        "兄弟待办未完成时不得放行"
    );
    assert_eq!(1, doing_count(&repo, iid, "review"));

    engine
        .execute_task_sync(siblings[1].task_id, &first_actor(&siblings[1]), &submit(SUBMIT_AGREE))
        .unwrap();
    assert_eq!(
        1,
        doing_count(&repo, iid, "final"),
        "最后一位办结且没点人才放行，且放行只发生一次"
    );
    assert_eq!(0, doing_count(&repo, iid, "review"));
}

/// test03 加给已办过的人＝合法（A→B→A 乒乓），计入轮次。
#[test]
fn test03_re_add_previously_handled_is_legal() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-03", &chain(ADD_SIGN_ON));
    let iid = start_to_review(&engine, &repo, define_id);

    let a = doing_named(&repo, iid, "review").unwrap();
    engine.execute_task_sync(a.task_id, "userA", &agree("userB")).unwrap();
    let b = doing_named(&repo, iid, "review").unwrap();
    engine.execute_task_sync(b.task_id, "userB", &agree("userA")).unwrap();

    let back = doing_named(&repo, iid, "review").expect("加给已办过的人应合法");
    assert!(back.actor_ids.contains(&"userA".to_string()));
    assert_eq!(
        back.variables.get_i64(&add_sign::round_key("review")),
        Some(2),
        "乒乓那一次记轮次 2"
    );
}

// ═══ 负向：四道写入口判据（报错且零写入）═══

/// test04 与 `tf_nextNodeOperator` 同传 ⇒ 报错，且任务行一格都没动。
#[test]
fn test04_mutex_with_next_node_operator_rejected() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-04", &chain(ADD_SIGN_ON));
    let iid = start_to_review(&engine, &repo, define_id);
    let a = doing_named(&repo, iid, "review").unwrap();
    let before = rows(&repo, iid, "review");

    let mut args = agree("userB");
    args.insert_str(add_sign::NEXT_NODE_OPERATOR, "userF");
    let err = engine
        .execute_task_sync(a.task_id, "userA", &args)
        .expect_err("互斥必须报错");
    assert_eq!(add_sign::MSG_MUTEX, err.message());

    assert!(task_row(&repo, a.task_id).is_doing(), "报错时任务行不得被办结");
    assert_eq!(before, rows(&repo, iid, "review"), "不得新增行");
    assert_eq!(10, instance_state(&repo, iid), "实例状态不得被动");
    assert!(doing_named(&repo, iid, "final").is_none(), "不得流出本节点");
}

/// test05 节点未开 `allowAddSign` 却传键 ⇒ 报错（不静默忽略），零写入。
#[test]
fn test05_switch_off_rejected() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-05", &chain(""));
    let iid = start_to_review(&engine, &repo, define_id);
    let a = doing_named(&repo, iid, "review").unwrap();

    let err = engine
        .execute_task_sync(a.task_id, "userA", &agree("userB"))
        .expect_err("开关没开却传键必须报错");
    assert!(
        err.message().contains("未开启 allowAddSign"),
        "文案须点名未开启开关：{}",
        err.message()
    );
    assert_eq!(
        "节点[review]未开启 allowAddSign，不接受 tf_addSignOperators",
        err.message()
    );
    assert!(task_row(&repo, a.task_id).is_doing(), "报错时任务行不得被办结");
    assert!(doing_named(&repo, iid, "final").is_none(), "报错时不得流出本节点");
}

/// test06 加给自己 ⇒ 报错（逐字文案），零写入。
#[test]
fn test06_self_rejected() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-06", &chain(ADD_SIGN_ON));
    let iid = start_to_review(&engine, &repo, define_id);
    let a = doing_named(&repo, iid, "review").unwrap();

    let err = engine
        .execute_task_sync(a.task_id, "userA", &agree("userA"))
        .expect_err("加给自己必须报错");
    assert_eq!(add_sign::MSG_SELF, err.message());
    assert!(task_row(&repo, a.task_id).is_doing(), "报错时任务行不得被办结");
    assert_eq!(1, doing_count(&repo, iid, "review"), "不得建出新待办");
    assert!(doing_named(&repo, iid, "final").is_none());
}

/// test07 超 `addSignMaxRounds` ⇒ 报错且**不强制放行**；上限内照常。
#[test]
fn test07_max_rounds_rejected() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-07", &chain(&format!(r#"{},"addSignMaxRounds":1"#, ADD_SIGN_ON)));
    let iid = start_to_review(&engine, &repo, define_id);

    // 第 1 圈在上限内 ⇒ 照常
    let a = doing_named(&repo, iid, "review").unwrap();
    engine.execute_task_sync(a.task_id, "userA", &agree("userB")).unwrap();
    let b = doing_named(&repo, iid, "review").unwrap();

    // 第 2 圈越界 ⇒ 报错，不办结、不放行
    let err = engine
        .execute_task_sync(b.task_id, "userB", &agree("userC"))
        .expect_err("超上限必须报错");
    assert!(
        err.message().contains("已达上限 1"),
        "文案须点名上限：{}",
        err.message()
    );
    assert_eq!("节点[review]的加签轮次已达上限 1，本次提交被拒绝", err.message());
    assert!(task_row(&repo, b.task_id).is_doing(), "越界那次不得办结任务行");
    assert!(doing_named(&repo, iid, "final").is_none(), "越界那次不得强制放行");

    // 不点人仍能正常收尾（上限拦的是加签，不是办结）
    engine
        .execute_task_sync(b.task_id, "userB", &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(doing_named(&repo, iid, "final").is_some());
}

// ═══ 三条解析期校验 ═══

fn assert_parse_fails(flow_json: &str, expected_fragment: &str) {
    match ModelParser::parse(flow_json) {
        Ok(_) => panic!("应报解析期配置错误：{}", expected_fragment),
        Err(e) => assert!(
            e.message().contains(expected_fragment),
            "期望含「{}」，实得「{}」",
            expected_fragment,
            e.message()
        ),
    }
}

/// test08 会签节点配 `allowAddSign` ⇒ 解析期报错（会签池内加人与会签之后加人是两种语义）。
#[test]
fn test08_countersign_node_rejected_at_parse() {
    assert_parse_fails(
        &chain(&format!(r#"{},"performType":1,"countersignType":"PARALLEL""#, ADD_SIGN_ON)),
        "会签节点",
    );
}

/// test09 首任务节点配 `allowAddSign` ⇒ 解析期报错（申请节点的办理是发起/重提语义）。
#[test]
fn test09_first_task_node_rejected_at_parse() {
    assert_parse_fails(
        &chain_with_apply(r#""assignee":"applicant","allowAddSign":true"#, ADD_SIGN_ON),
        "首任务节点",
    );
}

/// test10 非法配置值 ⇒ 解析期报错，不得静默落回缺省。
#[test]
fn test10_invalid_config_values_rejected_at_parse() {
    assert_parse_fails(&chain(r#""allowAddSign":"yes""#), "allowAddSign 配置非法");
    assert_parse_fails(
        &chain(&format!(r#"{},"addSignMaxRounds":-1"#, ADD_SIGN_ON)),
        "不得为负",
    );
    assert_parse_fails(
        &chain(&format!(r#"{},"addSignMaxRounds":"1.5""#, ADD_SIGN_ON)),
        "必须是整数",
    );
    // 正向对照：合法值照常解析成字段（证明上一组不是"任何值都报错"）
    let model = ModelParser::parse(&chain(&format!(r#"{},"addSignMaxRounds":2"#, ADD_SIGN_ON))).unwrap();
    let review = model.get_node("review").unwrap();
    assert!(review.allow_add_sign);
    assert_eq!(2, review.add_sign_max_rounds);
    assert!(!model.get_node("final").unwrap().allow_add_sign, "缺省 false");
    assert_eq!(0, model.get_node("final").unwrap().add_sign_max_rounds, "缺省 0＝不限");
}

// ═══ 废弃未完成加签任务 ═══

/// test11 不同意(2)：实例结束，本节点未完成的兄弟加签任务全部废弃，不留孤儿待办。
#[test]
fn test11_reject_abandons_siblings() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-11", &chain(ADD_SIGN_ON));
    let iid = start_to_review(&engine, &repo, define_id);

    let a = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(a.task_id, "userA", &agree_arr(&["userB", "userC"]))
        .unwrap();
    assert_eq!(2, doing_count(&repo, iid, "review"));

    let b = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_and_jump_to_end_sync(b.task_id, &first_actor(&b), &submit(SUBMIT_REJECT))
        .unwrap();

    assert_eq!(45, instance_state(&repo, iid), "实例应已结束（已拒绝）");
    assert_eq!(0, doing_count(&repo, iid, "review"), "未完成的加签兄弟不得留待办");
    let abandoned = repo
        .find_history_tasks(iid)
        .unwrap()
        .into_iter()
        .filter(|t| t.task_name == "review" && t.task_state == 99)
        .count();
    assert_eq!(1, abandoned, "被废弃的那条应留废弃态(99)");
}

/// test12 退回发起人(6)：申请待办生成，本节点未完成加签任务废弃。
#[test]
fn test12_return_to_applicant_abandons_siblings() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-12", &chain(ADD_SIGN_ON));
    let iid = start_to_review(&engine, &repo, define_id);

    let a = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(a.task_id, "userA", &agree_arr(&["userB", "userC"]))
        .unwrap();
    let b = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_and_jump_to_first_sync(b.task_id, &first_actor(&b), &submit(SUBMIT_RETURN))
        .unwrap();

    assert!(doing_named(&repo, iid, "apply").is_some(), "应生成申请待办");
    assert_eq!(0, doing_count(&repo, iid, "review"), "本节点未完成加签任务应被废弃");
    assert!(doing_named(&repo, iid, "final").is_none());
}

// ═══ 回归 ═══

/// test13 不开开关的节点行为逐字不变：同意即流出，且不因新判据受影响。
#[test]
fn test13_plain_node_unchanged() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-13", &chain(""));
    let iid = start_to_review(&engine, &repo, define_id);
    let a = doing_named(&repo, iid, "review").unwrap();

    engine
        .execute_task_sync(a.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(doing_named(&repo, iid, "final").is_some(), "普通档仍是同意即流出");
    assert_eq!(1, rows(&repo, iid, "review"));
    assert!(add_sign::history_of(&task_row(&repo, a.task_id)).is_empty(), "不开开关不留账本");
    assert!(task_row(&repo, a.task_id)
        .variables
        .get(&add_sign::round_key("review"))
        .is_none());
}

/// test14 两个同圈加签任务并发办结：**只放行一次**（后续节点恰好一条待办，不重复创建）。
#[test]
fn test14_concurrent_finish_releases_once() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-14", &chain(ADD_SIGN_ON));
    let iid = start_to_review(&engine, &repo, define_id);

    let a = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(a.task_id, "userA", &agree_arr(&["userB", "userC"]))
        .unwrap();
    let siblings = doing_all_named(&repo, iid, "review");
    assert_eq!(2, siblings.len());
    let jobs: Vec<(i64, String)> = siblings
        .iter()
        .map(|t| (t.task_id, first_actor(t)))
        .collect();

    let engine = Arc::new(engine);
    let errors = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for (tid, actor) in jobs {
            let engine = Arc::clone(&engine);
            handles.push(scope.spawn(move || {
                engine
                    .execute_task_sync(tid, &actor, &submit(SUBMIT_AGREE))
                    .is_err()
            }));
        }
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|e| *e)
            .count()
    });
    assert_eq!(0, errors, "并发办结不应报错");
    assert_eq!(1, doing_count(&repo, iid, "final"), "后续节点只应有一条待办");
    assert_eq!(0, doing_count(&repo, iid, "review"));
}

/// test15 根因守卫：控制类 `tf_*` 参数不得进实例变量（跨请求复活的病根），
/// 但任务行留痕必须完整——两者是同一枚判据的两面。
#[test]
fn test15_control_keys_never_enter_instance_variables() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-15", &chain(ADD_SIGN_ON));
    let iid = start_to_review(&engine, &repo, define_id);
    let a = doing_named(&repo, iid, "review").unwrap();

    let mut args = agree("userB");
    args.insert_str("tf_approvalComment", "请 B 复核");
    engine.execute_task_sync(a.task_id, "userA", &args).unwrap();

    // ① 实例变量里不得有任何 tf_ 键
    for k in instance_vars(&repo, iid) {
        assert!(!k.starts_with("tf_"), "实例变量里不应有任何 tf_ 键: {}", k);
    }
    let ivars = repo.find_instance_by_id(iid).unwrap().unwrap().variables;
    assert!(ivars.get(add_sign::ADD_SIGN_OPERATORS).is_none(), "tf_addSignOperators 不得进实例变量");
    assert!(ivars.get("tf_approvalComment").is_none(), "tf_approvalComment 不得进实例变量");

    // ② 留痕仍在任务行上（修根因不得把审计信息一起改掉）
    let row_vars = &task_row(&repo, a.task_id).variables;
    assert_eq!(Some("请 B 复核"), row_vars.get_str("tf_approvalComment"));
    assert_eq!(Some("userB"), row_vars.get_str(add_sign::ADD_SIGN_OPERATORS), "任务行留痕收整包参数");
    assert_eq!(1, add_sign::history_of(&task_row(&repo, a.task_id)).len());

    // ③ 复活探针：B 这次**没点人**，上一轮的 tf_addSignOperators 若跨请求复活就会凭空
    //    多建一条本节点待办（本栈的根因守卫正格——令牌必须直接流出本节点）
    let b = doing_named(&repo, iid, "review").unwrap();
    engine
        .execute_task_sync(b.task_id, "userB", &submit(SUBMIT_AGREE))
        .unwrap();
    assert_eq!(0, doing_count(&repo, iid, "review"), "上一轮的控制键不得复活成新的本节点待办");
    assert_eq!(1, doing_count(&repo, iid, "final"), "没点人即流出本节点");
}

/// test16 加签链上「退回上一步」(3) **禁**：当前行与血缘 parent 同节点，
/// 既有血缘守卫不过 ⇒ 走本栈既有的等价错误出口（内部码 20010008 档，对外逐字文案不带码）。
///
/// **报错那一次零写入**（issues/243 甲案，2026-11-03 落地）：血缘守卫由引擎排在
/// `complete_task`（办结当前行并落库）**之前**（java `checkRollbackPossible` 提到
/// `prepareExecution` 之前的同位），内存仓储无回滚也照得见——报错后当前行仍 DOING、
/// 待办还在办理人手里。修前的形状是"点了退回、报错了、待办也没了"（下面四格当时全红）。
#[test]
fn test16_rollback_on_add_sign_chain_rejected() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-16", &chain(ADD_SIGN_ON));
    let iid = start_to_review(&engine, &repo, define_id);
    let a = doing_named(&repo, iid, "review").unwrap();
    engine.execute_task_sync(a.task_id, "userA", &agree("userB")).unwrap();
    let b = doing_named(&repo, iid, "review").unwrap();
    let total_rows = repo.find_history_tasks(iid).unwrap().len();

    let err = engine
        .execute_and_jump_sync(b.task_id, "userB", &submit(SUBMIT_ROLLBACK), None)
        .expect_err("加签链上不得退回上一步");
    assert!(
        err.message().contains("无法驳回至上一步处理，请确认上一步骤并非fork、join、suprocess以及会签任务"),
        "须走既有 canRejected 守卫的逐字文案：{}",
        err.message()
    );
    assert!(doing_named(&repo, iid, "final").is_none(), "不得流出本节点");
    // 守卫不过＝不建复活行；守卫前置＝当前行也不许被办结（报错即零写入）。
    assert_eq!(2, rows(&repo, iid, "review"), "不得复活出第三条同节点行");
    assert_eq!(1, doing_count(&repo, iid, "review"),
        "报错零写入：当前行必须仍是待办（守卫排在办结之前）");
    let still = doing_named(&repo, iid, "review").expect("待办还在办理人手里");
    assert_eq!(b.task_id, still.task_id, "还是报错那一条，不是新建的行");
    assert_eq!(total_rows, repo.find_history_tasks(iid).unwrap().len(), "报错不建单：行总数不变");
    assert_eq!(10, instance_state(&repo, iid), "实例状态不受这次失败影响");
}

/// test17 共享夹具 `flows/19-add-sign-after.json` 必须真被本栈消费
/// （七语言仓 `flows/` 是 java 编辑源的入库副本，各语言移植后加签读的就是这一份）。
#[test]
fn test17_shared_fixture_chain_runs() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "add-sign-after", &load_flow("19-add-sign-after"));
    let iid = start_to_review(&engine, &repo, define_id);

    let a = doing_named(&repo, iid, "review").expect("共享夹具的申请节点办结后应到审批节点");
    assert!(a.actor_ids.contains(&"userA".to_string()), "夹具的审批节点办理人＝userA");
    engine.execute_task_sync(a.task_id, "userA", &agree("userB,userC")).unwrap();
    assert_eq!(2, doing_count(&repo, iid, "review"), "逗号串两形应建两行");
    assert!(doing_named(&repo, iid, "final").is_none());

    // 先取快照再逐条办结：办结会改写仓储里的进行中集合，边遍历边办是并发修改
    let siblings = doing_all_named(&repo, iid, "review");
    for s in &siblings {
        engine
            .execute_task_sync(s.task_id, &first_actor(s), &submit(SUBMIT_AGREE))
            .unwrap();
    }
    let fin = doing_named(&repo, iid, "final").expect("同圈兄弟全部办结后应放行");
    assert_eq!(1, rows(&repo, iid, "final"));
    engine
        .execute_task_sync(fin.task_id, "userF", &submit(SUBMIT_AGREE))
        .unwrap();
    assert_eq!(20, instance_state(&repo, iid), "实例办结");
}

/// 加签那一轮的到期时间与其余建单同一把尺子（spec/04「任务行 `expire_time` 由谁写」）。
/// 摘掉 `add_sign_leg` 里那句 `stamp_expire`  ⇒ 本格报红（坏样本守卫）。
#[test]
fn test18_expire_time_uses_the_same_ruler() {
    let (engine, repo) = as_engine();
    let flow = chain(&format!(r#"{},"expireTime":"2h""#, ADD_SIGN_ON));
    let define_id = save_define(&repo, "addsign-18", &flow);
    let iid = start_to_review(&engine, &repo, define_id);

    let a = doing_named(&repo, iid, "review").unwrap();
    engine.execute_task_sync(a.task_id, "userA", &agree("userB")).unwrap();
    let b = doing_named(&repo, iid, "review").unwrap();
    assert!(
        b.expire_time.is_some(),
        "加签续建必须按节点表达式写到期时间（未配才落 NULL）"
    );
    assert_eq!(b.expire_time, task_row(&repo, b.task_id).expire_time, "落库那份也要带上");
}

/// 加签链上"没有可用参与者"不建空任务：空串/纯空白＝没传，走正常流出档（spec 06 §2.11）。
#[test]
fn test19_blank_only_operators_is_not_a_submission() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "addsign-19", &chain(ADD_SIGN_ON));
    let iid = start_to_review(&engine, &repo, define_id);
    let a = doing_named(&repo, iid, "review").unwrap();

    engine.execute_task_sync(a.task_id, "userA", &agree("  , ")).unwrap();
    assert_eq!(0, doing_count(&repo, iid, "review"), "丢完为空＝没传，不得建无人可办的空任务");
    assert!(doing_named(&repo, iid, "final").is_some(), "空档即正常放行出边");
    assert!(add_sign::history_of(&task_row(&repo, a.task_id)).is_empty(), "没加签就不留账本");
}

/// test20 第五条口子：**重提直返那一拍（submitType=5）也不得绕过四道写入口判据**。
///
/// java 那边重新提交与其余入口共用 `prepareExecution`（`JeeflowEngineImpl.java:391` → `:449` 的
/// `AddSignSupport.validateSubmission`），本栈 `execute_reapply` 是独立一条编排，漏了就是
/// "带 `tf_addSignOperators` 的重新提交被静默丢掉点人"——开关没开的节点必须逐字报错。
/// 申请节点结构性开不了 `allowAddSign`（spec/02 §4.3 首任务节点禁令），所以这一发可达的正是
/// "未开启开关"那一档。
#[test]
fn test20_reapply_entry_cannot_bypass_validation() {
    let (engine, repo) = as_engine();
    let define_id = save_define(
        &repo,
        "addsign-20",
        &chain(&format!(r#"{},"reapplyMode":"RETURN_TO_SOURCE""#, ADD_SIGN_ON)),
    );
    let iid = start_to_review(&engine, &repo, define_id);
    let a = doing_named(&repo, iid, "review").unwrap();
    // 退回发起人 ⇒ 申请待办带重提直返上下文（下一拍只接受 submitType=5）
    engine
        .execute_and_jump_to_first_sync(a.task_id, "userA", &submit(SUBMIT_RETURN))
        .unwrap();
    let apply = doing_named(&repo, iid, "apply").expect("退回发起人应生成申请待办");

    let mut args = submit(5);
    args.insert_str(add_sign::ADD_SIGN_OPERATORS, "userB");
    let err = engine
        .execute_task_sync(apply.task_id, "applicant", &args)
        .expect_err("重提直返那一拍不得绕过加签写入口判据");
    assert_eq!("节点[apply]未开启 allowAddSign，不接受 tf_addSignOperators", err.message());
    // 报错且零写入（排在 complete_task 之前）
    assert!(task_row(&repo, apply.task_id).is_doing(), "报错时申请待办不得被办结");
    assert_eq!(1, doing_count(&repo, iid, "apply"), "报错时不得建出新的申请待办");
    assert_eq!(0, doing_count(&repo, iid, "review"), "报错时不得复活审批待办");
}
