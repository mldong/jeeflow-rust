//! 「动态会签（征询模式 `dynamicCountersign`）」**引擎侧** T0 验收。
//!
//! 分工：发起分流／闸门／撤回接口／detail 投影落在**门面**（逐字照 java
//! `JeeflowFacade.tryDynamicCountersign / gateDynamicCountersign / taskRevokeCountersign`），
//! 那八格的端到端语义在 `jeeflow-facade/tests/dynamic_countersign.rs`；
//! **本文件钉的是引擎侧的那几根承重柱**——成员行身份判据、收齐计数与投影的 99 跳过、
//! 征询档推进闸、三条解析期禁配、逐字文案常量。
//!
//! 契约正文：spec/02 §4.4、spec/04「动态会签（征询模式）」、spec/06 §2.7＋
//! §processTask/revokeDynamicCountersign。参考实现基准（逐字权威）：
//! `jeeflow-java@09c2afe` 的 `core/CollectSupport.java`、`model/TaskModel.exec` 的
//! `else if (dynamicCountersign)` 那一支、`parser/ModelParser.java` 收尾三段。
//!
//! 夹具纪律：全部内联 JSON，不碰共享 `flows/`（见 `add_sign_after.rs` 顶部那条说明）。

use jeeflow_core::add_sign;
use jeeflow_core::collect;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::{ProcessDefine, ProcessInstance, ProcessTask, TaskState};
use jeeflow_core::parser::{ModelParser, NodeType};
use jeeflow_core::spi::ProcessRepository;
use jeeflow_core::{FlowData, JeeflowEngineImpl, JsonValue, ServiceContext};
use std::sync::Arc;

const SUBMIT_APPLY: i64 = 0;
const SUBMIT_AGREE: i64 = 1;

const COLLECT_ON: &str = r#""dynamicCountersign":true"#;

// ═══ 工装 ═══

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

/// start→apply→review→final→end（与 java `DynamicCountersignTest.chain` 同形）。
fn chain(review_props: &str) -> String {
    let r = if review_props.is_empty() {
        String::new()
    } else {
        format!(",{}", review_props)
    };
    format!(
        r#"{{
            "name": "collect-inline",
            "displayName": "动态会签夹具",
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

fn task_row(repo: &Arc<MemoryRepository>, tid: i64) -> ProcessTask {
    repo.find_task_by_id(tid).unwrap().unwrap()
}

fn rows(repo: &Arc<MemoryRepository>, iid: i64) -> Vec<ProcessTask> {
    repo.find_history_tasks(iid).unwrap()
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

/// 纯函数格的行工厂：走聚合根那颗建单工厂（不手搓字段表），再按需盖状态。
fn row(id: i64, name: &str, parent: i64, state: i32, actor: &str) -> ProcessTask {
    let define = ProcessDefine {
        id: 1,
        name: "pure".into(),
        display_name: "pure".into(),
        define_type: "approval".into(),
        state: 1,
        content: b"{}".to_vec(),
        version: 1,
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    let mut inst = ProcessInstance::create(&define, "userA", &FlowData::new());
    inst.instance_id = 1;
    let mut t = inst.create_task(
        name,
        name,
        &[actor.to_string()],
        "userA",
        jeeflow_core::model::TaskType::Major,
        jeeflow_core::model::PerformType::Normal,
        Some("form".into()),
        Some(parent),
    );
    t.task_id = id;
    t.task_state = state;
    t
}

fn hist_entry(hist: &JsonValue, key: &str) -> JsonValue {
    hist.as_object()
        .and_then(|entries| entries.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()))
        .unwrap_or_else(|| panic!("账本条目缺键 {}: {:?}", key, hist))
}

fn obj_entry(o: &JsonValue, key: &str) -> JsonValue {
    o.as_object()
        .and_then(|entries| entries.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone()))
        .unwrap_or_else(|| panic!("对象缺键 {}: {:?}", key, o))
}

// ═══ 承重判据 1：成员行身份＝parent 且同节点同名 ═══

#[test]
fn dc01_member_row_identity_requires_same_node_name() {
    let host = row(10, "review", 5, TaskState::Doing.code(), "userA");
    let member = row(11, "review", 10, TaskState::Doing.code(), "userX");
    // 主办办结推进后产出的下游行：parent 同样是主办行 id（建单不变量），但节点名不同
    let downstream = row(12, "final", 10, TaskState::Doing.code(), "userF");
    let tasks = vec![host.clone(), member.clone(), downstream.clone()];

    let members = collect::member_rows(&tasks, &host);
    assert_eq!(
        vec![11],
        members.iter().map(|t| t.task_id).collect::<Vec<_>>(),
        "只按 parent 会把主办推进后的下游行误当成员——撤回腿会误废弃它们"
    );
    assert!(collect::is_member_row(&tasks, &member));
    assert!(
        !collect::is_member_row(&tasks, &downstream),
        "同名过滤不可省：同 parent 不同节点 ⇒ 不是成员行"
    );
    assert!(
        !collect::is_member_row(&tasks, &host),
        "parent 指向别的节点（发起行）⇒ 不是成员行"
    );
    // 无父行（parent=0）不得被读成成员
    let root = row(13, "review", 0, TaskState::Doing.code(), "userR");
    assert!(!collect::is_member_row(&tasks, &root));
    let orphans = vec![row(0, "review", 0, TaskState::Doing.code(), "z"), root.clone()];
    assert_eq!(
        0,
        collect::member_rows(&orphans, &root).len(),
        "host.task_id=0（未落库草稿行）不得把一堆无父行收进成员集合"
    );
}

// ═══ 承重判据 2：闸门收齐＝无 DOING 成员行；计数与投影跳过 99 ═══

#[test]
fn dc02_counts_and_pending_skip_abandoned_rows() {
    let host = row(10, "review", 5, TaskState::Doing.code(), "userA");
    let submitted = row(11, "review", 10, TaskState::Finished.code(), "userX");
    let pending = row(12, "review", 10, TaskState::Doing.code(), "userY");
    let abandoned = row(13, "review", 10, TaskState::Abandon.code(), "userZ");
    let tasks = vec![host.clone(), submitted, pending.clone(), abandoned];

    // 在册＝非 99 的成员行；已交＝其中 FINISHED 的那几枚
    assert_eq!((2, 1), collect::count_members(&tasks, &host));
    assert_eq!(
        vec![12],
        collect::pending_members(&tasks, &host)
            .iter()
            .map(|t| t.task_id)
            .collect::<Vec<_>>(),
        "闸门判据只看 DOING 成员行；撤回清场的 99 行不再算在册"
    );
    // 收齐（无 DOING 成员行）时 pending 为空 ⇒ 闸门自然恢复，不需要额外界复位动作
    let cleared = vec![
        host.clone(),
        row(11, "review", 10, TaskState::Finished.code(), "userX"),
        row(13, "review", 10, TaskState::Abandon.code(), "userZ"),
    ];
    assert!(collect::pending_members(&cleared, &host).is_empty());
    assert_eq!((1, 1), collect::count_members(&cleared, &host));
}

/// 投影（spec 06 §2.7）：形状五键齐、99 行不进分母也不进 actors、无成员行 ⇒ None（不写键）。
#[test]
fn dc03_projection_shape_and_abandon_skip() {
    let host = row(10, "review", 5, TaskState::Doing.code(), "userA");
    assert!(
        collect::build_projection(&[host.clone()], &host).is_none(),
        "无成员行 ⇒ None（门面据此不写 ext.dynamicCountersign 这一枚键）"
    );

    let submitted = row(11, "review", 10, TaskState::Finished.code(), "userX");
    let pending = row(12, "review", 10, TaskState::Doing.code(), "userY");
    let abandoned = row(13, "review", 10, TaskState::Abandon.code(), "userZ");
    let tasks = vec![host.clone(), submitted, pending, abandoned];
    let dc = collect::build_projection(&tasks, &host).expect("有成员行 ⇒ 出投影");

    assert_eq!(JsonValue::Bool(true), obj_entry(&dc, "active"), "还有未答 ⇒ active=true");
    assert_eq!(2.0, obj_entry(&dc, "total").as_f64().unwrap(), "99 行不进分母");
    assert_eq!(1.0, obj_entry(&dc, "submitted").as_f64().unwrap());
    assert_eq!(0.0, obj_entry(&dc, "round").as_f64().unwrap(), "主办行没账本 ⇒ 轮次 0");
    let actors = obj_entry(&dc, "actors")
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(2, actors.len(), "撤回清场的行不进 actors");
    assert_eq!("userX", obj_entry(&actors[0], "actor").as_str().unwrap());
    assert_eq!(
        Some(11),
        obj_entry(&actors[0], "taskId").as_i64(),
        "taskId 给成员行主键（前端按它定位那一行意见）"
    );
    assert_eq!(
        Some(TaskState::Finished.code() as i64),
        obj_entry(&actors[0], "state").as_i64()
    );
}

// ═══ 轮次与账本（同一本 tf_addSignHistory，与后加签共用）═══

#[test]
fn dc04_round_and_ledger_append_only() {
    let mut host = row(10, "review", 5, TaskState::Doing.code(), "userA");
    assert_eq!(0, collect::last_round(&host), "没账本 ⇒ 轮次 0");

    collect::append_collect_history(&mut host, "userA", &["userX".into()], 1, "要两部门意见");
    host.variables
        .insert_i64(add_sign::round_key("review"), 1);
    assert_eq!(1, collect::last_round(&host), "每次拉人追加一条＝轮次推进");

    collect::append_revoke_history(&mut host, "userA", &["userY".into()], 1, "部门选错重新拉");
    assert_eq!(
        1,
        collect::last_round(&host),
        "撤回**不消耗**轮次：末条仍是当轮那枚 round"
    );

    let ledger = add_sign::history_of(&host);
    assert_eq!(2, ledger.len(), "只追加不覆盖");
    assert_eq!("要两部门意见", hist_entry(&ledger[0], "reason").as_str().unwrap());
    assert_eq!("userA", hist_entry(&ledger[0], "fromActor").as_str().unwrap());
    assert_eq!(1.0, hist_entry(&ledger[0], "round").as_f64().unwrap());
    assert_eq!(
        JsonValue::Bool(true),
        hist_entry(&ledger[1], "revoked"),
        "撤回条目带 revoked 标记"
    );
    assert_eq!(
        "部门选错重新拉",
        hist_entry(&ledger[1], "reason").as_str().unwrap()
    );
    assert_eq!(
        add_sign::ADD_SIGN_HISTORY,
        "tf_addSignHistory",
        "簿记键名与 java `FlowConst.ADD_SIGN_HISTORY` 逐字同"
    );
    assert_eq!("tf_addSignReason", collect::KEY_ADD_SIGN_REASON);
    assert_eq!("addSignRound_review", add_sign::round_key("review"));

    // 成员行的 reason 戳＝单行出证自足（不回查主办行）
    let mut member = row(11, "review", 10, TaskState::Doing.code(), "userX");
    assert_eq!(None, collect::reason_of(&member), "没盖戳 ⇒ 读不到，不编造空串");
    member
        .variables
        .insert_str(collect::KEY_ADD_SIGN_REASON, "要两部门意见");
    assert_eq!(
        Some("要两部门意见".to_string()),
        collect::reason_of(&member)
    );
}

// ═══ 三条解析期硬校验（spec 02 §4.4）═══

#[test]
fn dc05_parse_bans_mutex_countersign_and_apply_nodes() {
    // ① 与 allowAddSign 双开＝共用 tf_addSignOperators 的行为歧义
    let err = ModelParser::parse(&chain(
        r#""dynamicCountersign":true,"allowAddSign":true"#,
    ))
    .expect_err("应拦：双开");
    assert_eq!(
        "节点[review]不允许同时配置 allowAddSign 与 dynamicCountersign：\
         两档共用 tf_addSignOperators，行为靠节点属性分流，双开＝行为歧义",
        err.message()
    );

    // ② 会签节点＝池推进与征询挂起打架
    let err = ModelParser::parse(&chain(
        r#""dynamicCountersign":true,"performType":1,"countersignType":"PARALLEL""#,
    ))
    .expect_err("应拦：会签节点禁配");
    assert_eq!(
        "会签节点[review]不允许配置 dynamicCountersign：会签池推进语义与征询挂起语义打架",
        err.message()
    );

    // ③ 申请节点＝那手走实例撤回＋重新发起，不需要征询挂起
    let err = ModelParser::parse(&chain_with_apply(
        r#""assignee":"applicant","dynamicCountersign":true"#,
        "",
    ))
    .expect_err("应拦：申请节点禁配");
    assert_eq!(
        "申请节点[apply]不允许配置 dynamicCountersign：申请节点那手走实例撤回＋重新发起，不需要征询挂起",
        err.message()
    );

    // 三段解析期禁配的**位序**（parser.rs 收尾顺序＝后加签 → 已办撤回 → 动态会签，
    // 逐字对齐 java `ModelParser`）：同时开 `allowAddSign` 又是会签节点时，先报的是
    // 后加签那一段（`add_sign::validate_model`），不是动态会签的双开那一条——
    // 这一格钉住的是三段共用的"先到先报"次序，不是谁更严重。
    let err = ModelParser::parse(&chain(
        r#""dynamicCountersign":true,"allowAddSign":true,"performType":1"#,
    ))
    .expect_err("三段同时命中应报最前那段");
    assert!(
        err.message().contains("不允许配置 allowAddSign"),
        "{}",
        err.message()
    );

    // 合法配置：解析通过且开关落在节点模型上（非任务节点不读这两项）
    let model = ModelParser::parse(&chain(COLLECT_ON)).unwrap();
    let review = model.get_node("review").unwrap();
    assert_eq!(NodeType::Task, review.node_type);
    assert!(review.dynamic_countersign);
    assert!(!review.allow_add_sign);
    assert!(
        !model
            .get_node("final")
            .map(|n| n.dynamic_countersign)
            .unwrap_or(true)
    );
    assert!(
        !model
            .get_node("start")
            .map(|n| n.dynamic_countersign)
            .unwrap_or(true),
        "非任务节点不得读这一枚开关"
    );

    // 非法值解析期报配置错误（与 allowAddSign／allowRecall 同一把尺子，不静默当缺省）
    let err = ModelParser::parse(&chain(r#""dynamicCountersign":"maybe""#)).expect_err("应拦");
    assert_eq!(
        "节点[review]的 dynamicCountersign 配置非法: maybe",
        err.message()
    );
}

// ═══ 征询档推进闸（java `TaskModel.exec` 的 `else if (dynamicCountersign)` 那一支）═══

/// 成员办结 ⇒ 令牌停留本节点（主办仍 DOING）；主办收齐后拍板 ⇒ 才流出出边。
#[test]
fn dc06_collect_leg_gate_keeps_token_on_node_until_host_completes() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "collect-06", &chain(COLLECT_ON));
    let iid = start_to_review(&engine, &repo, define_id);
    let define = repo.find_define_by_id(define_id).unwrap().unwrap();
    let model = ModelParser::parse(&define.content_str()).unwrap();
    let node = model.get_node("review").unwrap().clone();

    let host = doing_named(&repo, iid, "review").unwrap();
    let mut inst = repo.find_instance_by_id(iid).unwrap().unwrap();
    inst.tasks = rows(&repo, iid);

    let created = engine
        .create_collect_member_rows(
            &define,
            &model,
            &mut inst,
            &node,
            &host,
            "userA",
            &["userX".to_string()],
            1,
            "",
        )
        .unwrap();
    assert_eq!(1, created.len());
    let member = &created[0];
    assert_eq!("review", member.task_name, "成员行同节点");
    assert_eq!(Some(host.task_id), member.parent_task_id, "parent＝主办行 id");
    assert!(member.is_doing());
    assert_eq!(
        Some(1),
        task_row(&repo, member.task_id)
            .variables
            .get_i64(&add_sign::round_key("review")),
        "轮次戳落在成员行变量上"
    );
    assert_eq!(
        0,
        task_row(&repo, member.task_id).perform_type,
        "成员行是普通任务行（performType 0），不是会签"
    );
    assert_eq!(
        2,
        doing_all_named(&repo, iid, "review").len(),
        "主办行仍 DOING（征询模式拉人不办结主办）＋成员行一条"
    );
    assert!(
        doing_named(&repo, iid, "final").is_none(),
        "拉人这一步不流转令牌"
    );

    // ① 成员办结 ⇒ 推进闸 merged=false ⇒ 令牌仍停留本节点
    engine
        .execute_task_sync(member.task_id, "userX", &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(
        doing_named(&repo, iid, "final").is_none(),
        "主办还没拍板 ⇒ 成员办结不得流出本节点"
    );
    assert!(doing_named(&repo, iid, "review").is_some(), "主办仍 DOING");

    // ② 收齐 ⇒ 主办拍板 ⇒ 走既有出边
    let host_now = doing_named(&repo, iid, "review").unwrap();
    assert!(
        collect::pending_members(&rows(&repo, iid), &host_now).is_empty(),
        "唯一成员已提交 ⇒ 闸门开"
    );
    engine
        .execute_task_sync(host_now.task_id, "userA", &submit(SUBMIT_AGREE))
        .unwrap();
    assert!(
        doing_named(&repo, iid, "final").is_some(),
        "主办拍板后令牌才流出"
    );

    // ③ 承重判据 1 的实弹面：主办推进后的 final 行 parent 同样是主办行 id，
    //    但成员行集合仍只认同节点同名那几枚
    let members_after = collect::member_rows(&rows(&repo, iid), &host_now);
    assert_eq!(
        vec![member.task_id],
        members_after.iter().map(|t| t.task_id).collect::<Vec<_>>(),
        "主办推进产出的 final 行（parent 也是主办行）不得被算成成员行"
    );
    assert!(!collect::is_member_row(&rows(&repo, iid), &host_now));
}

/// 离开本节点的那条腿（实例撤回/退回路径）要连成员行一起清场，不留孤儿待办
/// （`add_sign::abandon_doing_on_node` 的开关面已扩到 `dynamicCountersign`）。
#[test]
fn dc07_abandon_doing_on_node_covers_collect_rows() {
    let (engine, repo) = as_engine();
    let define_id = save_define(&repo, "collect-07", &chain(COLLECT_ON));
    let iid = start_to_review(&engine, &repo, define_id);
    let define = repo.find_define_by_id(define_id).unwrap().unwrap();
    let model = ModelParser::parse(&define.content_str()).unwrap();
    let node = model.get_node("review").unwrap().clone();

    let host = doing_named(&repo, iid, "review").unwrap();
    let mut inst = repo.find_instance_by_id(iid).unwrap().unwrap();
    inst.tasks = rows(&repo, iid);
    let created = engine
        .create_collect_member_rows(
            &define,
            &model,
            &mut inst,
            &node,
            &host,
            "userA",
            &["userX".to_string(), "userY".to_string()],
            1,
            "",
        )
        .unwrap();
    assert_eq!(2, created.len());

    let mut inst = repo.find_instance_by_id(iid).unwrap().unwrap();
    inst.tasks = rows(&repo, iid);
    let abandoned = add_sign::abandon_doing_on_node(&model, &mut inst, "review", "userA");
    assert_eq!(
        vec![host.task_id, created[0].task_id, created[1].task_id],
        {
            let mut sorted = abandoned.clone();
            sorted.sort_unstable();
            sorted
        },
        "主办＋两条成员行同属本节点 DOING，一并清场"
    );
    for t in inst.tasks.iter().filter(|t| abandoned.contains(&t.task_id)) {
        assert_eq!(TaskState::Abandon.code(), t.task_state);
    }
    assert_eq!(
        0,
        rows(&repo, iid)
            .iter()
            .filter(|t| t.task_state == TaskState::Abandon.code())
            .count(),
        "本腿只改聚合根副本、**不代写仓储**（返回值形状），调用方漏写回就留孤儿待办"
    );
    for t in inst.tasks.iter().filter(|t| abandoned.contains(&t.task_id)) {
        repo.update_task(t).unwrap();
    }
    assert_eq!(
        3,
        rows(&repo, iid)
            .iter()
            .filter(|t| t.task_state == TaskState::Abandon.code())
            .count(),
        "逐条写回后仓储才见得到 99"
    );
}

/// 逐字文案常量自证（spec 04／06 钉死的对外 msg，防常量被改后测试仍绿）。
#[test]
fn dc08_pinned_messages_match_spec() {
    assert_eq!(
        "节点[review]动态会签意见未收齐（已提交 0/2），不能办理",
        collect::msg_pending("review", 0, 2)
    );
    assert_eq!(
        "节点[review]没有进行中的动态会签",
        collect::msg_no_pending("review")
    );
    assert_eq!("无权限撤回该动态会签", collect::MSG_NO_PERMISSION_REVOKE);
    assert_eq!("动态会签成员任务不能发起加签", collect::MSG_MEMBER_PULL);
    assert_eq!(
        "操作人[userX]已在节点[review]的动态会签中",
        collect::msg_duplicate("userX", "review")
    );
    assert_eq!(
        "当前参与者不能执行该流程任务",
        collect::MSG_NOT_ALLOWED,
        "＝java WfErrEnum.NOT_ALLOWED_EXECUTE 逐字串"
    );
    assert_eq!("dynamicCountersign", collect::PROP_DYNAMIC_COUNTERSIGN);
    assert_eq!(
        "tf_addSignOperators",
        add_sign::ADD_SIGN_OPERATORS,
        "复用同一枚协议键（行为靠节点属性分流，不新增参数档）"
    );
}
