//! SYNC 持久化「`tf_` 去前缀冗余写业务表列」这一条腿的**读源**判据。
//!
//! 契约正文＝jeeflow-doc `spec/04-engine-ops.md`「实例变量只收数据类键」段末的**连带条款**：
//! `tf_*` 既已不进实例变量，这条腿的读源就必须跟着换——只能读**当次提交的执行参数**
//! （或本次办结那条任务行的变量），不得再读实例变量；**反面教材**＝java 根因修复后这条腿
//! 静默断了（`approval_comment` 落空、无测试钉住时不响），由 `jeeflow-persist` 的
//! `testSyncModeFullCycle` 真红暴露。本文件就是 rust 那枚对应的真链路用例。
//!
//! 为什么必须走引擎全链路而不是只调拦截器：本栈 `complete_task`
//! （`jeeflow-core/src/model.rs`）**只并 `f_`** 进实例变量，所以"`tf_` 写在实例变量里"这个
//! 形状在真链路里根本不会出现——只拿合成 Execution 喂实例变量，测的是断掉的旧读源、
//! 而不是引擎真会给的那份数据（本 crate 的 `test_persist_sync_task_update_field_permission`
//! 原先就踩在这个假形状上，本轮把它的入参落点改回 `exec.args`，断言未动）。
//!
//! 夹具纪律：**内联流程 JSON**，不往共享 `flows/` 加文件（那份目录是七语言仓镜像＋漂移门禁
//! 的核对对象，见 `jeeflow-core/tests/add_sign_after.rs` 同条注释）。

use jeeflow_core::engine::Execution;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::json::{FlowData, JsonValue};
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::{ProcessDefine, ProcessInstance};
use jeeflow_core::parser::{NodeModel, NodeType, ProcessModel};
use jeeflow_core::spi::{FlowInterceptor, ProcessRepository};
use jeeflow_core::{JeeflowEngineImpl, ServiceContext};
use jeeflow_persist::{
    DynamicTableWriter, FieldMeta, InMemoryMetaProvider, InMemoryTableWriter,
    PersistPostInterceptor, TableMeta,
};
use std::collections::HashMap;
use std::sync::Arc;

/// java 侧同名件：本栈 `postInterceptors` 按类全串解析（`resolve_interceptors` →
/// `ctx.named_interceptors`），所以注册键必须逐字用它。
const PERSIST_FQCN: &str = "com.mldong.jeeflow.persist.interceptor.PersistPostInterceptor";

const TABLE: &str = "biz_sync";

/// start → apply（发起人）→ task1（leader，title 只读／amount 可编辑）→ finish。
/// 逐字对齐 java `testSyncModeFullCycle` 的夹具形状（含"结束节点改名 finish"那一手，
/// 虽然内存 writer 不拼 SQL、`end` 不会踩保留字，但两栈的列名要能对着读）。
const FLOW: &str = r#"{
    "name": "persist-sync-full",
    "displayName": "SYNC 同步演进全链路",
    "type": "approval",
    "persistMode": "SYNC",
    "relTableName": "biz_sync",
    "postInterceptors": "com.mldong.jeeflow.persist.interceptor.PersistPostInterceptor",
    "nodes": [
        {"id":"start","type":"snaker:start","properties":{}},
        {"id":"apply","type":"snaker:task","properties":{"form":"f-apply","taskType":0,"performType":0,"assignee":"applicant"}},
        {"id":"task1","type":"snaker:task","properties":{"form":"f-task","taskType":0,"performType":0,"assignee":"leader","field":{"PERMISSION_f_title":1,"PERMISSION_f_amount":2}}},
        {"id":"finish","type":"snaker:end","properties":{}}
    ],
    "edges": [
        {"id":"e1","type":"snaker:transition","sourceNodeId":"start","targetNodeId":"apply","properties":{}},
        {"id":"e2","type":"snaker:transition","sourceNodeId":"apply","targetNodeId":"task1","properties":{}},
        {"id":"e3","type":"snaker:transition","sourceNodeId":"task1","targetNodeId":"finish","properties":{}}
    ]
}"#;

/// 业务表列。
///
/// ⚠️ 状态列名带 `_{状态码}` 后缀（`apply_10`／`task1_10`／`finish_20`）不是随手写的：
/// `put_state_field` 的候选档是 `["{节点ID}_{状态码}", "{节点ID}"]` **取第一个探测到的列**，
/// 而本栈测试用的 `InMemoryTableWriter` 没有 schema、沿用 `DynamicTableWriter::columns` 的
/// **缺省实现（全部候选都算存在）** ⇒ 第一候选恒胜。java 那边由真库探列，所以它的同名用例
/// 能钉无后缀的 `apply`／`task1`／`finish`。这里若改成无后缀列，落库读数会整片为空（假绿）。
fn biz_meta() -> TableMeta {
    TableMeta::new(TABLE, "SYNC 全链路业务表")
        .with_field(FieldMeta::new("title", "VARCHAR(100)", "标题"))
        .with_field(FieldMeta::new("amount", "DECIMAL(10,2)", "金额"))
        .with_field(FieldMeta::new("opinion", "VARCHAR(200)", "审批意见（tf_ 冗余列）"))
        .with_field(FieldMeta::new("apply_10", "INT", "apply 节点推进态"))
        .with_field(FieldMeta::new("task1_10", "INT", "task1 节点推进态"))
        .with_field(FieldMeta::new("finish_20", "INT", "结束定稿态"))
        .with_field(FieldMeta::new("process_instance_id", "BIGINT", "实例ID"))
        .with_field(FieldMeta::new("apply_user_id", "VARCHAR(50)", "发起人"))
        .with_field(FieldMeta::new("apply_dept_id", "VARCHAR(50)", "发起部门"))
}

fn str_of(row: &HashMap<String, JsonValue>, col: &str) -> Option<String> {
    row.get(col).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn num_of(row: &HashMap<String, JsonValue>, col: &str) -> Option<f64> {
    row.get(col).and_then(|v| v.as_f64())
}

fn submit(args: &[(&str, JsonValue)]) -> FlowData {
    let mut d = FlowData::new();
    for (k, v) in args {
        d.insert(k.to_string(), v.clone());
    }
    d
}

fn text(s: &str) -> JsonValue {
    JsonValue::Str(s.to_string())
}

// ═══ 主用例：引擎全链路，业务表列值＝本次提交的 tf_ 值 ═══

#[test]
fn test_sync_full_cycle_tf_column_reads_current_submission() {
    let writer = Arc::new(InMemoryTableWriter::new());
    let mut provider = InMemoryMetaProvider::new();
    provider.register(biz_meta());

    let repo = Arc::new(MemoryRepository::new());
    let mut ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    ctx.register_named_interceptor(
        PERSIST_FQCN,
        Arc::new(PersistPostInterceptor::new(Arc::new(provider), writer.clone())),
    );
    let engine = JeeflowEngineImpl::new(ctx);

    let mut define = ProcessDefine {
        id: 0,
        name: "persist-sync-full".into(),
        display_name: "SYNC 同步演进全链路".into(),
        define_type: "approval".into(),
        state: 1,
        content: FLOW.as_bytes().to_vec(),
        version: 1,
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    repo.save_define(&mut define).unwrap();

    // ① 发起：本栈 Start 节点**不挂**后置拦截器（`execute_node` 的 Start 分支注释：创建任务
    //    ≠ 节点执行完成），所以这一拍业务表还没有行——行由第一次"任务被执行"落。
    let inst = engine
        .start_sync(
            define.id,
            "user1",
            &submit(&[
                ("f_title", text("年假申请")),
                ("f_amount", JsonValue::Number(800.0)),
                ("u_deptId", text("D01")),
            ]),
        )
        .unwrap();
    let iid = inst.instance_id;
    assert!(
        writer.get_row_by_key(TABLE, "process_instance_id", iid).is_none(),
        "① 本栈发起不落业务行（首条 INSERT 落在第一次任务办结）"
    );

    // ② apply 办结 ⇒ exists=false 那一拍走 INSERT：f_ 全量去前缀 ＋ 上下文 ＋ apply_10=10
    let apply = repo
        .find_doing_tasks(iid, &[])
        .unwrap()
        .into_iter()
        .find(|t| t.task_name == "apply")
        .expect("发起后应在 apply 节点有待办");
    engine
        .execute_task_sync(apply.task_id, "user1", &submit(&[("submitType", JsonValue::Number(0.0))]))
        .unwrap();

    let row = writer
        .get_row_by_key(TABLE, "process_instance_id", iid)
        .expect("② apply 办结应 INSERT 业务行");
    assert_eq!(str_of(&row, "title").as_deref(), Some("年假申请"), "② f_ 去前缀落列");
    assert_eq!(num_of(&row, "amount"), Some(800.0), "② f_ 去前缀落列");
    assert_eq!(num_of(&row, "apply_10"), Some(10.0), "② 任务节点写 DOING(10) 状态列");
    assert_eq!(str_of(&row, "apply_user_id").as_deref(), Some("user1"), "② 上下文列");
    assert_eq!(str_of(&row, "apply_dept_id").as_deref(), Some("D01"), "② u_deptId 进上下文列");
    assert!(row.get("opinion").is_none(), "② 这次没提交 tf_，opinion 列不该凭空出现");

    // ③ task1 办结：同一次提交带 `tf_opinion` ＋ 只读 `f_title` ＋ 可编辑 `f_amount`
    //    落库后令牌继续流到 finish，同一次调用里把定稿也写完。
    let task1 = repo
        .find_doing_tasks(iid, &[])
        .unwrap()
        .into_iter()
        .find(|t| t.task_name == "task1")
        .expect("apply 办结后应在 task1 节点有待办");
    engine
        .execute_task_sync(
            task1.task_id,
            "leader",
            &submit(&[
                ("submitType", JsonValue::Number(1.0)),
                ("tf_opinion", text("同意，注意控制预算")),
                ("f_title", text("修改标题")),
                ("f_amount", JsonValue::Number(999.0)),
            ]),
        )
        .unwrap();

    let row = writer
        .get_row_by_key(TABLE, "process_instance_id", iid)
        .expect("③ 业务行仍在（先插后更，不得另起一行）");
    // —— 承重格：`tf_` 冗余列的值＝**本次提交**的值 ——
    assert_eq!(
        str_of(&row, "opinion").as_deref(),
        Some("同意，注意控制预算"),
        "③ tf_ 去前缀必须落到业务表 opinion 列（读源＝本次提交的执行参数）。\
         本格正是 spec/04 连带条款点名的『java 那边静默断了』那一档：改回读实例变量它就空。"
    );
    assert_eq!(
        str_of(&row, "title").as_deref(),
        Some("年假申请"),
        "③ 只读字段（PERMISSION_f_title=1）不得被任务节点更新"
    );
    assert_eq!(num_of(&row, "amount"), Some(999.0), "③ 可编辑字段（PERMISSION_f_amount=2）更新");
    assert_eq!(num_of(&row, "task1_10"), Some(10.0), "③ task1 状态列 DOING(10)");
    assert_eq!(num_of(&row, "finish_20"), Some(20.0), "③ 结束节点定稿写 FINISHED(20)");
    assert_eq!(
        writer.get_table_rows(TABLE).len(),
        1,
        "③ 全链路应只有一行（先插后更，幂等键 process_instance_id）"
    );

    // ④ 根因面（与 ③ 同一枚判据的两面）：`tf_*` 没进实例变量，业务列却写到了值
    //    ⇒ 读源只能是本次提交的执行参数。谁想靠"把 tf_ 并回实例变量"来过 ③，这一格就拦下。
    let ivars = &repo.find_instance_by_id(iid).unwrap().unwrap().variables;
    assert!(
        ivars.get("tf_opinion").is_none(),
        "④ tf_ 控制键不得进实例变量（spec/04「实例变量只收数据类键」）"
    );
    assert!(
        ivars.inner().keys().all(|k| !k.starts_with("tf_")),
        "④ 实例变量里不应有任何 tf_ 键"
    );
    // 任务行留痕收整包参数（修读源不得把审计留痕一起改掉）
    let done = repo.find_task_by_id(task1.task_id).unwrap().unwrap();
    assert_eq!(
        done.variables.get_str("tf_opinion"),
        Some("同意，注意控制预算"),
        "④ 办结那条任务行仍留着本次的 tf_"
    );
}

// ═══ 结束节点定稿那一拍不带出 tf_（与 java `!exists || taskNode` 同尺）═══

/// 合成 Execution 的形状（与 `persist_column_and_logs_195.rs` 的同名件同形，私有件跨不过
/// 测试边界故各自复刻一份最小的）。
fn exec_at(node_id: &str, node_type: NodeType, vars: FlowData, args: FlowData, state: i32) -> Execution {
    let model = ProcessModel {
        name: "proc".to_string(),
        display_name: "proc".to_string(),
        model_type: "6".to_string(),
        persist_mode: Some("SYNC".to_string()),
        rel_table_name: Some(TABLE.to_string()),
        nodes: vec![NodeModel {
            id: node_id.to_string(),
            node_type,
            display_name: node_id.to_string(),
            properties: HashMap::new(),
            reapply_mode: jeeflow_core::reapply::ReapplyMode::Normal,
            allow_add_sign: false,
            allow_recall: false,
            dynamic_countersign: false,
            add_sign_max_rounds: 0,
        }],
        edges: vec![],
        ..Default::default()
    };
    let instance = ProcessInstance {
        instance_id: 9301,
        parent_id: None,
        define_id: 1,
        state,
        parent_node_name: None,
        business_no: None,
        operator: "user1".to_string(),
        expire_time: None,
        variables: vars,
        tasks: vec![],
        create_time: None,
        create_user: Some("user1".to_string()),
        update_time: None,
        update_user: None,
        define: None,
    };
    let define = ProcessDefine {
        id: 1,
        name: "proc".into(),
        display_name: "proc".into(),
        define_type: "approval".into(),
        state: 1,
        content: vec![],
        version: 1,
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    let mut e = Execution::new(instance, model, define, "user1", args);
    e.current_node = Some(e.process_model.nodes[0].clone());
    e
}

/// 同一份 `exec.args` 带着 `tf_opinion`，任务节点那一拍写列、结束节点那一拍**不写**：
/// 定稿只落状态与上下文，不拿本次提交的 tf_ 再盖一遍任务节点刚写过的列
/// （＝ java `extractFields` 两处都传 `!exists || taskNode` 的那把尺子）。
#[test]
fn test_sync_end_node_does_not_rewrite_tf_column() {
    let writer = Arc::new(InMemoryTableWriter::new());
    let mut provider = InMemoryMetaProvider::new();
    provider.register(biz_meta());
    let itc = PersistPostInterceptor::new(Arc::new(provider), writer.clone());

    let mut vars = FlowData::new();
    vars.insert_str("f_title", "年假申请");
    let mut args = FlowData::new();
    args.insert_i64("submitType", 1);
    args.insert_str("tf_opinion", "同意");

    // 任务节点：行不存在 ⇒ INSERT，tf_ 随本次提交落列
    let mut e_task = exec_at("task1", NodeType::Task, vars.clone(), args.clone(), 10);
    itc.intercept(&mut e_task).unwrap();
    let row = writer
        .get_row_by_key(TABLE, "process_instance_id", 9301)
        .expect("任务节点应落库");
    assert_eq!(str_of(&row, "opinion").as_deref(), Some("同意"), "任务节点写 tf_ 冗余列");

    // 结束节点：行已存在 ⇒ 只定稿状态；把 opinion 改成哨兵值再看它会不会被盖
    writer
        .update_by_key(
            TABLE,
            &HashMap::from([("opinion".to_string(), text("任务节点写的原值"))]),
            "process_instance_id",
            9301,
        )
        .unwrap();
    let mut e_end = exec_at("finish", NodeType::End, vars, args, 20);
    itc.intercept(&mut e_end).unwrap();

    let row = writer.get_row_by_key(TABLE, "process_instance_id", 9301).unwrap();
    assert_eq!(
        str_of(&row, "opinion").as_deref(),
        Some("任务节点写的原值"),
        "结束节点定稿不得再写 tf_ 列（与 java 同尺；改回 include_task_fields=true 这一格就红）"
    );
    assert_eq!(num_of(&row, "finish_20"), Some(20.0), "结束节点仍要写定稿状态列");
    assert!(row.get("title").is_some(), "任务节点写的 f_ 列不受影响");
}
