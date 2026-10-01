//! issues/137 §3-1（spec/06 §2.12）· 门面内部异常出口不得泄漏内部原文（rust 腿）。
//!
//! 判据形状照 java `FacadeInternalErrorNoLeakTest`：走**真实门面调用路径**（不是只测纯函数），
//! 两侧都要有牙——
//! - **负向**：`JeeflowError::Internal`（sqlx 驱动原文／serde 解析原文／std ParseIntError 原文／
//!   裸包装原文／集成方 provider 原文）⇒ 出口 msg 必须**逐字**等于固定文案 `流程处理失败`，
//!   原文只进日志（门面出口的 `eprintln!` 腿，取回口径＝`JeeflowError::detail()`，
//!   纯函数侧断言在 `jeeflow-core/src/error.rs` 的 `test_i137_*`）；
//! - **正向／回归**：引擎自己写的中文契约文案必须**仍逐字透出**——判据收窄成"一律固定文案"
//!   会静默改写契约面（八栈＋十三壳＋前端 toast 按原文逐字对齐），这一组就是挡那个的。
//!
//! 本栈的判别式不需要 java 那五条运行时异常类型族启发式：`JeeflowError` 的变体本身就分好档了
//! （`Business`＝契约档逐字透出、`Internal`＝内部档固定文案），纯函数＝
//! `jeeflow_core::error::is_foreign_detail`（对应 java `JeeflowFacade.isForeignDetail`）。
//!
//! 覆盖面（spec §2.12 每栈至少两处）：① 门面顶层出口（本文件）；② bizData/JSON 解析族
//! （既有格：`cc_dedup_and_parse_msg_i141_139.rs` 的 `test_i139_deploy_failure_msg_is_java_verbatim`／
//! `test_i139_redeploy_failure_msg_same_verbatim` ＋ core `parser.rs` 的
//! `test_i139_parse_failure_msg_is_java_verbatim`——解析器原文只挂 `source()`，msg 逐字＝
//! 「读取流程定义 JSON 失败」；本文件契约组再钉一次 deploy 腿）。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::error::{JeeflowError, JeeflowResult};
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::json::{FlowData, JsonValue};
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::{
    DefineRow, InstanceRow, PageQuery, PageResult, ProcessDefine, ProcessInstance, ProcessTask,
    TaskRow, TaskState,
};
use jeeflow_core::spi::{ProcessRepository, UserSearchProvider};
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::Arc;

/// 八栈同一串逐字（spec/06 §2.12 第八条逐字契约文本）。
const FIXED: &str = "流程处理失败";

// ── 夹具：每条腿都返回同一枚预置错误的仓储（对应 java 测试里的 throwing proxy） ──

struct ErrRepo(JeeflowError);

impl ErrRepo {
    fn fail<T>(&self) -> JeeflowResult<T> {
        Err(self.0.clone())
    }
}

impl ProcessRepository for ErrRepo {
    fn find_define_by_id(&self, _: i64) -> JeeflowResult<Option<ProcessDefine>> { self.fail() }
    fn save_define(&self, _: &mut ProcessDefine) -> JeeflowResult<()> { self.fail() }
    fn update_define(&self, _: &ProcessDefine) -> JeeflowResult<()> { self.fail() }
    fn update_define_state(&self, _: i64, _: i32) -> JeeflowResult<()> { self.fail() }
    fn remove_define(&self, _: i64) -> JeeflowResult<()> { self.fail() }
    fn find_instance_by_id(&self, _: i64) -> JeeflowResult<Option<ProcessInstance>> { self.fail() }
    fn save_instance(&self, _: &mut ProcessInstance) -> JeeflowResult<()> { self.fail() }
    fn update_instance(&self, _: &ProcessInstance) -> JeeflowResult<()> { self.fail() }
    fn find_task_by_id(&self, _: i64) -> JeeflowResult<Option<ProcessTask>> { self.fail() }
    fn save_task(&self, _: &mut ProcessTask) -> JeeflowResult<()> { self.fail() }
    fn update_task(&self, _: &ProcessTask) -> JeeflowResult<()> { self.fail() }
    fn find_doing_tasks(&self, _: i64, _: &[String]) -> JeeflowResult<Vec<ProcessTask>> { self.fail() }
    fn find_done_tasks(&self, _: i64, _: &[String]) -> JeeflowResult<Vec<ProcessTask>> { self.fail() }
    fn find_history_tasks(&self, _: i64) -> JeeflowResult<Vec<ProcessTask>> { self.fail() }
    fn find_task_actors(&self, _: i64) -> JeeflowResult<Vec<String>> { self.fail() }
    fn add_task_actor(&self, _: i64, _: &[String]) -> JeeflowResult<()> { self.fail() }
    fn remove_task_actor(&self, _: i64, _: &[String]) -> JeeflowResult<()> { self.fail() }
    fn create_cc_instance(&self, _: i64, _: &str, _: &[String]) -> JeeflowResult<()> { self.fail() }
    fn update_cc_status(&self, _: i64, _: &str) -> JeeflowResult<()> { self.fail() }
    fn page_todo_tasks(&self, _: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> { self.fail() }
    fn page_done_tasks(&self, _: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> { self.fail() }
    fn page_instances(&self, _: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> { self.fail() }
    fn page_cc_instances(&self, _: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> { self.fail() }
    fn page_defines(&self, _: &PageQuery) -> JeeflowResult<PageResult<DefineRow>> { self.fail() }
    fn count_todo_tasks(&self, _: &str) -> JeeflowResult<i64> { self.fail() }
    fn get_all_instances(&self) -> JeeflowResult<Vec<ProcessInstance>> { self.fail() }
    fn get_all_tasks(&self) -> JeeflowResult<Vec<ProcessTask>> { self.fail() }
}

fn facade_over(err: JeeflowError) -> JeeflowFacade {
    let ctx = ServiceContext::new()
        .with_repository(Arc::new(ErrRepo(err)) as Arc<dyn ProcessRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(900000)));
    JeeflowFacade::new(ctx)
}

fn page_args() -> HashMap<String, Json> {
    let mut a = HashMap::new();
    a.insert("operator".to_string(), json!("user1"));
    a.insert("pageNum".to_string(), json!(1));
    a.insert("pageSize".to_string(), json!(10));
    a
}

/// 内部原文经真实门面出口后：msg 逐字＝固定文案，原文与前缀一个字符都不许出现。
async fn assert_fixed_at_exit(internal_text: &str, leak_markers: &[&str]) {
    let facade = facade_over(JeeflowError::Internal(internal_text.to_string()));
    let resp = facade.flow("processInstance/page", &page_args()).await;
    assert_eq!(resp["code"], 99999999, "内部异常仍走业务失败码：{resp}");
    assert_eq!(resp["msg"], FIXED, "出口 msg 必须逐字＝固定文案：{resp}");
    let msg = resp["msg"].as_str().unwrap_or_default();
    for marker in leak_markers {
        assert!(!msg.contains(marker), "内部原文不得进 msg，泄漏了 {marker}：{resp}");
    }
    assert!(!msg.contains("内部错误"), "旧出口前缀「内部错误: 」不得复活：{resp}");
}

// ── 负向：泄漏原文的各形状 ⇒ 出口只给固定文案 ──

/// serde_json 解析器真原文（`from_str` 当场造，不手写仿品）。
#[tokio::test]
async fn test_i137_serde_parse_original_text_not_leaked() {
    let raw = serde_json::from_str::<Json>("{'这不是合法JSON").unwrap_err().to_string();
    assert!(raw.contains("line 1"), "夹具前提：serde 原文带位置信息，实得 {raw}");
    assert_fixed_at_exit(&raw, &["line 1", "string"]).await;
}

/// sqlx 驱动错误原文形状（repository-sqlx 全仓 `map_err(|e| Internal(e.to_string()))`，
/// 真库腿在 `jeeflow-repository-sqlx`；这里按同形状喂驱动风格原文）。
#[tokio::test]
async fn test_i137_sqlx_driver_original_text_not_leaked() {
    assert_fixed_at_exit(
        "error communicating with server: Connection refused (os error 61)",
        &["os error 61", "Connection refused", "communicating"],
    )
    .await;
}

/// std `parse::<i64>()` 的 ParseIntError 真原文（java 侧 NumberFormatException
/// `For input string: "x"` 的本栈等价物）。
#[tokio::test]
async fn test_i137_parse_int_error_original_text_not_leaked() {
    let raw = "x".parse::<i64>().unwrap_err().to_string();
    assert_eq!(raw, "invalid digit found in string", "夹具前提：ParseIntError 原文");
    assert_fixed_at_exit(&raw, &["invalid digit"]).await;
}

/// 裸包装形状（java 判别式第 3 条 `new RuntimeException(e)` 的本栈等价物：
/// message 就是下层 cause 原文，引擎没写过它）。
#[tokio::test]
async fn test_i137_bare_wrapper_cause_text_not_leaked() {
    assert_fixed_at_exit("内部驱动细节 12345", &["12345", "内部驱动细节"]).await;
}

/// 集成方/第三方 provider 返回的错误原文（经仓储腿进 Internal 档）。
#[tokio::test]
async fn test_i137_third_party_text_via_repo_not_leaked() {
    assert_fixed_at_exit(
        "third-party provider exploded: token=abc123",
        &["token=abc123", "provider exploded"],
    )
    .await;
}

/// 第三方 provider 走**真实 SPI 路径**：`IUserSearchProvider.page` 返回 Internal，
/// `processTask/candidatePage` 无模型候选时 `?` 直接把它送到门面出口。
#[tokio::test]
async fn test_i137_user_search_provider_error_not_leaked() {
    struct ThrowingSearchProvider;
    impl UserSearchProvider for ThrowingSearchProvider {
        fn page(&self, _: &PageQuery) -> JeeflowResult<PageResult<HashMap<String, JsonValue>>> {
            Err(JeeflowError::Internal(
                "第三方用户搜索 provider 驱动原文 secret=xyz789".into(),
            ))
        }
        fn find_by_id(&self, _: &str) -> JeeflowResult<Option<HashMap<String, JsonValue>>> {
            Err(JeeflowError::Internal(
                "第三方用户搜索 provider 驱动原文 secret=xyz789".into(),
            ))
        }
    }

    let repo = Arc::new(MemoryRepository::new());
    seed_task(&repo, 9301, TaskState::Doing.code(), &[]);
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn jeeflow_core::spi::ProcessExtRepository>)
        .with_user_search_provider(Arc::new(ThrowingSearchProvider))
        .with_id_generator(Arc::new(AtomicIdGenerator::new(900000)));
    let facade = JeeflowFacade::new(ctx);

    let mut args = HashMap::new();
    args.insert("processTaskId".to_string(), json!(9301));
    let resp = facade.flow("processTask/candidatePage", &args).await;
    assert_eq!(resp["code"], 99999999, "{resp}");
    assert_eq!(resp["msg"], FIXED, "provider 原文不得进 msg：{resp}");
    assert!(
        !resp["msg"].as_str().unwrap_or_default().contains("xyz789"),
        "provider 原文泄漏：{resp}"
    );
}

/// 门面自己的内部档（扩展仓未注册）同样只给固定文案——英文内部文本
/// 「ExtRepository not registered」改前是带「内部错误: 」前缀整个外透的。
#[tokio::test]
async fn test_i137_ext_repo_missing_internal_not_leaked() {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo as Arc<dyn ProcessRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(900000)));
    let facade = JeeflowFacade::new(ctx); // 故意不挂 ext repository

    for action in ["processDesign/page", "processSurrogate/page"] {
        let resp = facade.flow(action, &page_args()).await;
        assert_eq!(resp["code"], 99999999, "{action}: {resp}");
        assert_eq!(resp["msg"], FIXED, "{action}: {resp}");
        assert!(
            !resp["msg"].as_str().unwrap_or_default().contains("ExtRepository"),
            "{action} 泄漏内部英文文本：{resp}"
        );
    }
}

// ── 正向／回归：引擎契约文案仍逐字透出（判据收窄成"一律固定文案"时这一组红） ──

/// java `contractExceptionMessageStillPassesThroughVerbatim` 的本栈等价格：
/// `Business` 档（契约异常族）经同一条门面出口**逐字**透出。
#[tokio::test]
async fn test_i137_business_contract_text_passes_through_verbatim() {
    let facade = facade_over(JeeflowError::Business("刷新令牌已失效或不存在".into()));
    let resp = facade.flow("processInstance/page", &page_args()).await;
    assert_eq!(resp["code"], 99999999, "{resp}");
    assert_eq!(resp["msg"], "刷新令牌已失效或不存在", "契约文案必须逐字留在 msg：{resp}");
}

// ── 夹具：直连内存仓种任务行（不走引擎建单，removeTaskActor 守卫只看任务行与参与者表） ──

fn seed_task(repo: &MemoryRepository, task_id: i64, state: i32, actors: &[&str]) {
    let mut define = ProcessDefine {
        id: 9101,
        name: "i137-flow".into(),
        display_name: "i137 流程".into(),
        define_type: "approval".into(),
        state: 1,
        content: b"{}".to_vec(), // 无节点候选的合法占位（ModelParser 解析不出候选即可）
        version: 0,
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    repo.save_define(&mut define).unwrap();
    let mut inst = ProcessInstance {
        instance_id: 9201,
        parent_id: None,
        define_id: 9101,
        state: 10,
        parent_node_name: None,
        business_no: None,
        operator: "user1".into(),
        expire_time: None,
        variables: FlowData::new(),
        tasks: Vec::new(),
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
        define: None,
    };
    repo.save_instance(&mut inst).unwrap();
    let mut task = ProcessTask {
        task_id,
        process_instance_id: 9201,
        task_name: "node_1".into(),
        display_name: "审批".into(),
        task_type: 0,
        perform_type: 0,
        task_state: state,
        actor_id: None,
        actor_ids: actors.iter().map(|s| s.to_string()).collect(),
        finish_time: None,
        expire_time: None,
        form_key: None,
        parent_task_id: None,
        variables: FlowData::new(),
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    repo.save_task(&mut task).unwrap();
    if !actors.is_empty() {
        repo.add_task_actor(task_id, &actors.iter().map(|s| s.to_string()).collect::<Vec<_>>())
            .unwrap();
    }
}

fn memory_facade() -> (JeeflowFacade, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn jeeflow_core::spi::ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(900000)));
    (JeeflowFacade::new(ctx), repo)
}

async fn call_remove(facade: &JeeflowFacade, task_id: i64, actor_ids: Json, operator: &str) -> Json {
    let mut args = HashMap::new();
    args.insert("processTaskId".to_string(), json!(task_id));
    args.insert("actorIds".to_string(), actor_ids);
    args.insert("operator".to_string(), json!(operator));
    facade.flow("processTask/removeTaskActor", &args).await
}

/// spec/06 失败文案表里的逐字串经门面出口原样透出（这一组同样重要——判据过宽会把
/// 契约面静默改掉，而 java 侧这类文案一处测试都没钉）。
#[tokio::test]
async fn test_i137_contract_failure_texts_still_verbatim_at_exit() {
    let (facade, repo) = memory_facade();

    // operator 必填（withdraw 硬必填档）
    let mut no_op = HashMap::new();
    no_op.insert("id".to_string(), json!(9201));
    let r = facade.flow("processInstance/withdraw", &no_op).await;
    assert_eq!(r["msg"], "operator 必填", "{r}");

    // 任务不存在（removeTaskActor 的逐字判据）
    let r = call_remove(&facade, 424242, json!(["9001"]), "flow.admin").await;
    assert_eq!(r["msg"], "任务不存在", "{r}");

    // 任务非进行中，不可摘除参与人（已办结任务行）
    seed_task(&repo, 9311, TaskState::Finished.code(), &["user2"]);
    let r = call_remove(&facade, 9311, json!(["user2"]), "flow.admin").await;
    assert_eq!(r["msg"], "任务非进行中，不可摘除参与人", "{r}");

    // 至少需保留一名参与人（进行中任务、唯一参与人被摘）
    seed_task(&repo, 9312, TaskState::Doing.code(), &["user2"]);
    let r = call_remove(&facade, 9312, json!(["user2"]), "flow.admin").await;
    assert_eq!(r["msg"], "至少需保留一名参与人", "{r}");

    // 读取流程定义 JSON 失败（覆盖面②解析族：msg 只有基准逐字文案，解析器原文不进 msg）
    let mut bad = HashMap::new();
    bad.insert("content".to_string(), json!("{ 这不是合法的流程定义 JSON "));
    bad.insert("operator".to_string(), json!("user1"));
    let r = facade.flow("processDefine/deploy", &bad).await;
    assert_eq!(r["msg"], "读取流程定义 JSON 失败", "{r}");

    // 未知 action（门面自己的既有固定文案，不被判别式波及）
    let r = facade.flow("not/anAction", &HashMap::new()).await;
    assert_eq!(r["msg"], "未知 action: not/anAction", "{r}");
}

/// 原文确实还在（不是"把原文整个丢掉"的假修）：门面出口对内部档记日志，
/// 日志文本口径＝`JeeflowError::detail()`——在错误对象上直接验证取回路径。
#[test]
fn test_i137_original_text_retrievable_for_logging() {
    let err = JeeflowError::Internal("mysql execute: Deadlock found when trying to get lock".to_string());
    assert_eq!(err.message(), FIXED, "出口一侧是固定文案");
    let detail = err.detail();
    assert!(
        detail.contains("Deadlock found when trying to get lock"),
        "日志一侧必须能取回原文（门面出口的 eprintln! 用的就是这一份）：{detail}"
    );
}
