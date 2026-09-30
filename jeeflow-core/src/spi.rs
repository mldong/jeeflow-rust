//! SPI trait definitions — aligned with Java reference implementation.
//! All repository and provider traits are SYNCHRONOUS (no async/BoxFuture).
//! Only the engine methods are async. This matches Go/Java patterns.
//! spec/05-spi.md

use crate::error::JeeflowResult;
use crate::json::JsonValue;
use crate::model::*;
use std::collections::HashMap;

// ═══════════════════════════════════════════════════════
// IProcessRepository (21+ methods) — spec/05
// ═══════════════════════════════════════════════════════

pub trait ProcessRepository: Send + Sync {
    // ═══ Define operations ═══
    fn find_define_by_id(&self, define_id: i64) -> JeeflowResult<Option<ProcessDefine>>;
    fn save_define(&self, define: &mut ProcessDefine) -> JeeflowResult<()>;
    fn update_define(&self, define: &ProcessDefine) -> JeeflowResult<()>;
    fn update_define_state(&self, define_id: i64, state: i32) -> JeeflowResult<()>;
    fn remove_define(&self, define_id: i64) -> JeeflowResult<()>;

    // ═══ Instance operations ═══
    fn find_instance_by_id(&self, instance_id: i64) -> JeeflowResult<Option<ProcessInstance>>;
    fn save_instance(&self, instance: &mut ProcessInstance) -> JeeflowResult<()>;
    fn update_instance(&self, instance: &ProcessInstance) -> JeeflowResult<()>;

    // ═══ Task operations ═══
    fn find_task_by_id(&self, task_id: i64) -> JeeflowResult<Option<ProcessTask>>;
    fn save_task(&self, task: &mut ProcessTask) -> JeeflowResult<()>;
    fn update_task(&self, task: &ProcessTask) -> JeeflowResult<()>;
    fn find_doing_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>>;
    fn find_done_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>>;
    fn find_history_tasks(&self, instance_id: i64) -> JeeflowResult<Vec<ProcessTask>>;

    // ═══ Task actor operations ═══
    fn find_task_actors(&self, task_id: i64) -> JeeflowResult<Vec<String>>;
    /// 任务参与者写入口（`wf_process_task_actor.actor_id`，**归属列**）。
    ///
    /// issues/142 B 批 · spec 06-facade.md §2.11「归属值写侧归一」的**写侧兜底层**义务
    /// （与 [`Self::create_cc_instance`] 同一条尺子，只是换到任务侧）：入参集合必须自己再过一遍
    /// [`crate::model::normalize_actors`]——**逐元素 trim、空串/纯空白丢弃、同一次调用内折叠**，
    /// 落库值取 trim 后的串。判据必须落在这一层而不只落在门面/引擎漏斗里：绕过它们直连仓储的
    /// 调用方（集成层、第三方仓储消费者）同样不得把空归属值灌进 `actor_id`——那正是 issues/129
    /// 那族"空 operator 读全库"的上游进水口。
    ///
    /// ⚠️ 本仓两仓（内存仓 / sqlx 仓）**必须同答案**（issues/117 场景 27 那把尺子）：只有一仓
    /// 挡空值＝"绕过门面"的调用方在真库里灌空值/灌重复。
    ///
    /// 反向哨兵（§2.11 硬要求④）：`"0"` 这类"看起来像空"的正常 id **不得**被当成空值丢掉。
    ///
    /// 主键另判一档：`task_id` 不是归属值，**不**参与归一——它是调用方给错了（缺失/空/0），
    /// 由参数解析层响亮报错，不得拿 `0` 当 id 落库（§2.11 末段）。
    fn add_task_actor(&self, task_id: i64, actors: &[String]) -> JeeflowResult<()>;
    /// 任务参与者删除（issues/142 §9.2 第二批 · §2.11 删除位与写侧同一条尺子）。
    ///
    /// 实现方必须：① 删除列表先过 [`crate::model::normalize_actors`]（与 [`Self::add_task_actor`]
    /// 同一枚，不另立尺子）——存量行存的是 trim 后的串，入参带空格时按原样比会**静默不中**
    /// （转办"摘原人"那一腿就落在这种"报成功却没删"上）；② **归一后为空 ⇒ 一条都不删**（早退）——
    /// 空串入参在历史 `actor_id=''` 的脏行上会批量误删（issues/129 的删除位对偶）。
    /// 本仓两仓（内存 / sqlx）都按这一条实现，同一条判据给同一个答案（issues/117 场景 27）。
    fn remove_task_actor(&self, task_id: i64, actors: &[String]) -> JeeflowResult<()>;

    // ═══ CC operations ═══
    /// 建 cc 行的最底层写入口（`wf_process_cc_instance`）。
    ///
    /// issues/141 G10「空不创建行」（spec 06-facade.md §2.10）：入参里的**空串、纯空白一律丢弃**，
    /// 落库值取 **trim 后的串**（`" 123 "` 与 `"123"` 是同一个人，才与 G2 的写侧判重咬合）。
    /// 判据必须落在这一层而不只落在引擎漏斗里——绕过 `parse_cc_actors`／门面直连仓储的调用方
    /// （集成层、第三方仓储消费者）同样不得把空归属值灌进 `actor_id`，那正是 issues/129
    /// 那族"空 operator 读全库"的病根。归一腿用 [`crate::model::normalize_cc_actors`]，
    /// 本仓两仓（内存仓 / sqlx 仓）共用它，覆写本方法的第三方仓储**也必须**过这一支。
    fn create_cc_instance(&self, instance_id: i64, creator: &str, actor_ids: &[String]) -> JeeflowResult<()>;
    /// 抄送已读回写（`wf_process_cc_instance.state` → 1）。
    ///
    /// issues/142 B 批 · spec 06-facade.md §2.11 表第四行：入参 `actor_id`（门面的 `operator`）
    /// **必须先归一再比**——取 trim 后的值，trim 后为空则按各仓既有的"缺参数/默认操作人"档处理。
    /// 不归一就直接比，空 operator 会把 `state=1` 打到历史 `actor_id=''` 的脏行上（越权改别人的
    /// 已读位），带空格的同一人又永远命不中。判据本体＝[`crate::model::normalize_actors`]
    /// （单值档取归一后的那一个元素），**不要另抄一份 `trim()` 判据**。
    fn update_cc_status(&self, instance_id: i64, actor_id: &str) -> JeeflowResult<()>;

    /// issues/141 G2 写侧判重的**读侧**（spec 06 §4「抄送写侧判重＝幂等空操作」）：
    /// 取某实例**已存在**的 cc 行 actor id，供建 cc 的三条入口判重用。
    ///
    /// default 返回空集＝不判重——未覆写的第三方仓储维持旧行为（全量建行、全量 fire），
    /// SPI 源码兼容不破。jeeflow 自带的两仓（内存仓 / sqlx 仓）**必须**覆写：
    /// 否则 issues/141 G1 那条「同一栈两个仓储两个答案」的分叉在写侧重演一遍。
    fn find_cc_actor_ids(&self, instance_id: i64) -> JeeflowResult<Vec<String>> {
        let _ = instance_id;
        Ok(Vec::new())
    }

    /// issues/141 G2：写侧幂等建 cc 行，返回**实际新建**的 actor 子集。
    ///
    /// 同一 `(instance_id, actor_id)` 已有 cc 行时**跳过**——①不新增行、②不重置未读状态
    /// （`state` 保持原值，owner 2026-09-29 明确「不需要重置」，不产生"再提醒一次"语义）、
    /// ③不更新原行时间（`create_time`/`update_time` 逐字不变）；重复抄送同一个人在数据面上
    /// 是 no-op。**判重在写侧**：查询侧不引入 DISTINCT，历史重复行也不清理。
    /// 同一次调用内重复给同一个人也折叠（只落一行）。
    ///
    /// 为什么返回子集而不是 `()`：spec 11.2 原则 1「码值表达发生了什么事实」⇒
    /// 没发生"创建"就不得 fire `CC_CREATE`（码 4）。三条入口（发起 `f_ccActors`／办理
    /// `tf_ccActors`／门面手动 `processInstance/createCCInstance`）逐人 fire 的入参一律换成
    /// 这个子集，子集为空则整支不 fire（不空转、也不照旧按原始请求全量 fire）。
    ///
    /// 未覆写 [`Self::find_cc_actor_ids`] 的第三方仓储走本 default ⇒ 与旧
    /// `create_cc_instance` 逐字一致（全量插入、全量返回），不静默改变既有集成方行为。
    ///
    /// issues/141 G10「空不创建行」（spec 06 §2.10）写侧兜底第二层：入参先过
    /// [`crate::model::normalize_cc_actors`]（空串/纯空白丢弃、值取 trim 后的串）——
    /// 返回的子集是**拿去 fire `CC_CREATE` 的那一批**，含空值就等于对空抄送人发了码 4。
    fn create_cc_instance_if_absent(
        &self,
        instance_id: i64,
        creator: &str,
        actor_ids: &[String],
    ) -> JeeflowResult<Vec<String>> {
        // 先取快照再写：不在持锁期间做插入（本仓内存仓有"持锁跨 await 自死锁"的前科）。
        // G10：归一在取快照之前——空值既进不了子集，也进不了下面的判重比较。
        let actors = crate::model::normalize_cc_actors(actor_ids);
        let mut existing = self.find_cc_actor_ids(instance_id)?;
        let mut fresh: Vec<String> = Vec::new();
        for actor_id in &actors {
            if existing.iter().any(|a| a == actor_id) {
                continue; // 已有 cc 行 ⇒ 幂等空操作
            }
            existing.push(actor_id.clone()); // 同一次调用内的重复也算"已存在"
            fresh.push(actor_id.clone());
        }
        if !fresh.is_empty() {
            self.create_cc_instance(instance_id, creator, &fresh)?;
        }
        Ok(fresh)
    }

    // ═══ Page queries ═══
    fn page_todo_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>>;
    fn page_done_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>>;
    fn page_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>>;

    /// 我的抄送（`processInstance/ccList` 的取数腿）。
    ///
    /// **归属条件必填**（issues/141 G1 · spec 06 §2.5「抄送分页同一条尺子」）：查询必须带
    /// 归属列 `cc.actor_id` 的**有效**条件；条件**整条没给**或**给了但是空值**时
    /// **返回空页**（`record_count=0`、`rows=[]`），严禁退化成"这条条件不加"而把全部实例摊出去。
    ///
    /// 有效条件＝值非 null／字符串去空白后非空／集合非空。判据的**唯一出口**是
    /// [`crate::model::has_effective_cc_ownership`]，本仓两腿（内存仓 / sqlx 仓）共用它，
    /// 第三方仓储覆写本方法时**也必须**用它——"同一栈两个仓储两个答案"正是
    /// issues/117 场景 27 立过法的那一类（那把尺子本轮从 `pageInstances` 扩到 ccList）。
    ///
    /// 只收归属谓词：**非归属列的空值放行不变**（`m_like_business_no=""` 这类"没填"
    /// 依旧按各仓既有语义处理，不得顺手改成空页）。
    fn page_cc_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>>;
    fn page_defines(&self, query: &PageQuery) -> JeeflowResult<PageResult<DefineRow>>;
    fn count_todo_tasks(&self, user_id: &str) -> JeeflowResult<i64>;

    // ═══ Stats bulk queries (issues/103) ═══
    fn get_all_instances(&self) -> JeeflowResult<Vec<ProcessInstance>>;
    fn get_all_tasks(&self) -> JeeflowResult<Vec<ProcessTask>>;
}

// ═══════════════════════════════════════════════════════
// IProcessExtRepository (14 methods) — spec/05
// ═══════════════════════════════════════════════════════

pub trait ProcessExtRepository: Send + Sync {
    // ═══ Design operations ═══
    fn find_design_by_id(&self, design_id: i64) -> JeeflowResult<Option<ProcessDesign>>;
    fn save_design(&self, design: &mut ProcessDesign) -> JeeflowResult<()>;
    fn update_design(&self, design: &ProcessDesign) -> JeeflowResult<()>;
    fn remove_design(&self, design_id: i64) -> JeeflowResult<()>;
    fn page_designs(&self, query: &PageQuery) -> JeeflowResult<PageResult<ProcessDesign>>;

    // ═══ Design history ═══
    fn save_design_his(&self, his: &mut ProcessDesignHis) -> JeeflowResult<()>;
    fn list_design_his(&self, design_id: i64) -> JeeflowResult<Vec<ProcessDesignHis>>;

    // ═══ Surrogate operations ═══
    fn find_surrogate_by_id(&self, surrogate_id: i64) -> JeeflowResult<Option<ProcessSurrogate>>;
    fn save_surrogate(&self, surrogate: &mut ProcessSurrogate) -> JeeflowResult<()>;
    fn update_surrogate(&self, surrogate: &ProcessSurrogate) -> JeeflowResult<()>;
    fn remove_surrogate(&self, surrogate_id: i64) -> JeeflowResult<()>;
    fn page_surrogates(&self, query: &PageQuery) -> JeeflowResult<PageResult<ProcessSurrogate>>;
    fn get_surrogate(&self, operator: &str, process_name: &str, time: &str) -> JeeflowResult<Option<ProcessSurrogate>>;
}

// ═══════════════════════════════════════════════════════
// IUserProvider — spec/05
// ═══════════════════════════════════════════════════════

pub trait UserProvider: Send + Sync {
    fn get_user(&self, user_id: &str) -> JeeflowResult<Option<UserInfo>>;
}

// ═══════════════════════════════════════════════════════
// IOrgUserProvider — spec/05 (v1.6.0)
// ═══════════════════════════════════════════════════════

pub trait OrgUserProvider: Send + Sync {
    fn find_dept_leaders(&self, dept_id: &str) -> JeeflowResult<Vec<String>>;
    fn find_dept_main_leaders(&self, dept_id: &str) -> JeeflowResult<Vec<String>>;
    fn find_by_role(&self, role_code: &str) -> JeeflowResult<Vec<String>>;
}

// ═══════════════════════════════════════════════════════
// IUserSearchProvider — spec/06 §4.3/§6 (v1.2.0)
// ═══════════════════════════════════════════════════════

pub trait UserSearchProvider: Send + Sync {
    fn page(&self, query: &PageQuery) -> JeeflowResult<PageResult<HashMap<String, JsonValue>>>;
    fn find_by_id(&self, user_id: &str) -> JeeflowResult<Option<HashMap<String, JsonValue>>>;
}

// ═══════════════════════════════════════════════════════
// IJsonProvider — spec/05
// ═══════════════════════════════════════════════════════

pub trait JsonProvider: Send + Sync {
    fn to_json(&self, value: &JsonValue) -> String;
    fn from_json(&self, json: &str) -> JeeflowResult<JsonValue>;
    fn is_json(&self, s: &str) -> bool;
}

// ═══════════════════════════════════════════════════════
// IExpressionEvaluator — spec/05
// ═══════════════════════════════════════════════════════

pub trait ExpressionEvaluator: Send + Sync {
    fn eval(&self, expression: &str, context: &HashMap<String, JsonValue>) -> JeeflowResult<JsonValue>;
}

// ═══════════════════════════════════════════════════════
// ITransactionTemplate — spec/05
// ═══════════════════════════════════════════════════════

pub trait TransactionTemplate: Send + Sync {
    /// Execute an action within a transaction.
    /// Uses a boxed closure for dyn compatibility.
    fn execute_in_tx(&self, action: Box<dyn FnOnce() -> JeeflowResult<Box<dyn std::any::Any + Send>> + Send>) -> JeeflowResult<Box<dyn std::any::Any + Send>>;
}

// ═══════════════════════════════════════════════════════
// IIdGenerator — spec/05
// ═══════════════════════════════════════════════════════

pub trait IdGenerator: Send + Sync {
    fn next_id(&self) -> i64;
}

// ═══════════════════════════════════════════════════════
// IActionPermissionProvider — spec/06 §2.6 (v1.8.3)
// ═══════════════════════════════════════════════════════

pub trait ActionPermissionProvider: Send + Sync {
    fn permission_codes(&self, action: &str) -> Vec<String>;
}

/// Default implementation: wf:{action / → :}
pub struct DefaultActionPermissionProvider;

impl ActionPermissionProvider for DefaultActionPermissionProvider {
    fn permission_codes(&self, action: &str) -> Vec<String> {
        let code = format!("wf:{}", action.replace('/', ":"));
        vec![code]
    }
}

// ═══════════════════════════════════════════════════════
// FlowInterceptor — concepts/04
// ═══════════════════════════════════════════════════════

pub trait FlowInterceptor: Send + Sync {
    fn intercept(&self, execution: &mut crate::engine::Execution) -> JeeflowResult<()>;
    fn order(&self) -> i32 { 0 }
}

// ═══════════════════════════════════════════════════════
// BizDataReader — processInstance/bizData 读侧（issues/30）
// ═══════════════════════════════════════════════════════

/// 按 relTableName + process_instance_id 回显业务表单条。
pub trait BizDataReader: Send + Sync {
    fn read_by_process_instance(
        &self,
        table_name: &str,
        process_instance_id: i64,
    ) -> JeeflowResult<Option<HashMap<String, JsonValue>>>;
}

// ═══════════════════════════════════════════════════════
// AssignmentHandler — guides/07
// ═══════════════════════════════════════════════════════

pub trait AssignmentHandler: Send + Sync {
    fn assign(&self, execution: &crate::engine::Execution) -> JeeflowResult<String>;
}

// ═══════════════════════════════════════════════════════
// DecisionHandler
// ═══════════════════════════════════════════════════════

pub trait DecisionHandler: Send + Sync {
    fn decide(&self, execution: &crate::engine::Execution) -> JeeflowResult<String>;
}

// ═══════════════════════════════════════════════════════
// CandidateHandler
// ═══════════════════════════════════════════════════════

pub trait CandidateHandler: Send + Sync {
    fn handle(&self, node: &crate::parser::NodeModel) -> JeeflowResult<Vec<Candidate>>;
}

#[derive(Debug, Clone)]
pub struct Candidate {
    pub id: String,
    pub display_name: String,
}

// ═══════════════════════════════════════════════════════
// CustomNodeHandler — 记录类（snaker:custom）节点处理器
// ═══════════════════════════════════════════════════════

/// 记录类节点 `properties.clazz` 缺省时返回值落进流程变量的键
/// （逐字对齐 java `FlowConst.CUSTOM_RETURN_VAL`，spec/02 §6）。
pub const CUSTOM_RETURN_VAL: &str = "custom_return_val";

/// 记录类（`snaker:custom`）节点处理器（issues/142 A 批 · spec/02 §6.1／§6.2）。
///
/// **为什么是"按名注册表"而不是反射**：Rust 没有 `Class.forName(clazz)` 这一层，
/// `clazz` 串在共享夹具（`flows/08-custom-node.json`）里写的是 Java FQCN，
/// 本栈无从解析也**不该**报错——c# 与 python 本轮同策走按名注册
/// （python `extensions.py::register_custom`／c# `ServiceContext.CustomHandlers`），
/// 集成方把 `clazz` 原样串当注册名即可，同一份流程 JSON 不用为 Rust 改。
///
/// 注册入口：[`crate::context::ServiceContext::register_custom_handler`]。
///
/// 返回值形状：
/// - `Ok(Some(v))` ⇒ 引擎按节点 `val`（缺省 [`CUSTOM_RETURN_VAL`]）把 `v` 写进流程变量；
/// - `Ok(None)` ⇒ 不写（对齐 java 的 `IHandler` 那一支：处理器自己往 `execution.args` 里塞）；
/// - `Err(..)` ⇒ **处理器自身执行失败**，属业务错误，照旧外抛打断本次执行
///   （spec/02 §6.2 第 2 条末句明写这一档不在豁免内）；
///   panic 同样外抛（本栈不套 `catch_unwind`——那会把业务错误悄悄降级成"跳过处理器"）。
///
/// 节点属性 `methodName` / `args` 由处理器自己从 `execution.current_node` 读
/// （它们在 java 侧是反射入参形状，本栈按名注册后只剩"节点配置"的含义）。
pub trait CustomNodeHandler: Send + Sync {
    fn handle(&self, execution: &mut crate::engine::Execution) -> JeeflowResult<Option<JsonValue>>;
}

// ═══════════════════════════════════════════════════════
// ProcessEventListener — concepts/04 §4.1
// ═══════════════════════════════════════════════════════

pub trait ProcessEventListener: Send + Sync {
    fn on_event(&self, event: &crate::event::ProcessEvent);
}
