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
    /// audit8 R110：该定义下是否存在任何实例（含已办结/已撤回）。
    fn has_instances_by_define_id(&self, define_id: i64) -> JeeflowResult<bool>;

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
    /// 任务参与者删除腿（`processTask/removeTaskActor`／转办摘原人／任何摘人都落这一支）。
    ///
    /// **归属值删除腿义务**（issues/137 §3-6 · spec 06 §processTask/removeTaskActor 语义 6，
    /// owner 2026-10-02 拍「两形并集」）——⚠️ **与 [`Self::add_task_actor`] 的写侧义务不同，
    /// 别照抄**：写侧只取 trim 形落库；删除腿必须取「原值 ∪ trim 值」两形并集。实现方必须：
    /// ① **空值一律丢弃、不喂 `DELETE`**——空串/纯空白不得进匹配集合，否则历史 `actor_id=''`
    ///    脏行会被批量误删（那是替脏数据做掉唯一痕迹）；
    /// ② **非空值同时以「原值」与「trim 值」两形匹配**（保序、按字面去重）。只取 trim 形 ⇒
    ///    门面按语义 6 交出的**行上原值** `" 9101 "`（修复前落下的未 trim 历史脏行）被削成
    ///    `9101`，真库 NO PAD 排序规则下那一行删不掉而门面报成功（**假成功**：被摘的人待办
    ///    还在）；只取原值 ⇒ 第三方绕过门面直连仓储传 `" 8601 "` 时删不掉写侧归一后落库的
    ///    规范行 `8601`（issues/142 §9.2 那一路）。两形并集同时满足两侧——按 §2.11 归一口径
    ///    两行本就是同一个人，都删才是"摘掉这个人"的正确结果；
    /// ③ **并集为空 ⇒ 早退，一条 `DELETE` 都不发**——不得退化成"清空该任务全部参与者"。
    ///
    /// 判据本体只有一枚＝[`crate::model::actor_delete_forms`]（八栈同名件），trim 与判空规则
    /// 仍复用 [`crate::model::normalize_actors`] 那一枚，**不要在仓储里抄第二份**。判空一律
    /// `trim().is_empty()`：`"0"` 是合法 id 必须留下，`"0"` 与 `"00"` 是两个人。本仓两仓
    /// （内存 / sqlx）都按这一条实现，同一条判据给同一个答案（issues/117 场景 27）。
    /// `task_id` 仍是**主键**不是归属值，不参与并集展开（同 [`Self::add_task_actor`] 末段那一档）。
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
    /// issues/142 B 批 · spec 06-facade.md §2.11 表第四行（rust 漏收口随 issues/172 补齐）：入参
    /// `actor_id`（门面的 `operator`）**必须先归一再比**——取 trim 后的值；trim 后为空 ⇒
    /// 一行都不动（仓储层 no-op 档；门面层已把空白/缺键回落缺省操作人，两层都挡）。
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
    ///
    /// `'a`（R9/145-5）：闭包允许借用调用方栈上的参数——引擎入口的 operator/args 都是借用，
    /// 动作同步执行完即返回，闭包不逃逸本调用。**join 语义由实现方负责**：调用线程上已有
    /// 事务时并入外层（不重开 BEGIN、不提交）；空转/原子/归属三档判据见 spec/12 §12.4。
    fn execute_in_tx<'a>(
        &self,
        action: Box<dyn FnOnce() -> JeeflowResult<Box<dyn std::any::Any + Send>> + Send + 'a>,
    ) -> JeeflowResult<Box<dyn std::any::Any + Send>>;

    /// 提交后腿通道（audit8 R77 读法一，spec/12 §12.3.1／issues/234 §4.3）：
    /// 语义＝"确保这条腿在**本次事务提交之后**执行；当前线程没有活动事务就立即执行"。
    /// 回滚 ⇒ 整批丢弃。默认实现直接调用回调＝本实现没有这条通道 ⇒ 行为与上一版逐字不变
    /// （＝ C# 的 DIM 等价物，既有实现方零改动）。
    /// 契约下限＝"监听器看到的世界＝已提交"由**监听器侧**承担，不要求 fire 点搬家
    /// （参考实现 java＝事务内 fire＋监听器自取 afterCommit；php 已由 R25 收口同形）。
    fn after_commit(&self, cb: Box<dyn FnOnce() + Send>) {
        cb();
    }
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
    /// action → 权限码集合（**OR 语义**：任一持有即有权）。
    ///
    /// **空集合 = 放行**（登录即可，spec/06 §2.6；与 go 的 `nil`／java 的 `null` 同一档，
    /// 本栈签名是 `Vec<String>` 拿不到 `null`，故以"空＝放行"承载——集成方判据是
    /// `!codes.is_empty()` 才去校验）。
    fn permission_codes(&self, action: &str) -> Vec<String>;
}

/// issues/248（权限码表归引擎、八栈统一）rust 腿。
///
/// 语义权威＝jeeflow-java `com.mldong.jeeflow.spi.DefaultActionPermissionProvider`
/// （`OR_RULES` 7 条／`NO_PERM_ACTIONS` 11 条），本表与其**逐条一致**；go 腿的同款见
/// `jeeflow-go/spi/action_permission.go`。改这张表必须八栈同批改，否则跨栈读出两套码。
///
/// 为什么表放在引擎里而不是各集成壳自备：现读十壳各抄一份
/// （go 三壳 `wf_permission.go`／python 三壳 `wf_permission.py`／nestjs `wf-permission.ts`／
/// laravel `WfPermission.php`／salvo `wf_permission.rs`／csharp `WfEndpoints.cs`），
/// 新 action 或粒度调整要改十处＋引擎，"只用引擎、自写集成层"的客户还会读出两套码。
///
/// 用静态数组而不是 [`std::collections::HashMap`]：本 crate 零第三方依赖且表只有个位数条目，
/// 线性查表的成本可忽略，还能保住"表顺序＝java 侧书写顺序"这条可读性；不需要 `lazy_static`／
/// `OnceLock` 之类的额外依赖。
const OR_RULES: &[(&str, &[&str])] = &[
    // boot3 注解 OR 语义的 action（任选其一即有权）
    (
        "processDefine/detail",
        &["wf:processDefine:detail", "wf:processDesign:listByType"],
    ),
    (
        "processDefine/startAndExecute",
        &["wf:processDefine:startAndExecute", "wf:processDesign:listByType"],
    ),
    (
        "processDefine/getLastByName",
        &[
            "wf:processDefine:detail",
            "wf:processDesign:listByType",
            "wf:processDefine:getLastByName",
        ],
    ),
    (
        "processTask/candidatePage",
        &["wf:processTask:execute", "wf:processTask:candidatePage"],
    ),
    ("processTask/jumpAbleTaskNameList", &["wf:processTask:execute"]),
    // issues/246／247 两枚新 action 定 B 案（owner 2026-11-04）：**复用"办理"这枚码**，
    // 不另建 sys_menu 按钮种子。判据是既有惯例——transfer／removeTaskActor／candidatePage／latest
    // 都没有按钮行，candidatePage／jumpAbleTaskNameList 就是走本表复用 execute；而撤回／撤会签的
    // 主体按引擎判据本来就必然是该行参与者（＝有办理权的人），拆独立码实际给不出去。
    ("processTask/recall", &["wf:processTask:execute"]),
    ("processTask/revokeDynamicCountersign", &["wf:processTask:execute"]),
];

/// 放行清单（登录即可、不配权限码）——对齐 java `NO_PERM_ACTIONS`。
/// 统计三条是 issues/103 的收口成果（普通用户分析页曾被默认规则 `wf:processInstance:stats:*` 拦住）。
const NO_PERM_ACTIONS: &[&str] = &[
    "processInstance/detail",
    "processInstance/highLight",
    "processInstance/approvalRecord",
    "processInstance/getAssigneeTextData",
    "processInstance/bizData",
    "processTask/detail",
    "processTask/addCandidate",
    "processTask/latest",
    "processInstance/stats/overview",
    "processInstance/stats/trend",
    "processInstance/stats/group",
];

/// 引擎内置默认件：放行 → OR → 默认派生 `wf:{action 的 / 换 :}`。
///
/// java 的 `permissionCodes(null)` 返回 `null` 那一档在本栈**结构性不可达**
/// （入参是 `&str`，非空类型），无需照抄。
pub struct DefaultActionPermissionProvider;

impl ActionPermissionProvider for DefaultActionPermissionProvider {
    fn permission_codes(&self, action: &str) -> Vec<String> {
        if NO_PERM_ACTIONS.contains(&action) {
            return Vec::new(); // 放行：空集合＝登录即可
        }
        if let Some((_, codes)) = OR_RULES.iter().find(|(a, _)| *a == action) {
            return codes.iter().map(|c| c.to_string()).collect(); // OR 语义
        }
        vec![format!("wf:{}", action.replace('/', ":"))] // 默认派生
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

// ═══════════════════════════════════════════════════════
// 测试
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    /// 内置默认件的便捷出口（等价于宿主不注册时 `ServiceContext` 回落的那一件）。
    fn perm(action: &str) -> Vec<String> {
        DefaultActionPermissionProvider.permission_codes(action)
    }

    /// 期望值构造器：把 java 侧的 `String[]` 字面量形状一比一搬过来。
    fn want(codes: &[&str]) -> Vec<String> {
        codes.iter().map(|c| c.to_string()).collect()
    }

    /// 正向＋负向：默认派生 `wf:{action 的 / 换 :}`。
    /// 断言的是"与 java 逐条一致"，不是"与这份实现一致"——改这张表必须同时改 java，否则本卡必红。
    #[test]
    fn test_i248_default_derivation() {
        assert_eq!(perm("processDesign/save"), want(&["wf:processDesign:save"]));
        // 没配进表的 action 一律派生，不得被误放行（java `defaultRuleStillApplies` 同两枚）
        assert_eq!(perm("processDefine/deploy"), want(&["wf:processDefine:deploy"]));
        assert_eq!(
            perm("processInstance/updateCCStatus"),
            want(&["wf:processInstance:updateCCStatus"])
        );
        // 三段 action 的 '/' 是**全量**替换（java 的 `replace` 语义），不是只换第一枚
        assert_eq!(
            perm("processInstance/stats/notInList"),
            want(&["wf:processInstance:stats:notInList"])
        );
    }

    /// OR 族：表 7 条逐条钉住（顺序＝java `OR_RULES` 的书写顺序）。
    #[test]
    fn test_i248_or_rules_match_java() {
        assert_eq!(
            perm("processDefine/detail"),
            want(&["wf:processDefine:detail", "wf:processDesign:listByType"])
        );
        assert_eq!(
            perm("processDefine/startAndExecute"),
            want(&["wf:processDefine:startAndExecute", "wf:processDesign:listByType"])
        );
        assert_eq!(
            perm("processDefine/getLastByName"),
            want(&[
                "wf:processDefine:detail",
                "wf:processDesign:listByType",
                "wf:processDefine:getLastByName"
            ])
        );
        assert_eq!(
            perm("processTask/candidatePage"),
            want(&["wf:processTask:execute", "wf:processTask:candidatePage"])
        );
        assert_eq!(
            perm("processTask/jumpAbleTaskNameList"),
            want(&["wf:processTask:execute"])
        );
        assert_eq!(
            perm("processTask/recall"),
            want(&["wf:processTask:execute"])
        );
        assert_eq!(
            perm("processTask/revokeDynamicCountersign"),
            want(&["wf:processTask:execute"])
        );
    }

    /// 放行族：11 条全部返回**空集合**（空＝登录即可；含 issues/103 的统计三条）。
    #[test]
    fn test_i248_no_perm_actions_are_empty() {
        for action in NO_PERM_ACTIONS.iter().copied() {
            assert!(
                perm(action).is_empty(),
                "action {} 应放行（空集合＝登录即可），实得 {:?}",
                action,
                perm(action)
            );
        }
        // 统计三条单列（issues/103：普通用户分析页曾被默认规则 wf:processInstance:stats:* 拦住）
        for action in [
            "processInstance/stats/overview",
            "processInstance/stats/trend",
            "processInstance/stats/group",
        ] {
            assert!(perm(action).is_empty(), "统计 action {} 应放行", action);
        }
    }

    /// issues/246／247 两枚新 action 的 B 案粒度（owner 2026-11-04 拍）：**恰为** execute 一枚，
    /// 不派生成 `wf:processTask:recall`／`wf:processTask:revokeDynamicCountersign`（那是 A 案，
    /// 普通角色会因为没 `sys_menu` 种子行而 403）。
    ///
    /// 反向哨兵同钉在这里：没进两张表的 action 必须仍走默认派生——否则"整族都被塞进 execute"
    /// 这类错法会被本测试放过。
    #[test]
    fn test_i248_new_actions_ride_execute_code() {
        for action in ["processTask/recall", "processTask/revokeDynamicCountersign"] {
            assert_eq!(
                perm(action),
                want(&["wf:processTask:execute"]),
                "{} 须复用「办理」这枚码",
                action
            );
        }
        assert_eq!(perm("processTask/transfer"), want(&["wf:processTask:transfer"]));
        assert_eq!(
            perm("processTask/removeTaskActor"),
            want(&["wf:processTask:removeTaskActor"])
        );
    }

    /// 形状锚：两张表的条目数与 java 钉死（`OR_RULES` 7 条／`NO_PERM_ACTIONS` 11 条）。
    /// 这不是"会漂的读数"，而是跨栈契约的形状——加一条规则要八栈同批改。
    #[test]
    fn test_i248_table_shapes_match_java() {
        assert_eq!(OR_RULES.len(), 7, "OR 规则条数应为 7（对齐 java OR_RULES）");
        assert_eq!(
            NO_PERM_ACTIONS.len(),
            11,
            "放行清单条数应为 11（含统计三条，对齐 java NO_PERM_ACTIONS）"
        );
        // 两表不得有交集，也不得有重复项（否则条目数会"凑对"而语义走偏）
        for (action, codes) in OR_RULES {
            assert!(
                !NO_PERM_ACTIONS.contains(action),
                "{} 同时出现在 OR 表与放行清单",
                action
            );
            assert!(!codes.is_empty(), "{} 的 OR 码不得为空", action);
        }
        let mut or_actions: Vec<&str> = OR_RULES.iter().map(|(a, _)| *a).collect();
        or_actions.sort_unstable();
        let mut no_perm_actions: Vec<&str> = NO_PERM_ACTIONS.to_vec();
        no_perm_actions.sort_unstable();
        assert_eq!(
            or_actions.windows(2).filter(|w| w[0] == w[1]).count(),
            0,
            "OR 表内 action 重复"
        );
        assert_eq!(
            no_perm_actions.windows(2).filter(|w| w[0] == w[1]).count(),
            0,
            "放行清单内 action 重复"
        );
    }
}
