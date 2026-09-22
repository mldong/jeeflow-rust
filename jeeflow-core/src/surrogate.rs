//! 委托代理**运行期自动生效**（issues/116 批次 D）——引擎内置、默认开启、可显式关闭。
//!
//! 契约依据：`docs/spec/06-facade.md` §4.5「运行期语义」条款 1~6 + `05-spi.md`
//! 「SurrogateInterceptor（委托生效，内置实现）」。
//!
//! 此前本栈只有 `processSurrogate/*` 五个台账 action，`get_surrogate` 的唯一调用方是
//! `memory.rs` 的测试（"有仓储无运行期"）。用户配好"休假期间张三替我批"后，
//! 保存成功、列表看得到，单子来了仍只发张三本人。本模块把它变成引擎能力：
//!
//! 1. **时机**（条款 1）：任务参与者解析完成后、参与者落库前。挂点是引擎的
//!    **新任务落库唯一收口** [`crate::engine::JeeflowEngineImpl::persist_tasks`]
//!    ——发起 / 办理推进 / **串行会签每一步推进** / 跳转四条路径全走它，
//!    只挂"发起"一处会漏掉流转中产生的新单。
//! 2. **动作**（条款 2 ⚠️）：把被委托人**并入参与者集合本身**（`task.actor_ids`），
//!    随后随任务一起落库。**不走**"事后再调一次 `add_task_actor` 补写"——Java 首版
//!    `SurrogateInterceptor` 正是走补写路，而它在 taskId 分配前触发，补写打在空 id 上
//!    **静默无效**（能力看起来实现了，实际一单都没代理出去）。本栈 `persist_tasks` 里
//!    actor 落库虽在 `save_task` 之后（id 已分配），但同样只改集合、不额外补写，
//!    由同一次收口把"原人 + 代理人"整体写入 `wf_process_task_actor`。
//!    授权人保留，任一可办（委托不是转办，不摘原人）。
//! 3. **不级联**（条款 1.2）：先取原始参与者**快照**再遍历，代理人自身的委托不展开
//!    （A→B 且 B→C 时 C 不因此收到该单，天然免疫环状委托死循环）。
//! 4. **默认开启、可显式关闭**（条款 3）：关闭一条 API
//!    ——`ServiceContext::with_surrogate_auto_apply(false)`，关闭后回到"仅台账"行为。
//!    等价姿势：装配一个 `get_surrogate` 恒返回 `None` 的扩展仓储。
//! 5. **未配置扩展仓储时静默跳过**（条款 4）：`ctx.ext_repository` 为 `None` 直接返回，
//!    不得抛"未配置扩展仓储"打断建单；仓储自身报错（表未建等）同样只记 stderr 后跳过。
//!
//! 另含**委托查询四判据**（条款 5）的本栈共用谓词 [`surrogate_hit`] /
//! [`surrogate_effective`] / [`newest_in_scope`] / [`normalize_time_text`] / [`pick_surrogate`]
//! ——内存仓（`memory.rs`）与 sqlx 仓对同一份数据必须给出同一结论（条款 6），
//! 两条路径共用一份判据即本模块存在的另一半理由：
//!
//! - 判据① 空 `processName` 全流程兜底：作用域分两腿——精确流程名一腿、
//!   `process_name IS NULL OR process_name = ''` 一腿；
//! - 判据② 时间窗 `start_time <= now <= end_time`，边界为 NULL/空/不可解析 → 该侧不限；
//! - 判据③ 自委托过滤 `surrogate <> operator`；
//! - 判据④ `enabled` 只有 1 生效，脏值/NULL 不得当启用。
//!
//! ⚠️ **判序**（条款 1.4 的正确形状，issues/123）：**每个作用域各自先按主键 id 取「最新一条」**
//! （[`newest_in_scope`]，不带任何生效判据过滤），**再由四判据裁决这一条**
//! （[`surrogate_effective`]）。反过来写——先按 enabled/窗口/自委托把候选滤掉、剩下的才取最新——
//! 等价于「同一授权人历史上只要留过一条窗内且 enabled=1 的记录，之后新建的窗外/停用/脏值/
//! 自委托记录全都判不动它」⇒ 代理人被**永久并入**，用户随后改设置一律不算数。
//! 精确作用域那条判否后**仍要看全流程作用域的最新一条**（不得判否即止）。
//!
//! 修复前本栈的欠账（issues/116 §5）：sqlx 仓缺判据①兜底与判据③；内存仓形参 `_time`
//! 直接忽略判据②、无判据③，且多条命中按 `HashMap` 随机序取"遍历首条"（违条款 1.4）。

use crate::context::ServiceContext;
use crate::model::{current_time_str, ProcessSurrogate, ProcessTask, TaskState};
use crate::spi::ProcessExtRepository;

/// 时间文本归一为可直接比大小的 `yyyy-MM-dd HH:mm:ss`（19 字符定长，字典序 == 时间序）。
///
/// 接受形态：`yyyy-MM-dd HH:mm:ss`（契约格式）、`yyyy-MM-ddTHH:mm:ss`（ISO，前端偶发）、
/// 带毫秒 `.fff`（sqlx DATETIME(3) 回读）、以及纯日期 `yyyy-MM-dd`（按 MySQL 语义补
/// `00:00:00`）。空串 / 形态不符 → `None`，调用方按「该侧不限」处理
/// （对齐契约"start_time/end_time 为 NULL 表示该侧不限"与 Go 的 `at.IsZero()` 姿势）。
///
/// 本栈 `jeeflow-core` 零第三方依赖（不引 chrono），故手写归一而非解析日历。
pub fn normalize_time_text(s: &str) -> Option<String> {
    let t = s.trim();
    let b = t.as_bytes();
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let digits = |r: &[u8]| r.iter().all(u8::is_ascii_digit);
    if !digits(&b[0..4]) || !digits(&b[5..7]) || !digits(&b[8..10]) {
        return None;
    }
    let date = &t[0..10];
    if b.len() == 10 {
        return Some(format!("{} 00:00:00", date));
    }
    if b[10] != b' ' && b[10] != b'T' {
        return None;
    }
    let time = &t[11..];
    let tb = time.as_bytes();
    if tb.len() < 5 || tb[2] != b':' || !digits(&tb[0..2]) || !digits(&tb[3..5]) {
        return None;
    }
    let hms = &time[0..5];
    // 秒：有则取（截掉毫秒/时区），无则按 00
    let sec = if tb.len() >= 8 && tb[5] == b':' && digits(&tb[6..8]) {
        &time[6..8]
    } else {
        "00"
    };
    Some(format!("{} {}:{}", date, hms, sec))
}

/// 委托是否命中「四判据」（条款 5）+ 判据① 的作用域归属。
///
/// 单条裁决口径见 [`surrogate_effective`]（四判据本体）与 [`surrogate_in_scope`]（作用域腿）。
/// ⚠️ 调用方**不得**把本谓词当作"候选过滤器"再从中取最新——那是 issues/123 点名的错形状；
/// 正确顺序是 [`newest_in_scope`] 先取该作用域的最新一条，再用本谓词裁决那一条。
///
/// - `operator`：待查授权人；
/// - `process_name`：当前流程名（判据①精确腿）；
/// - `all_flows`：`true` 表示这是**全流程兜底腿**，只认 `process_name` 为空的委托；
/// - `time`：判定时刻（`yyyy-MM-dd HH:mm:ss`）；空串 = 不判时间窗（判据②）。
///
/// ⚠️ `operator` / `process_name` 一律**原值相等比较**（不 trim、不大小写折叠）——
/// sqlx 侧 `operator = ?` / `process_name = ?` 就是这个语义，内存仓多算一步 trim
/// 就会双仓分叉（条款 6）。
pub fn surrogate_hit(
    sg: &ProcessSurrogate,
    operator: &str,
    process_name: &str,
    all_flows: bool,
    time: &str,
) -> bool {
    sg.operator == operator
        && surrogate_in_scope(sg, process_name, all_flows)
        && surrogate_effective(sg, operator, time)
}

/// 判据① 的作用域归属：精确腿认 `process_name == 当前流程名`；
/// 兜底腿只认 `process_name` 为空（库里 NULL 已由 sqlx 侧 `map_surrogate` 归一成 ""）。
pub fn surrogate_in_scope(sg: &ProcessSurrogate, process_name: &str, all_flows: bool) -> bool {
    if all_flows {
        sg.process_name.is_empty()
    } else {
        sg.process_name == process_name
    }
}

/// 四判据（条款 5 + issues/123 §1）的**单条裁决**：本条委托此刻对该授权人是否生效。
///
/// ① `enabled` **严格等于整数 1**（0 / 2 等脏值 / NULL 折成的 0 都算不生效）；
/// ② 被委托人非空且不等于授权人本人（自委托不新增、不重复）；
/// ③ `time` 不可解析（空串）⇒ 不比窗；否则 `start_time` 缺失 = 下界不限、
///    `end_time` 缺失 = 上界不限，需 `start <= time <= end`。
///
/// ⚠️ 本方法只裁决**单条**，不做多条择优——调用方必须先按 id 选出「最新一条」再问它
/// （规范 06 §4.5 条款 1.4，[`newest_in_scope`]）。SQL 仓与内存仓必须走同一份判据
/// （条款 6 要求双仓同答案）。
pub fn surrogate_effective(sg: &ProcessSurrogate, operator: &str, time: &str) -> bool {
    // 判据④：只有 1 生效（脏值/0/其它整数都算停用）
    if sg.enabled != 1 {
        return false;
    }
    // 判据③：自委托过滤（自己委托给自己不生效；空代理人同判否——SQL 侧 `surrogate <> operator`
    // 对 NULL 求值为 UNKNOWN，也不参与命中）
    if sg.surrogate.is_empty() || sg.surrogate == operator {
        return false;
    }
    // 判据②：时间窗，边界缺失/不可解析 → 该侧不限
    if let Some(now) = normalize_time_text(time) {
        if let Some(start) = sg.start_time.as_deref().and_then(normalize_time_text) {
            if start > now {
                return false;
            }
        }
        if let Some(end) = sg.end_time.as_deref().and_then(normalize_time_text) {
            if end < now {
                return false;
            }
        }
    }
    true
}

/// 条款 1.4（issues/123 修正后的判序）：在**一个作用域内**按主键 id 取**最新一条**，
/// 只排序取首行，**不带任何生效判据过滤**（enabled / 时间窗 / 自委托都不参与择优）。
///
/// 内存仓底层是 `HashMap`，遍历序随机——不显式取最大就会出现"同一份数据两次调用返回
/// 不同行"，且与 SQL 侧 `ORDER BY id DESC LIMIT 1` 分叉。
pub fn newest_in_scope<'a, I>(
    candidates: I,
    operator: &str,
    process_name: &str,
    all_flows: bool,
) -> Option<ProcessSurrogate>
where
    I: Iterator<Item = &'a ProcessSurrogate>,
{
    candidates
        .filter(|sg| sg.operator == operator)
        .filter(|sg| surrogate_in_scope(sg, process_name, all_flows))
        .max_by_key(|sg| sg.id)
        .cloned()
}

/// 委托查询主漏斗（规范 06 §4.5 条款 1.4 + issues/123）：
/// **每个作用域各自先按 id 取最新一条，再由四判据裁决这一条**。
///
/// 精确作用域那条记录判否（或该作用域压根没有记录）时，**仍要看全流程作用域的最新一条**——
/// 不得判否即止（Java 参考实现 `JdbcProcessExtRepositoryTest#testSurrogateCrudAndGet`
/// 钉的正是"精确已过期 → 兜底全流程委托"）。
pub fn pick_surrogate<'a, I>(
    candidates: I,
    operator: &str,
    process_name: &str,
    time: &str,
) -> Option<ProcessSurrogate>
where
    I: Iterator<Item = &'a ProcessSurrogate> + Clone,
{
    // 判据① 精确腿：该流程名作用域内的最新一条交裁决
    if !process_name.is_empty() {
        if let Some(newest) = newest_in_scope(candidates.clone(), operator, process_name, false) {
            if surrogate_hit(&newest, operator, process_name, false, time) {
                return Some(newest);
            }
        }
    }
    // 全流程委托腿（process_name 为空）：同样只认自己作用域里的最新一条
    let global = newest_in_scope(candidates, operator, process_name, true)?;
    surrogate_hit(&global, operator, process_name, true, time).then_some(global)
}

/// 对**参与者快照**逐个查生效委托，返回并入被委托人后的集合（顺序稳定：原人原序在前，
/// 代理人按命中序追加在后）。
///
/// - 快照遍历（`actors.to_vec()`）→ 本轮追加的代理人不再触发查询，即条款 1.2 不级联；
/// - 去重：代理人已在集合里不重复追加（幂等，可安全多次调用）；
/// - 单个参与者查询报错只记 stderr 后跳过，**不打断建单**（条款 4）。
pub fn expand_actors(
    ext: &dyn ProcessExtRepository,
    actors: &[String],
    process_name: &str,
    time: &str,
) -> Vec<String> {
    let snapshot = actors.to_vec();
    let mut out = snapshot.clone();
    for actor in snapshot {
        let hit = match ext.get_surrogate(&actor, process_name, time) {
            Ok(h) => h,
            Err(e) => {
                eprintln!(
                    "[jeeflow] 委托查询失败 actor={} process={} err={}（跳过该参与者，不中断建单）",
                    actor,
                    process_name,
                    e.message()
                );
                continue;
            }
        };
        let Some(hit) = hit else { continue };
        let agent = hit.surrogate;
        if agent.is_empty() || agent == actor || out.iter().any(|a| a == &agent) {
            continue;
        }
        out.push(agent);
    }
    out
}

/// 引擎收口处调用：任务落库前把生效中的被委托人**并入 `task.actor_ids`**。
///
/// 返回是否真的追加了人（供调用方/测试观测）。以下情形一律原样返回 `false`，
/// 不打断建单（条款 3/4）：
/// - 开关关闭（`with_surrogate_auto_apply(false)`）；
/// - 未配置扩展仓储（`ctx.ext_repository == None`）；
/// - 任务不是进行中（只对新建待办生效，不追溯历史行）；
/// - 参与者为空。
pub fn apply_surrogate_to_task(
    ctx: &ServiceContext,
    task: &mut ProcessTask,
    process_name: &str,
) -> bool {
    if !ctx.surrogate_auto_apply {
        return false;
    }
    let Some(ext) = ctx.ext_repository.as_ref() else {
        return false;
    };
    if task.task_state != TaskState::Doing.code() || task.actor_ids.is_empty() {
        return false;
    }
    let merged = expand_actors(ext.as_ref(), &task.actor_ids, process_name, &current_time_str());
    if merged.len() == task.actor_ids.len() {
        return false;
    }
    task.actor_ids = merged;
    true
}

// ═══════════════════════════════════════════════════════
// 双仓对拍共用「数据 + 期望」（契约 06 §4.5 条款 6）
// ═══════════════════════════════════════════════════════

/// 内存仓（`memory.rs` 测试）与 sqlx 真机仓（`jeeflow-repository-sqlx` 测试）跑
/// **同一张判据矩阵**——两侧各写一套断言迟早漂移（Go 栈同思路建了 `internal/surrparity`）。
///
/// 覆盖四判据（条款 5）+ "每作用域取最新一条再裁决"（条款 1.4，issues/123）。
/// 每行只服务一个判据，并用**不同授权人**隔离"全流程兜底腿"的串扰：
/// 只有 `zhangsan` / `lisi` 配了全流程委托，其余授权人的负例不会被兜底腿"救回来"。
/// id 段 911101~911114 是 issues/116 的原始矩阵；911121 起是 issues/123 补的
/// 「同一作用域内新旧两条并存、最新那条无效」矩阵。
pub mod parity {
    use crate::model::ProcessSurrogate;

    /// 判定基准时刻（所有时间窗都相对它构造）。
    pub const NOW: &str = "2026-09-21 12:00:00";

    /// 委托台账测试行（`None` 一律表示**库里该列是 NULL**，不是空串）。
    #[derive(Debug, Clone, Copy)]
    pub struct SgRow {
        pub id: i64,
        /// `None` = `process_name IS NULL`；`Some("")` = 空串。判据①要求两者同属
        /// "全部流程"委托，内存仓（只有 String，NULL 归一成 ""）与 SQL 仓必须同答案。
        pub process_name: Option<&'static str>,
        pub operator: &'static str,
        pub surrogate: &'static str,
        pub start_time: Option<&'static str>,
        pub end_time: Option<&'static str>,
        /// `None` = `enabled IS NULL`（判据④：不得当启用）。
        pub enabled: Option<i32>,
        pub note: &'static str,
    }

    /// 一次查询的期望结论。
    #[derive(Debug, Clone, Copy)]
    pub struct Expect {
        pub operator: &'static str,
        pub process_name: &'static str,
        /// 判定时刻：`""` = 不判时间窗（与 SQL 侧 `time.is_empty()` / 内存侧
        /// `normalize_time_text(None)` 同语义）。
        pub time: &'static str,
        /// 期望命中的委托 id；`None` = 不得命中。
        pub hit_id: Option<i64>,
        pub note: &'static str,
    }

    pub const ROWS: &[SgRow] = &[
        SgRow { id: 911101, process_name: Some("leave"),   operator: "zhangsan", surrogate: "agent_old101",     start_time: None, end_time: None, enabled: Some(1), note: "leave 委托（旧）" },
        SgRow { id: 911102, process_name: Some("leave"),   operator: "zhangsan", surrogate: "agent_new102",     start_time: None, end_time: None, enabled: Some(1), note: "leave 委托（新，条款 1.4 应取它）" },
        SgRow { id: 911103, process_name: Some("expense"), operator: "zhangsan", surrogate: "agent_exp103",     start_time: None, end_time: None, enabled: Some(1), note: "另一流程名精确行" },
        SgRow { id: 911104, process_name: None,            operator: "zhangsan", surrogate: "agent_allnull104", start_time: None, end_time: None, enabled: Some(1), note: "全流程委托（库里 NULL）" },
        SgRow { id: 911105, process_name: Some(""),        operator: "lisi",     surrogate: "agent_empty105",   start_time: None, end_time: None, enabled: Some(1), note: "全流程委托（库里空串）" },
        SgRow { id: 911106, process_name: Some("inwin"),    operator: "sunwu",  surrogate: "agent_in106",  start_time: Some("2026-09-01 00:00:00"), end_time: Some("2026-09-30 23:59:59"), enabled: Some(1), note: "窗内" },
        SgRow { id: 911107, process_name: Some("pastwin"),  operator: "sunwu",  surrogate: "agent_past107", start_time: Some("2020-01-01 00:00:00"), end_time: Some("2020-12-31 23:59:59"), enabled: Some(1), note: "窗外（已过期）" },
        SgRow { id: 911108, process_name: Some("futwin"),   operator: "sunwu",  surrogate: "agent_fut108",  start_time: Some("2030-01-01 00:00:00"), end_time: Some("2030-12-31 23:59:59"), enabled: Some(1), note: "窗外（未开始）" },
        SgRow { id: 911109, process_name: Some("halfopen"), operator: "sunwu",  surrogate: "agent_half109", start_time: None, end_time: Some("2026-09-30 23:59:59"), enabled: Some(1), note: "start_time NULL = 起不限" },
        SgRow { id: 911110, process_name: Some("halfopn2"), operator: "sunwu",  surrogate: "agent_half110", start_time: Some("2026-09-01 00:00:00"), end_time: None, enabled: Some(1), note: "end_time NULL = 止不限" },
        SgRow { id: 911111, process_name: Some("off"),      operator: "zhouqi", surrogate: "agent_off111",   start_time: None, end_time: None, enabled: Some(0),    note: "enabled=0 停用" },
        SgRow { id: 911112, process_name: Some("off"),      operator: "zhouqi", surrogate: "agent_null112",  start_time: None, end_time: None, enabled: None,      note: "enabled 为 NULL，不得当启用" },
        SgRow { id: 911113, process_name: Some("selfdel"),  operator: "zhouqi", surrogate: "zhouqi",         start_time: None, end_time: None, enabled: Some(1),    note: "自己委托给自己" },
        SgRow { id: 911114, process_name: Some("leave"),    operator: "zhaoliu",surrogate: "agent_zl114",    start_time: None, end_time: None, enabled: Some(1),    note: "别人的 leave 委托" },
        // ── issues/123：同一作用域内「旧的有效 + 新的无效」两条并存，必须由**最新一条**裁决 ──
        // 每格都先落一条窗内 enabled=1 的旧行，再落一条 id 更大的无效行（四种无效形状各一格）。
        // 判据①腿按 operator 隔离：这四位授权人都**没有**全流程委托行，兜底腿救不回来。
        SgRow { id: 911121, process_name: Some("m123"), operator: "n9out",   surrogate: "agent_ok121",     start_time: Some("2026-09-01 00:00:00"), end_time: Some("2026-09-30 23:59:59"), enabled: Some(1), note: "123 旧行：窗内 + enabled=1" },
        SgRow { id: 911122, process_name: Some("m123"), operator: "n9out",   surrogate: "agent_fut122",    start_time: Some("2030-01-01 00:00:00"), end_time: Some("2030-12-31 23:59:59"), enabled: Some(1), note: "123 新行：窗外（未开始）→ 由它裁决" },
        SgRow { id: 911123, process_name: Some("m123"), operator: "n9off",   surrogate: "agent_ok123",     start_time: Some("2026-09-01 00:00:00"), end_time: Some("2026-09-30 23:59:59"), enabled: Some(1), note: "123 旧行：窗内 + enabled=1" },
        SgRow { id: 911124, process_name: Some("m123"), operator: "n9off",   surrogate: "agent_off124",    start_time: Some("2026-09-01 00:00:00"), end_time: Some("2026-09-30 23:59:59"), enabled: Some(0), note: "123 新行：enabled=0" },
        SgRow { id: 911125, process_name: Some("m123"), operator: "n9dirty", surrogate: "agent_ok125",     start_time: Some("2026-09-01 00:00:00"), end_time: Some("2026-09-30 23:59:59"), enabled: Some(1), note: "123 旧行：窗内 + enabled=1" },
        SgRow { id: 911126, process_name: Some("m123"), operator: "n9dirty", surrogate: "agent_dirty126",  start_time: Some("2026-09-01 00:00:00"), end_time: Some("2026-09-30 23:59:59"), enabled: Some(2), note: "123 新行：enabled 脏值 2（只认 1）" },
        SgRow { id: 911127, process_name: Some("m123"), operator: "n9self",  surrogate: "agent_ok127",     start_time: Some("2026-09-01 00:00:00"), end_time: Some("2026-09-30 23:59:59"), enabled: Some(1), note: "123 旧行：窗内 + enabled=1" },
        SgRow { id: 911128, process_name: Some("m123"), operator: "n9self",  surrogate: "n9self",          start_time: Some("2026-09-01 00:00:00"), end_time: Some("2026-09-30 23:59:59"), enabled: Some(1), note: "123 新行：自委托" },
        // B 格（防"改成恒不并入"）：作用域内只有一条窗内 enabled=1 ⇒ 必须命中
        SgRow { id: 911129, process_name: Some("m123"), operator: "onlyvalid", surrogate: "agent_only129", start_time: Some("2026-09-01 00:00:00"), end_time: Some("2026-09-30 23:59:59"), enabled: Some(1), note: "123 B 格：唯一一条有效委托" },
        // 全流程腿同样要"先取最新再裁决"：旧的有效 + 新的停用两条都在空 process_name 作用域里
        SgRow { id: 911130, process_name: None,         operator: "g9new",   surrogate: "agent_gok130",    start_time: None, end_time: None, enabled: Some(1), note: "123 旧行：全流程作用域内有效" },
        SgRow { id: 911131, process_name: Some(""),     operator: "g9new",   surrogate: "agent_goff131",   start_time: None, end_time: None, enabled: Some(0), note: "123 新行：全流程作用域内最新那条 enabled=0（库里空串）" },
    ];

    pub const EXPECT: &[Expect] = &[
        Expect { operator: "zhangsan", process_name: "leave",   time: NOW, hit_id: Some(911102), note: "多条命中取 id 最大（条款 1.4，不得取遍历首条）" },
        Expect { operator: "zhangsan", process_name: "expense", time: NOW, hit_id: Some(911103), note: "精确腿另一流程名命中自身" },
        Expect { operator: "zhangsan", process_name: "nosuch",  time: NOW, hit_id: Some(911104), note: "精确腿未命中 → 全流程兜底（库里 NULL）" },
        Expect { operator: "lisi",     process_name: "nosuch",  time: NOW, hit_id: Some(911105), note: "全流程兜底（库里空串）" },
        Expect { operator: "lisi",     process_name: "leave",   time: NOW, hit_id: Some(911105), note: "本人无 leave 精确行 → 走兜底" },
        Expect { operator: "zhaoliu",  process_name: "leave",   time: NOW, hit_id: Some(911114), note: "只命中本人委托" },
        Expect { operator: "unknown",  process_name: "leave",   time: NOW, hit_id: None,         note: "无委托的授权人不得命中任何行（含兜底腿）" },
        Expect { operator: "sunwu",    process_name: "inwin",    time: NOW, hit_id: Some(911106), note: "判据② 窗内命中" },
        Expect { operator: "sunwu",    process_name: "pastwin",  time: NOW, hit_id: None,         note: "判据② 窗外（已过期）不得命中" },
        Expect { operator: "sunwu",    process_name: "futwin",   time: NOW, hit_id: None,         note: "判据② 窗外（未开始）不得命中" },
        Expect { operator: "sunwu",    process_name: "halfopen", time: NOW, hit_id: Some(911109), note: "判据② start_time NULL = 该侧不限" },
        Expect { operator: "sunwu",    process_name: "halfopn2", time: NOW, hit_id: Some(911110), note: "判据② end_time NULL = 该侧不限" },
        Expect { operator: "sunwu",    process_name: "pastwin",  time: "",  hit_id: Some(911107), note: "time 传空串 = 不判窗（两仓同语义）" },
        Expect { operator: "zhouqi",   process_name: "off",      time: NOW, hit_id: None,         note: "判据④ enabled=0 / NULL 均不得当启用" },
        Expect { operator: "zhouqi",   process_name: "selfdel",  time: NOW, hit_id: None,         note: "判据③ 自委托过滤 surrogate <> operator" },
        // ── issues/123：同一作用域内「旧有效 + 新无效」⇒ 判否，且旧的那条**不得复活** ──
        Expect { operator: "n9out",    process_name: "m123", time: NOW, hit_id: None,         note: "123 最新一条窗外 ⇒ 不命中，也不得回落到旧的窗内行" },
        Expect { operator: "n9off",    process_name: "m123", time: NOW, hit_id: None,         note: "123 最新一条 enabled=0 ⇒ 不命中，旧的窗内行不得复活" },
        Expect { operator: "n9dirty",  process_name: "m123", time: NOW, hit_id: None,         note: "123 最新一条 enabled 脏值 2 ⇒ 不命中，旧的窗内行不得复活" },
        Expect { operator: "n9self",   process_name: "m123", time: NOW, hit_id: None,         note: "123 最新一条自委托 ⇒ 不命中，旧的窗内行不得复活" },
        Expect { operator: "onlyvalid",process_name: "m123", time: NOW, hit_id: Some(911129), note: "123 B 格：作用域内只有一条有效委托 ⇒ 必须命中（防写成恒不并）" },
        Expect { operator: "g9new",    process_name: "m123", time: NOW, hit_id: None,         note: "123 全流程腿同样按最新一条裁决（新那条 enabled=0，旧的有效行不得复活）" },
        Expect { operator: "g9new",    process_name: "m123", time: "",  hit_id: None,         note: "123 time 传空串也不救：判否来自 enabled=0，与窗口无关" },
    ];

    /// 装载到内存仓（`process_name`/`enabled` 的 NULL 归一为 ""/0，判据结论不变）。
    pub fn memory_row(row: &SgRow) -> ProcessSurrogate {
        ProcessSurrogate {
            id: row.id,
            process_name: row.process_name.unwrap_or_default().to_string(),
            operator: row.operator.to_string(),
            surrogate: row.surrogate.to_string(),
            start_time: row.start_time.map(str::to_string),
            end_time: row.end_time.map(str::to_string),
            enabled: row.enabled.unwrap_or(0),
            create_time: None,
            create_user: Some("parity_seed".to_string()),
            update_time: None,
            update_user: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sg(id: i64, operator: &str, process_name: &str, agent: &str, enabled: i32) -> ProcessSurrogate {
        ProcessSurrogate {
            id,
            process_name: process_name.into(),
            operator: operator.into(),
            surrogate: agent.into(),
            start_time: None,
            end_time: None,
            enabled,
            create_time: None,
            create_user: None,
            update_time: None,
            update_user: None,
        }
    }

    #[test]
    fn test_normalize_time_text_forms() {
        // 契约格式原样
        assert_eq!(
            normalize_time_text("2026-09-21 08:20:54").as_deref(),
            Some("2026-09-21 08:20:54")
        );
        // ISO T 形态归一为空格
        assert_eq!(
            normalize_time_text("2026-09-21T08:20:54").as_deref(),
            Some("2026-09-21 08:20:54")
        );
        // 毫秒截断（sqlx DATETIME(3) 回读可能带 .fff）
        assert_eq!(
            normalize_time_text("2026-09-21 08:20:54.123").as_deref(),
            Some("2026-09-21 08:20:54")
        );
        // 纯日期按 MySQL 语义补 00:00:00
        assert_eq!(
            normalize_time_text("2026-09-21").as_deref(),
            Some("2026-09-21 00:00:00")
        );
        // 不可解析 → None（该侧不限）
        for bad in ["", "   ", "abc", "2026/09/21", "2026-09-21T8:20:54", "20260921"] {
            assert_eq!(normalize_time_text(bad), None, "坏值 {:?} 应归 None", bad);
        }
        // 归一后可比大小（字典序 == 时间序）
        let a = normalize_time_text("2026-09-21 08:20:54").unwrap();
        let b = normalize_time_text("2026-09-21T08:20:55").unwrap();
        assert!(a < b);
    }

    #[test]
    fn test_surrogate_hit_four_predicates() {
        let now = "2026-09-21 12:00:00";
        // 判据④ enabled 只有 1 生效
        assert!(!surrogate_hit(&sg(1, "zs", "leave", "ls", 0), "zs", "leave", false, now));
        assert!(!surrogate_hit(&sg(1, "zs", "leave", "ls", 2), "zs", "leave", false, now));
        assert!(surrogate_hit(&sg(1, "zs", "leave", "ls", 1), "zs", "leave", false, now));
        // 判据③ 自委托过滤
        assert!(!surrogate_hit(&sg(1, "zs", "leave", "zs", 1), "zs", "leave", false, now));
        // 判据① 精确腿不认全流程委托；兜底腿只认全流程委托
        assert!(!surrogate_hit(&sg(1, "zs", "", "ls", 1), "zs", "leave", false, now));
        assert!(surrogate_hit(&sg(1, "zs", "", "ls", 1), "zs", "", true, now));
        assert!(!surrogate_hit(&sg(1, "zs", "leave", "ls", 1), "zs", "", true, now));
        // 判据② 时间窗（NULL 侧不限）
        let mut in_window = sg(1, "zs", "leave", "ls", 1);
        in_window.start_time = Some("2026-09-01 00:00:00".into());
        in_window.end_time = Some("2026-09-30 23:59:59".into());
        assert!(surrogate_hit(&in_window, "zs", "leave", false, now));
        let mut before = sg(1, "zs", "leave", "ls", 1);
        before.start_time = Some("2026-10-01 00:00:00".into());
        assert!(!surrogate_hit(&before, "zs", "leave", false, now));
        let mut after = sg(1, "zs", "leave", "ls", 1);
        after.end_time = Some("2026-09-21T11:59:59".into());
        assert!(!surrogate_hit(&after, "zs", "leave", false, now));
        // time 为空串 → 不判窗（与 sqlx 侧 `if time.is_empty()` 同语义）
        assert!(surrogate_hit(&after, "zs", "leave", false, ""));
    }

    #[test]
    fn test_pick_surrogate_takes_max_id_and_falls_back() {
        // 多条命中 → 取 id 最大（条款 1.4，不得"取遍历首条"）
        let list = vec![
            sg(10, "zs", "leave", "old", 1),
            sg(30, "zs", "leave", "new", 1),
            sg(20, "zs", "leave", "mid", 1),
        ];
        let hit = pick_surrogate(list.iter(), "zs", "leave", "").unwrap();
        assert_eq!(hit.id, 30, "多条命中应取 id 最大");
        assert_eq!(hit.surrogate, "new");
        // 精确腿未命中 → 空 processName 兜底腿
        let mixed = vec![
            sg(1, "zs", "expense", "ls", 1),
            sg(2, "zs", "", "all", 1),
        ];
        let hit = pick_surrogate(mixed.iter(), "zs", "leave", "").unwrap();
        assert_eq!(hit.id, 2, "流程名未命中应回落到全流程委托");
        assert_eq!(hit.surrogate, "all");
        // 精确腿命中时不被兜底腿覆盖（兜底腿 id 更大也不行）
        let prefer = vec![sg(99, "zs", "", "all", 1), sg(1, "zs", "leave", "exact", 1)];
        let hit = pick_surrogate(prefer.iter(), "zs", "leave", "").unwrap();
        assert_eq!(hit.surrogate, "exact", "精确命中优先于全流程兜底");
        // 全不命中
        assert!(pick_surrogate(prefer.iter(), "ww", "leave", "").is_none());
    }

    /// A 格（issues/123）：同一 operator+processName 先建"窗内+enabled=1"，
    /// 再建一条更"新"的无效记录 ⇒ 最新一条说了算，**旧的有效行不得复活**。
    ///
    /// 把实现改回"先滤生效、再从剩下的取最新"（即修复前的形状）时，本用例四格全红；
    /// 而那正是 13 栈在 L2-17/L2-18 上恒并入的成因。
    #[test]
    fn test_pick_surrogate_newest_invalid_beats_older_valid() {
        // 旧行：窗内 + enabled=1（911101 段之外，另起局部 id 段，只在本用例内比大小）
        let mut older_valid = sg(100, "zs", "leave", "agent_old_valid", 1);
        older_valid.start_time = Some("2026-09-01 00:00:00".into());
        older_valid.end_time = Some("2026-09-30 23:59:59".into());
        let now = "2026-09-21 12:00:00";

        // 四种"最新一条不生效"的形状（可参数化，逐格断言）
        let newest_invalid: Vec<(ProcessSurrogate, &str)> = vec![
            {
                // ① 窗外：整扇窗在未来
                let mut s = sg(200, "zs", "leave", "agent_new_outwin", 1);
                s.start_time = Some("2030-01-01 00:00:00".into());
                s.end_time = Some("2030-12-31 23:59:59".into());
                (s, "最新一条窗外")
            },
            {
                // ② 窗外：窗已过期
                let mut s = sg(200, "zs", "leave", "agent_new_expired", 1);
                s.start_time = Some("2020-01-01 00:00:00".into());
                s.end_time = Some("2020-12-31 23:59:59".into());
                (s, "最新一条窗外（已过期）")
            },
            ({ sg(200, "zs", "leave", "agent_new_off", 0) }, "最新一条 enabled=0"),
            ({ sg(200, "zs", "leave", "agent_new_dirty", 2) }, "最新一条 enabled 脏值 2（契约：只认 1）"),
            ({ sg(200, "zs", "leave", "zs", 1) }, "最新一条自委托"),
        ];
        for (fresh, why) in newest_invalid {
            let list = vec![older_valid.clone(), fresh];
            let got = pick_surrogate(list.iter(), "zs", "leave", now).map(|s| s.id);
            assert_eq!(got, None, "{why} ⇒ 不得命中，更不得回落到旧的窗内有效行");
        }
    }

    /// B 格（issues/123）：作用域内**只有一条**"窗内 + enabled=1" ⇒ 必须命中。
    /// 防 A 格的修法被写成"恒不并入"。
    #[test]
    fn test_pick_surrogate_sole_valid_row_still_hits() {
        let now = "2026-09-21 12:00:00";
        let mut only = sg(7, "zs", "leave", "agent_only", 1);
        only.start_time = Some("2026-09-01 00:00:00".into());
        only.end_time = Some("2026-09-30 23:59:59".into());
        let list = vec![only];
        let hit = pick_surrogate(list.iter(), "zs", "leave", now).unwrap();
        assert_eq!(hit.id, 7, "唯一一条有效委托必须命中（精确腿）");
        assert_eq!(hit.surrogate, "agent_only");

        // 全流程腿同理：唯一一条空 processName 的有效委托也要命中
        let mut only_all = sg(8, "zs", "", "agent_all_only", 1);
        only_all.start_time = Some("2026-09-01 00:00:00".into());
        only_all.end_time = None;
        let list_all = vec![only_all];
        let hit = pick_surrogate(list_all.iter(), "zs", "leave", now).unwrap();
        assert_eq!(hit.id, 8, "唯一一条全流程有效委托必须命中（兜底腿）");
        assert_eq!(hit.surrogate, "agent_all_only");
    }

    /// 精确作用域最新一条判否后，**仍要看全流程作用域的最新一条**（不得判否即止）。
    /// 钉的是 Java `JdbcProcessExtRepositoryTest#testSurrogateCrudAndGet` 同一条形。
    #[test]
    fn test_pick_surrogate_exact_scope_invalid_still_checks_global_scope() {
        let now = "2026-09-21 12:00:00";
        let mut expired_exact = sg(50, "zs", "leave", "agent_expired", 1);
        expired_exact.start_time = Some("2020-01-01 00:00:00".into());
        expired_exact.end_time = Some("2020-12-31 23:59:59".into());
        let mut live_global = sg(40, "zs", "", "agent_global", 1);
        live_global.start_time = Some("2026-09-01 00:00:00".into());
        live_global.end_time = Some("2026-09-30 23:59:59".into());
        // 兜底行 id 更小也一样要接住：两腿各自取自己作用域里的最新一条
        let list = vec![expired_exact, live_global];
        let hit = pick_surrogate(list.iter(), "zs", "leave", now).unwrap();
        assert_eq!(hit.id, 40, "精确作用域判否后须落到全流程作用域");
        assert_eq!(hit.surrogate, "agent_global");
    }
}
