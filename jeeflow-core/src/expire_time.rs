//! issues/126 案 A · 任务行 `expire_time`：到期表达式**求值器** + 建单写点共用的尺子。
//!
//! 参考实现并排：
//!   - Java 求值器 `FlowUtil.processTime(String, FlowData)`（jeeflow-java `util/FlowUtil.java:64`）
//!   - Java 写点 `ProcessInstance.applyExpireTime` / `applyNodeExpireTime`（commit `cb541d4`，**五处**）
//!   - 基准侧 boot2 内置版 `ProcessTaskServiceImpl` :213 普通建单 / :386 回退新建 / :524 会签建单
//!   - 同批已落地的姊妹栈：go `engine/expire_time.go`（`d10ebd0`）、php `Util/FlowUtil.php`（`b9bea0d`）
//!
//! 本栈修前形状：全仓**没有**到期求值器（普查见
//! `jeeflow-hub/docs/goal-126-到期时间七引擎-启动词.md` §1.5），只在 `start_async` 里把定义级
//! `model.expire_time` 原串搬到**实例**那一列（`engine.rs` 的 `if let Some(et) = &model.expire_time`），
//! 任务行五处写点一个都没接 ⇒ 常规流上 `wf_process_task.expire_time` 恒 NULL ⇒ 逾期统计恒 0
//! （跨栈判据＝门禁 L2-27）。实例级那一档按 §1.9-2 **不在本案范围**：只记录，不改。
//!
//! 与其余七栈最大的一处不同：**core 零依赖、没有 chrono**，时间一律是 `"yyyy-MM-dd HH:mm:ss"`
//! 墙钟串，取时只有一个出口 [`clock::current_time_str`]（issues/120 路 1 的成果）。本文件因此
//! 不出现 `SystemTime` / `std::time` / 时区库，全部算法在"墙钟串 ⇄ 伪 epoch 秒"之间做：
//! 伪 epoch 只用于求值，落库的仍是同格式的墙钟串，与 `create_time` 天然同基准可直接相减。

use crate::clock;
use crate::json::{FlowData, JsonValue};
use crate::model::ProcessTask;
use crate::parser::NodeModel;

/// 任务节点上配的到期表达式属性键
/// （对齐 Java `TaskParser` 的 `expireTime` ← 设计器 JSON 的 `properties.expireTime`）。
pub const PROP_EXPIRE_TIME: &str = "expireTime";

const SECS_PER_DAY: i64 = 86_400;
/// 绝对档格式长度：`yyyy-MM-dd HH:mm:ss` 定长 19 字符。
const DATE_TIME_LEN: usize = 19;

/// 读任务节点上配的到期表达式；**属性缺键 / 配成 JSON null ⇒ `None`**（＝节点没配）。
///
/// 刻意不走 `NodeModel::prop_str`：那条对 JSON null 与非字符串值一律给 `None`，
/// 会把"配了个非字符串表达式"和"没配"混成同一档（go 的 `expireExprOf` 同因）。
/// 非字符串值按 Java `FlowData.getStr` 的 `v.toString()` 同档处理。
pub fn expire_expr_of(node: &NodeModel) -> Option<String> {
    match node.properties.get(PROP_EXPIRE_TIME) {
        None | Some(JsonValue::Null) => None,
        Some(JsonValue::Str(s)) => Some(s.clone()),
        Some(other) => Some(scalar_to_text(other)),
    }
}

/// 非字符串标量 → 文本（对齐 Java `Object.toString()`；数组/对象给 JSON 串，最终落"解析不出"档）。
fn scalar_to_text(v: &JsonValue) -> String {
    match v {
        JsonValue::Str(s) => s.clone(),
        JsonValue::Number(n) if n.is_finite() && n.fract() == 0.0 => format!("{}", *n as i64),
        JsonValue::Bool(b) => b.to_string(),
        other => other.to_json_string(),
    }
}

/// 五处建单写点**共用**的赋值口（对齐 Java `ProcessInstance.applyExpireTime`）。
///
/// `expr` 为 `None` 或去空白后为空 ⇒ **一个字都不动**，这一列保持原样（新建行即保持 NULL；
/// 回退新建那条更要紧——boot2 的回退是"克隆历史行 + 仅在当前节点配了表达式时重算"，
/// 没配就沿用继承值，不许把已有值清空，也不许造 `now()` / `''` / `0` 这种占位）。
/// 配了但算不出 ⇒ 按 [`process_time`] 的结果写 NULL（Java 同形：`setExpireTime(processTime(...))`）。
pub fn apply_expire_time(task: &mut ProcessTask, expr: Option<&str>, args: &FlowData) {
    let Some(raw) = expr else { return };
    if raw.trim().is_empty() {
        return;
    }
    task.expire_time = process_time(Some(raw), args);
}

/// 求值器：把节点上配的到期表达式算成 `"yyyy-MM-dd HH:mm:ss"`，算不出给 `None`。
///
/// 三档**顺序不可变**（逐字对齐 Java `FlowUtil.processTime` / C# `FlowUtil.ProcessTime`）：
///
/// ⓪ 表达式为 `None`／空／纯空白 ⇒ `None`（这一列留 NULL）。
/// ① `args` 里存在**键名等于表达式原串**的项 ⇒ 取该项的值：
///    墙钟串 → 该串（解析失败即**终局** `None`，不落穿）；整值数值 → 毫秒时间戳；
///    类型不认识（bool / null / 数组 / 对象 / 非整值数值）→ **落穿**到后面两档（易错点①）。
/// ② 以 `s|m|h|d` 结尾且前缀是**非负**整数 ⇒ 基准 + N 秒/分/时/天（`d` 走**日历加天**；
///    负数前缀按 issues/137 D 算"解析不出来"，落穿到第 ③ 档，加号档仍合法）。
/// ③ 否则把表达式本身按 `yyyy-MM-dd HH:mm:ss` 严格解析；失败 → `None`。
///
/// 变量档**优先于**相对档（易错点②：`args` 里真有个键叫 `"2h"` 时取的是变量值）。
/// **任何一档都不许返回基准时刻本身**：本案病灶恰是"非空但错"的占位 `now()`（建单即逾期、
/// `expire − create ≈ 0`），一旦让 now() 兜底，L2-27 那格「同一行 expire − create 必须≈表达式偏移」
/// 就永远抓不到东西。
///
/// 基准＝本栈时钟单源 [`clock::current_time_str`]（宿主未注入时为 UTC）。
pub fn process_time(expr: Option<&str>, args: &FlowData) -> Option<String> {
    process_time_in(expr, args, &clock::current_time_str(), &clock::utc_time_str())
}

/// 同 [`process_time`]，但把"基准墙钟串"与"同一刻的 UTC 墙钟串"显式喂进来。
///
/// 存在的理由有两个：① 相对/绝对档可做**逐值**断言（不必拿真实 now 去凑带宽）；
/// ② 毫秒档需要宿主偏移，而偏移只能由这两串相减得出（见 [`clock`] 模块的注入约定）。
/// 生产调用点一律走 [`process_time`]，不要绕过时钟单源自己造基准。
pub fn process_time_in(expr: Option<&str>, args: &FlowData, base: &str, utc_base: &str) -> Option<String> {
    let raw = expr?;
    if raw.trim().is_empty() {
        return None;
    }
    // ① 变量档（键名＝表达式原串）
    if let Some(v) = args.get(raw) {
        match v {
            JsonValue::Str(s) => {
                // 字符串档解析失败＝终局 None（对齐 Java `catch → return null`），**不落穿**
                return to_epoch_secs(s).map(from_epoch_secs);
            }
            JsonValue::Number(n) if n.is_finite() && n.fract() == 0.0 => {
                // 毫秒时间戳档。核心没有时区概念 ⇒ 拿"引擎钟串 − UTC 串"当宿主偏移，
                // 让毫秒档与相对/绝对档落在**同一基准**里（未注入钟 ⇒ 偏移 0，等价于 UTC）。
                let offset = to_epoch_secs(base)? - to_epoch_secs(utc_base)?;
                return Some(from_epoch_secs((*n as i64).div_euclid(1000) + offset));
            }
            // bool / null / 数组 / 对象 / 非整值数值 ⇒ Java 侧不匹配任何 `instanceof` 档 ⇒ 落穿
            _ => {}
        }
    }
    // ② 相对档（基准串本身解析不出时跳过这一档，仍给绝对档一次机会）
    if let Some(b) = to_epoch_secs(base) {
        if let Some(t) = relative_secs(raw, b) {
            return Some(from_epoch_secs(t));
        }
    }
    // ③ 绝对档
    to_epoch_secs(raw).map(from_epoch_secs)
}

/// 相对档 `Ns`/`Nm`/`Nh`/`Nd`；后缀不识别、前缀非整数**或前缀是负数** ⇒ `None`（交回调用方走绝对档）。
///
/// §1.9-3 口径：前缀不是整数（节点误配成 `xh`）时 Java 会 `Integer.parseInt` **抛异常打断建单**，
/// 本栈与 C# 同走"**落穿** → 绝对档 → 最终 NULL"（配置写错不该让流程卡死）。
/// **与 Java 的差异是故意的**，要改成"跟 Java 一样抛"必须八栈同批改、另立案。
///
/// issues/137 D（owner 2026-10-01 拍"判非负"，spec/04 §任务行 expire_time）：**负数前缀同样算
/// "解析不出来"**，与上面误配档同一落穿路径 ⇒ 绝对档 ⇒ 仍解析不出就 NULL。
fn relative_secs(expr: &str, base: i64) -> Option<i64> {
    let bytes = expr.as_bytes();
    if bytes.len() < 2 {
        return None;
    }
    let unit = *bytes.last()?;
    // 末字节是 UTF-8 续字节（>=0x80）⇒ 不可能是 s/m/h/d，先返回，顺带避开按字节切串的 panic
    if !unit.is_ascii() {
        return None;
    }
    let prefix = expr[..expr.len() - 1].trim();
    let n: i64 = prefix.parse().ok()?;
    // issues/137 D 判非负：负数偏移不是合法到期档——放行 `-5h` 会算出一个**过去**的时刻 ⇒
    // 新建的行当场就是逾期，比"没配到期时间"更难发现，也正是上面"任何一档都不许退化成取当前时间"
    // （issues/126 病灶）的同向延伸。归 `None` 走既有的 `?` 落穿分支，不新造返回路径。
    // 四档共用**这一枚**前缀解析 ⇒ 一处即全覆盖：`d` 档走 `add_calendar_days` 也只是把同一个 `n`
    // 交给历日加天（负数＝历日倒退），没有第二条前缀解析的旁路。
    // 只裁负、**不裁加号**：各栈整数解析（python `[+-]?`、node `[-+]?\d+`、php `[+-]?\d{1,18}`、
    // 本栈 `i64::from_str`）都收 `+`，裁掉加号等于新造一处跨栈分叉。
    if n < 0 {
        return None;
    }
    match unit {
        b's' => Some(base + n),
        b'm' => Some(base + n.checked_mul(60)?),
        b'h' => Some(base + n.checked_mul(3600)?),
        b'd' => Some(add_calendar_days(base, n)),
        _ => None,
    }
}

/// `Nd` 档：**日历加天**，不是乘 86400 秒——与 Java `Calendar.add(DAY_OF_MONTH)`、
/// Go `AddDate(0,0,n)` 同量纲（跨月/跨年/闰日按历日走）。
fn add_calendar_days(secs: i64, days: i64) -> i64 {
    let (y, m, d) = civil_from_days(secs.div_euclid(SECS_PER_DAY));
    let tod = secs.rem_euclid(SECS_PER_DAY);
    days_from_civil(y, m, d + days) * SECS_PER_DAY + tod
}

/// 墙钟串 → 伪 epoch 秒（只用于求值）。严格档：定长 19、分隔位、月/日/时/分/秒取值域
/// （**不接受** `2026-02-30` 这类越界日；Java 的 `SimpleDateFormat` 是宽松档会滚成 3-02，
/// 本栈与 Go `time.Parse` 同口径按"解析不出"处理，落 NULL 而不是造一个滚出来的时刻）。
pub(crate) fn to_epoch_secs(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() != DATE_TIME_LEN {
        return None;
    }
    if !(b[4] == b'-' && b[7] == b'-' && b[10] == b' ' && b[13] == b':' && b[16] == b':') {
        return None;
    }
    let digits = |from: usize, to: usize| -> Option<i64> {
        let mut v = 0i64;
        for byte in &b[from..to] {
            if !byte.is_ascii_digit() {
                return None;
            }
            v = v * 10 + (byte - b'0') as i64;
        }
        Some(v)
    };
    let (y, m, d) = (digits(0, 4)?, digits(5, 7)?, digits(8, 10)?);
    let (hh, mm, ss) = (digits(11, 13)?, digits(14, 16)?, digits(17, 19)?);
    if !(1..=12).contains(&m) || !(1..=days_in_month(y, m)).contains(&d) {
        return None;
    }
    if hh > 23 || mm > 59 || ss > 59 {
        return None;
    }
    Some(days_from_civil(y, m, d) * SECS_PER_DAY + hh * 3600 + mm * 60 + ss)
}

/// 伪 epoch 秒 → 墙钟串：**复用时钟模块的格式化出口**，不另起一套历法（`clock::format_unix_utc`）。
fn from_epoch_secs(secs: i64) -> String {
    clock::format_unix_utc(secs)
}

/// Howard Hinnant `days_from_civil`：proleptic Gregorian 日期 → 距 1970-01-01 的天数。
/// `d` 允许越界（`add_calendar_days` 就靠这一点做日历加天）。
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400; // [0, 399]
    let mp = if m > 2 { m - 3 } else { m + 9 };
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// `days_from_civil` 的逆（与 `clock::format_unix_utc` 内的那段同源）。
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as i64;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m as i64, d)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn days_in_month(y: i64, m: i64) -> i64 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap(y) {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 固定基准（与 `utc_base` 相差整 8 小时，顺带把"毫秒档按宿主偏移换算"这一档钉住）
    const BASE: &str = "2026-09-28 12:00:00";
    const UTC_BASE: &str = "2026-09-28 04:00:00"; // 偏移 = +8h

    fn ev(expr: &str, args: &[(&str, JsonValue)]) -> Option<String> {
        let mut fd = FlowData::new();
        for (k, v) in args {
            fd.insert((*k).to_string(), v.clone());
        }
        process_time_in(Some(expr), &fd, BASE, UTC_BASE)
    }

    /// 正向①-a：三档语义逐字对齐（相对四单位 + 绝对档），**逐值**断言不含带宽
    #[test]
    fn test_relative_and_absolute_tiers_exact() {
        assert_eq!(ev("90s", &[]).as_deref(), Some("2026-09-28 12:01:30"));
        assert_eq!(ev("45m", &[]).as_deref(), Some("2026-09-28 12:45:00"));
        assert_eq!(ev("2h", &[]).as_deref(), Some("2026-09-28 14:00:00"));
        assert_eq!(ev("3d", &[]).as_deref(), Some("2026-10-01 12:00:00"));
        // 本格原为 `assert_eq!(ev("-1h", &[]).as_deref(), Some("2026-09-28 11:00:00"));`
        // ——issues/137 D owner 2026-10-01 拍"判非负"：原断负偏移生效＝放行负数、新建行当场逾期，
        // 本格按裁定**翻面**（负数相对档一律算不出，见 negative_relative_expression_stays_null）。
        // 原本靠 `-1h` 覆盖的"时档做加减算术 + 跨日界"覆盖面用**正数**补回，不缩水：
        // `+1h` 同时钉住"加号合法"（只裁负不裁加号），`31d` 钉跨月正向历日推进。
        assert_eq!(ev("-1h", &[]), None, "负数时档按 issues/137 D 算解析不出");
        assert_eq!(ev("+1h", &[]).as_deref(), Some("2026-09-28 13:00:00"));
        assert_eq!(ev("31d", &[]).as_deref(), Some("2026-10-29 12:00:00"), "跨月正向");
        assert_eq!(
            ev("2027-03-04 05:06:07", &[]).as_deref(),
            Some("2027-03-04 05:06:07"),
            "绝对档：表达式本身就是时刻"
        );
    }

    /// 正向①-b：`d` 档必须走**日历加天**（跨月/跨年/闰日），不是"乘 86400 后随便滚"
    #[test]
    fn test_day_tier_is_calendar_based() {
        assert_eq!(ev("1d", &[]).as_deref(), Some("2026-09-29 12:00:00"));
        // 月末 +1 天 ⇒ 落到下月 1 号；年末 +1 天 ⇒ 跨年；闰日 / 闰年内 +N 天 ⇒ 按历日走
        let cases = [
            ("2026-01-31 23:00:00", "1d", "2026-02-01 23:00:00"),
            ("2026-12-31 23:00:00", "1d", "2027-01-01 23:00:00"),
            ("2024-02-29 10:00:00", "1d", "2024-03-01 10:00:00"),
            ("2024-02-27 10:00:00", "3d", "2024-03-01 10:00:00"),
            ("2026-09-28 12:00:00", "100d", "2027-01-06 12:00:00"),
        ];
        for (base, expr, want) in cases.iter() {
            assert_eq!(
                process_time_in(Some(*expr), &FlowData::new(), base, base).as_deref(),
                Some(*want),
                "{base} + {expr} 应为日历加天"
            );
        }
    }

    /// 正向②：表达式是**变量名** ⇒ 取该变量的值（墙钟串 / 毫秒时间戳两档）
    #[test]
    fn test_variable_tier_takes_its_value() {
        assert_eq!(
            ev("dueAt", &[("dueAt", JsonValue::string("2026-12-31 10:00:00"))]).as_deref(),
            Some("2026-12-31 10:00:00")
        );
        // 毫秒档：epoch 毫秒按**宿主偏移**换算进引擎钟基准（偏移 = 基准串 − UTC 串 = +8h，
        // 所以 epoch 2026-12-31 02:00:00Z 在宿主眼里是 10:00:00），与其余两档同基准
        let ms = to_epoch_secs("2026-12-31 02:00:00").unwrap() * 1000;
        assert_eq!(
            ev("dueMs", &[("dueMs", JsonValue::number(ms as f64))]).as_deref(),
            Some("2026-12-31 10:00:00"),
            "毫秒档必须按宿主偏移落进引擎钟基准"
        );
    }

    /// 三个易错点各自的钉子：落穿 / 变量档优先 / 字符串档失败即终局
    #[test]
    fn test_variable_tier_three_pitfalls() {
        // 易错点①：变量存在但类型不认识 ⇒ **落穿**到相对档（不是提前返回空）
        assert_eq!(
            ev("2h", &[("2h", JsonValue::Bool(true))]).as_deref(),
            Some("2026-09-28 14:00:00"),
            "bool 值该落穿走相对档"
        );
        // 非整值数值（Java 的 Double 档）同样落穿；此处落穿后无档可走 ⇒ 空
        assert_eq!(
            ev("dueF", &[("dueF", JsonValue::number(1.5))]).as_deref(),
            None,
            "非整值数值落穿后仍算不出 ⇒ 空"
        );
        // 易错点②：变量档**优先于**相对档（键真叫 "2h" 时取变量值，不是基准+2h）
        assert_eq!(
            ev("2h", &[("2h", JsonValue::string("2026-12-31 10:00:00"))]).as_deref(),
            Some("2026-12-31 10:00:00")
        );
        // 易错点③：字符串档解析失败＝终局空，**不落穿**（键名 "1h" 若落穿会算出基准+1h）
        assert_eq!(
            ev("1h", &[("1h", JsonValue::string("tomorrow"))]).as_deref(),
            None,
            "变量值是解析不出的字符串 ⇒ 终局空，不许落穿去走相对档"
        );
        // 值为 JSON null ⇒ 落穿
        assert_eq!(ev("dueNull", &[("dueNull", JsonValue::Null)]).as_deref(), None);
    }

    /// 负向：解析不出的五种写法一律**空**，且必须不是基准时刻（占位 `now()` 是本病灶的形状）
    #[test]
    fn test_unparsable_stays_null_never_base_time() {
        for expr in ["not-a-time", "2027-03-04", "2027-03-04T05:06:07", "xh", "hh", "  ", "2026-02-30 10:00:00"] {
            let got = process_time_in(Some(expr), &FlowData::new(), BASE, UTC_BASE);
            assert_eq!(got, None, "表达式 {expr:?} 该算不出，实得 {got:?}");
            assert_ne!(
                got.as_deref(),
                Some(BASE),
                "任何一档都不许兜底成基准时刻（那等于建单即逾期）"
            );
        }
    }

    /// 负向（issues/137 D · owner 2026-10-01 拍"判非负"，对齐 java 基准
    /// `ExpireTimeOnCreateTest.negativeRelativeExpressionStaysNull`）：**负数相对档不是合法偏移**。
    ///
    /// 放行 `-5h` 会算出一个**过去**的时刻 ⇒ 新建的行当场就是逾期，比"没配到期时间"更难发现，
    /// 与上面 `test_unparsable_stays_null_never_base_time` 的"不许退化成取当前时间"（issues/126 病灶）同向。
    /// 判据形状是 `None`（落穿绝对档后仍解析不出）——**不是 panic、不是基准时刻、不是回拨后的时刻**。
    /// 四档 `s/m/h/d` 共用 [`relative_secs`] 里同一枚前缀解析，故四档各自钉一格；`d` 档那一格
    /// 尤其要紧：它走 `add_calendar_days`，负数是**历日倒退**，不是乘 86400 秒。
    #[test]
    fn test_negative_relative_expression_stays_null() {
        // ① 四档负数前缀全部算不出，三个侧面一起钉：不是回拨后的时刻（那样的话 `实得 {got:?}`
        // 直接把那个过去的时刻打在失败信息里）、不是基准时刻、求值不得 panic
        for expr in ["-30s", "-5m", "-5h", "-5d"] {
            let got = ev(expr, &[]);
            assert_eq!(got, None, "负数相对档 {expr:?} 该算不出（落穿 ⇒ 空），实得 {got:?}");
            assert_ne!(
                got.as_deref(),
                Some(BASE),
                "负数档也不许兜底成基准时刻"
            );
        }

        // ② 正向对照：**只裁负、不裁加号**——`+2h` 必须仍算得出 now+7200s，`2h`/`2d` 一字不变。
        // 这一格保证上面两判不是"把整档裁成恒真"：往 prefix 上动刀禁掉 '+'、或把整枚相对档
        // 直接改成 return None，都会红在这里（摘掉 `< 0` 那一判则红在 ①）
        let plus = ev("+2h", &[]).expect("加号档必须仍合法：各栈整数解析都收 '+'，裁加号＝新造跨栈分叉");
        assert_eq!(plus.as_str(), "2026-09-28 14:00:00");
        assert_eq!(
            to_epoch_secs(&plus).unwrap() - to_epoch_secs(BASE).unwrap(),
            7200,
            "+2h 必须以基准为起点加满 7200s"
        );
        assert_eq!(ev("2h", &[]).as_deref(), Some("2026-09-28 14:00:00"), "无符号时档不变");
        assert_eq!(ev("2d", &[]).as_deref(), Some("2026-09-30 12:00:00"), "无符号天档不变");
        assert_eq!(ev("+3d", &[]).as_deref(), Some("2026-10-01 12:00:00"), "加号天档同样合法");

        // ③ 变量档与绝对档不受本次裁定影响（变量档**先于**相对档，键名恰好带负号也照取）
        assert_eq!(
            ev("dueAt", &[("dueAt", JsonValue::string("2026-12-31 10:00:00"))]).as_deref(),
            Some("2026-12-31 10:00:00"),
            "变量档照旧取变量值"
        );
        assert_eq!(
            ev("-5h", &[("-5h", JsonValue::string("2026-12-31 10:00:00"))]).as_deref(),
            Some("2026-12-31 10:00:00"),
            "键名叫 \"-5h\" 的变量仍走变量档（判非负只作用于相对档，不得漏进变量档）"
        );
        assert_eq!(
            ev("2026-12-31 10:00:00", &[]).as_deref(),
            Some("2026-12-31 10:00:00"),
            "绝对档照旧成功"
        );

        // ④ 坏前缀行为不变（本来就落穿）：非整数 / 小数 / 后缀不认识三档
        for expr in ["xh", "2.5h", "3hh", "-2.5h"] {
            assert_eq!(ev(expr, &[]), None, "误配档 {expr:?} 行为不变：落穿 ⇒ 空");
        }
    }

    /// 负向：`None` / 空串 / 纯空白三档都算"没配"
    #[test]
    fn test_unconfigured_returns_none() {
        let empty = FlowData::new();
        assert_eq!(process_time_in(None, &empty, BASE, UTC_BASE), None);
        assert_eq!(process_time_in(Some(""), &empty, BASE, UTC_BASE), None);
        assert_eq!(process_time_in(Some("   "), &empty, BASE, UTC_BASE), None);
    }

    /// 写点尺子：没配 ⇒ **不动这一列**（回退新建靠这一档沿用继承值）；配了算不出 ⇒ 清成空
    #[test]
    fn test_apply_expire_time_untouched_when_unconfigured() {
        let mut task = ProcessTask {
            task_id: 1,
            process_instance_id: 1,
            task_name: "t".into(),
            display_name: "T".into(),
            task_type: 0,
            perform_type: 0,
            task_state: 10,
            actor_id: None,
            actor_ids: vec!["u".into()],
            finish_time: None,
            expire_time: Some("2026-09-28 12:00:00".into()), // 模拟回退克隆来的继承值
            form_key: None,
            parent_task_id: None,
            variables: FlowData::new(),
            create_time: Some(BASE.into()),
            create_user: None,
            update_time: None,
            update_user: None,
        };
        apply_expire_time(&mut task, None, &FlowData::new());
        assert_eq!(
            task.expire_time.as_deref(),
            Some(BASE),
            "没配表达式时不得清空已有值，也不得改写"
        );
        apply_expire_time(&mut task, Some("   "), &FlowData::new());
        assert_eq!(task.expire_time.as_deref(), Some(BASE), "空白串同样算没配");
        apply_expire_time(&mut task, Some("garbage"), &FlowData::new());
        assert_eq!(
            task.expire_time, None,
            "配了但算不出 ⇒ 写 NULL（对齐 Java setExpireTime(processTime(...))）"
        );
    }

    /// 求值器**不自己取钟**：未注入时钟时 `process_time` 的相对档必须跟着 `clock::current_time_str()`
    /// （issues/120 路 1 的时钟单源约束；拿真实 UTC 或 `SystemTime` 当基准都算破口）
    #[test]
    fn test_process_time_follows_engine_clock() {
        use crate::clock::testclock::at;
        let _scope = clock::ClockScope::injected(fixed_clock);
        assert_eq!(clock::current_time_str(), at(48), "内部对照：注入钟生效");
        let got = process_time(Some("2h"), &FlowData::new()).expect("2h 该算出值");
        let secs = to_epoch_secs(&got).unwrap() - to_epoch_secs(&at(48)).unwrap();
        assert_eq!(secs, 7200, "相对档必须以注入钟为基准，实得偏移 {secs}s");
        // 同一格拿真实 UTC 当基准去比必须落在外面 ⇒ 证明它读的是注入钟而不是系统钟
        let utc = clock::utc_time_str();
        assert_ne!(Some(got.clone()), process_time_at_utc("2h", &utc),
            "注入钟生效时不得等于按真实 UTC 算的值");
    }

    fn fixed_clock() -> String {
        crate::clock::testclock::at(48)
    }

    fn process_time_at_utc(expr: &str, utc_now: &str) -> Option<String> {
        process_time_in(Some(expr), &FlowData::new(), utc_now, utc_now)
    }

    /// 历法编解码自洽（伪 epoch ⇄ 墙钟串），免得档位断言建在错尺子上
    #[test]
    fn test_wall_clock_codec() {
        assert_eq!(to_epoch_secs("1970-01-01 00:00:00"), Some(0));
        assert_eq!(from_epoch_secs(0), "1970-01-01 00:00:00");
        assert_eq!(to_epoch_secs("2000-02-29 12:00:00").is_some(), true);
        assert_eq!(to_epoch_secs("1900-02-29 12:00:00"), None, "1900 非闰年");
        assert_eq!(to_epoch_secs("2026-13-01 00:00:00"), None);
        assert_eq!(to_epoch_secs("2026-09-31 00:00:00"), None, "9 月没有 31 号");
        assert_eq!(to_epoch_secs("2026-09-28 24:00:00"), None);
        assert_eq!(to_epoch_secs("2026-9-28 12:00:00"), None, "月必须两位");
        assert_eq!(to_epoch_secs("2026-09-2812:00:00"), None, "分隔符必须有空格");
        for s in ["1970-01-01 00:00:00", "2024-02-29 23:59:59", "2026-12-31 00:00:00"] {
            assert_eq!(from_epoch_secs(to_epoch_secs(s).unwrap()).as_str(), s);
        }
    }

    /// 节点表达式读取：缺键 / null / 串 / 非串四档
    #[test]
    fn test_expire_expr_of_node() {
        use crate::parser::ModelParser;
        let json = r#"{
            "name":"e","displayName":"e","type":"approval",
            "nodes":[
                {"id":"a","type":"snaker:task","text":{"value":"A"},"properties":{"assignee":"u"}},
                {"id":"b","type":"snaker:task","text":{"value":"B"},"properties":{"expireTime":null}},
                {"id":"c","type":"snaker:task","text":{"value":"C"},"properties":{"expireTime":"2h"}},
                {"id":"d","type":"snaker:task","text":{"value":"D"},"properties":{"expireTime":3600}},
                {"id":"e2","type":"snaker:task","text":{"value":"E"},"properties":{"expireTime":""}}
            ],
            "edges":[]
        }"#;
        let model = ModelParser::parse(json).unwrap();
        let expr_of = |id: &str| expire_expr_of(model.get_node(id).unwrap());
        assert_eq!(expr_of("a"), None, "属性缺键＝没配");
        assert_eq!(expr_of("b"), None, "配成 JSON null 同样＝没配（不得伪造成 \"null\" 串）");
        assert_eq!(expr_of("c").as_deref(), Some("2h"));
        assert_eq!(expr_of("d").as_deref(), Some("3600"), "非字符串按 toString 同档");
        assert_eq!(expr_of("e2").as_deref(), Some(""), "空串由尺子那一层判'没配'");
    }
}
