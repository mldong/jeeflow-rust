//! 引擎时间串的唯一出口（issues/120）。
//!
//! 基准由**宿主注入**决定，引擎不自取：core 保持零依赖（`std` 只有 epoch，拿不到本地时区偏移），
//! 未注入时回落 UTC。需要本地基准的宿主（框架壳、demo）在启动时 [`set_clock`] 注入格式化函数，
//! 此后写库审计列、转办留痕、`NOW()` 占位符、委托生效窗判据全部同基准——此前它们是两套
//! （`model::current_time_str()` 走 UTC，门面 `NOW()` 走 `chrono::Local`）。
//!
//! 与 MoonBit `core/model/clock.mbt` 的 `set_clock`、C# `ClockSpi.cs` 的 `IClock` 同构。

use std::sync::{Mutex, MutexGuard, RwLock};

pub type ClockFn = fn() -> String;

static CLOCK: RwLock<Option<ClockFn>> = RwLock::new(None);
/// 注入的独占权——时钟是进程级的，用例并发跑时会互相串成假绿/假红。
static CLOCK_SCOPE: Mutex<()> = Mutex::new(());

/// 注入时钟；传 `None` 恢复默认基准（UTC）。
///
/// 宿主在**启动期**（处理请求之前）调用一次。用例请改用 [`ClockScope`]，它带互斥且自动复原。
pub fn set_clock(f: Option<ClockFn>) {
    *CLOCK.write().unwrap_or_else(|e| e.into_inner()) = f;
}

pub fn current_time_str() -> String {
    match CLOCK.read().unwrap_or_else(|e| e.into_inner()).as_ref() {
        Some(f) => f(),
        None => utc_time_str(),
    }
}

/// RAII 时钟作用域：构造时独占并注入 `f`，`drop` 时恢复默认基准。
///
/// 守卫跨 `await` 持有，故所在 future 不是 `Send`（用例用 current-thread runtime 即可）。
pub struct ClockScope {
    _scope: MutexGuard<'static, ()>,
}

impl ClockScope {
    pub fn injected(f: ClockFn) -> Self {
        let scope = lock_scope();
        set_clock(Some(f));
        Self { _scope: scope }
    }
}

impl Drop for ClockScope {
    fn drop(&mut self) {
        set_clock(None);
    }
}

/// 取得时钟作用域独占权但不注入：用于断言「无人注入时的默认基准」。
///
/// 已持有 [`ClockScope`] 时**不可**再调用——内部是非重入锁，会自锁。
pub fn lock_scope() -> MutexGuard<'static, ()> {
    CLOCK_SCOPE.lock().unwrap_or_else(|e| e.into_inner())
}

/// 默认基准：UTC（未注入时钟时走这条）。
pub fn utc_time_str() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    format_unix_utc(secs)
}

/// Format unix seconds (UTC) as `yyyy-MM-dd HH:mm:ss`.
pub(crate) fn format_unix_utc(secs: i64) -> String {
    // civil_from_days (Howard Hinnant) — days since 1970-01-01
    let z = secs.div_euclid(86400) + 719468;
    let era = if z >= 0 { z } else { z - 146096 }.div_euclid(146097);
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let tod = secs.rem_euclid(86400) as u32;
    let hh = tod / 3600;
    let mm = (tod % 3600) / 60;
    let ss = tod % 60;
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, hh, mm, ss)
}

#[cfg(test)]
pub(crate) mod testclock {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    pub(crate) fn utc_secs() -> i64 {
        SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
    }

    /// 以「本 UTC 日 00:00 + hour 小时」为基准生成时间串：同一 UTC 日内恒定，
    /// 且 `at(48)`（后天零点）与真实 UTC 至少相差 48h——
    /// 这样"以注入钟为基准的窗"必然容不下真实 now，用例才不会退化成
    /// 台账 120 §4 点名的那种"宽到与时区无关"的假绿格。
    pub(crate) fn at(hour: i64) -> String {
        let s = utc_secs();
        format_unix_utc(s - s.rem_euclid(86400) + hour * 3600)
    }
}

#[cfg(test)]
mod tests {
    use super::testclock::at;
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn fixed() -> String {
        at(48)
    }

    /// 正向：注入后出口给的就是注入串，守卫离开作用域后自动恢复。
    #[test]
    fn test_i120_injected_clock_wins_and_restores() {
        {
            let _scope = ClockScope::injected(fixed);
            assert_eq!(current_time_str(), at(48), "注入时钟后出口必须给注入值");
        }
        let _idle = lock_scope();
        assert_ne!(current_time_str(), at(48), "作用域结束必须恢复默认基准");
    }

    /// 回归：未注入时默认基准仍是 UTC（±2s 容差，避免跨秒假红）。
    /// 取 `lock_scope` 独占——否则别的用例的注入值会漏进来（实测串过：读到 `at(48)`）。
    #[test]
    fn test_i120_default_baseline_is_utc() {
        let _idle = lock_scope();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64;
        let within: Vec<String> = (now - 2..=now + 2).map(format_unix_utc).collect();
        let got = current_time_str();
        assert!(within.contains(&got), "默认基准应为 UTC，实得 {got}（候选 {within:?}）");
        assert_eq!(got.len(), 19);
    }

    /// 判窗用的真窗：以注入钟为基准的 now±1h 必命中、now+1h 起必不命中。
    fn sg(start: Option<String>, end: Option<String>) -> crate::model::ProcessSurrogate {
        crate::model::ProcessSurrogate {
            id: 1,
            process_name: "simple".into(),
            operator: "zhang".into(),
            surrogate: "agent".into(),
            start_time: start,
            end_time: end,
            enabled: 1,
            create_time: None,
            create_user: None,
            update_time: None,
            update_user: None,
        }
    }

    #[test]
    fn test_i120_window_is_evaluated_on_engine_clock() {
        let _scope = ClockScope::injected(fixed);
        let now = current_time_str();
        let open = sg(Some(at(47)), Some(at(49)));
        assert!(
            crate::surrogate::surrogate_hit(&open, "zhang", "simple", false, &now),
            "以注入钟为基准的 ±1h 窗必命中（注入钟={now}）"
        );
        // 同一扇窗拿真实 UTC 去判必须落在窗外——否则这格测不出时钟出口（假绿源头）
        assert!(
            !crate::surrogate::surrogate_hit(&open, "zhang", "simple", false, &utc_time_str()),
            "判窗若不读注入钟就会退化成用真实 UTC：这格负责抓住它"
        );
        let future = sg(Some(at(49)), Some(at(50)));
        assert!(
            !crate::surrogate::surrogate_hit(&future, "zhang", "simple", false, &now),
            "start 在注入钟之后必不命中"
        );
    }

    #[test]
    fn test_format_unix_utc_known_values() {
        assert_eq!(format_unix_utc(0), "1970-01-01 00:00:00");
        assert_eq!(format_unix_utc(1_704_067_200), "2024-01-01 00:00:00");
    }
}
