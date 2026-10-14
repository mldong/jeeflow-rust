//! issues/216 · 门面 stats 取钟串解析失败必须留可诊断日志（rust 腿）。
//!
//! audit8 队列 R48（层-2）：`stats_now()` 解析 `clock::current_time_str()` 失败时**静默**
//! 回落 `chrono::Local` —— 宿主注入非 `%Y-%m-%d %H:%M:%S` 格式的钟时，数据基准
//! （写库时间列＝注入串原样）与统计口径（回落 Local）静默分家，且 stats 族无 runner 格，
//! 对外读数与"注入钟生效"完全同形、零线索。修法照 156/160/162/201 同法＝保留回落面，
//! 回落前落一条带原因 WARN（文案纯函数单点）。
//!
//! 日志「真拿到」的取回口径照 162/201 的 rust 腿＝**自再执行**（进程内没有捕获 stderr 的稳定
//! API）：把本测试二进制以 `--exact <本测试> --nocapture` 重新拉起当子进程（档位由 `I216_CHILD`
//! 选择），`eprintln!` 落到它真实的 stderr，父进程整段捕获。
//!
//! 判据分工＝①坏钟档：不 panic＋WARN 归因齐（raw 原串／expect 格式／回落后果），且回落 Local
//! 真被用上（种子＝Local 当日 ⇒ todayNew=1，防"解析失败后统计被废掉/清零"换形态）；
//! ②好钟档：**不许**有该 WARN（挂在失败支上，不是每次统计都刷屏）＋注入钟真生效
//! （todayNew=1；真实今天盘不出 2025 种子——判别法同 i120 格）。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::json::FlowData;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::ProcessInstance;
use jeeflow_core::spi::{ProcessExtRepository, ProcessRepository};
use jeeflow_facade::JeeflowFacade;
use std::collections::HashMap;
use std::sync::Arc;

const CHILD_ENV: &str = "I216_CHILD";
const TEST_NAME: &str = "test_i216_stats_clock_parse_failure_warns_and_falls_back";
const WARN_MARK: &str = "stats 取钟串解析失败";
/// 坏钟＝RFC3339 带时区（宿主最可能犯的一类：直接把系统钟 format 成 ISO 串注入）。
const BAD_CLOCK: &str = "2026-01-02T03:04:05+08:00";

fn facade_with_instance(create_time: &str) -> JeeflowFacade {
    let repo = Arc::new(MemoryRepository::new());
    let mut inst = ProcessInstance {
        instance_id: 0,
        parent_id: None,
        define_id: 1,
        state: 10,
        parent_node_name: None,
        business_no: None,
        operator: "u1".into(),
        expire_time: None,
        variables: FlowData::new(),
        tasks: vec![],
        create_time: Some(create_time.into()),
        create_user: None,
        update_time: None,
        update_user: None,
        define: None,
    };
    repo.save_instance(&mut inst).unwrap();
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
    JeeflowFacade::new(ctx)
}

/// 以子进程重跑本测试自身（--nocapture 让 eprintln! 落到真实 stderr），返回 (stdout, stderr, ok)。
fn rerun_child(mode: &str) -> (String, String, bool) {
    let exe = std::env::current_exe().expect("current_exe");
    let out = std::process::Command::new(&exe)
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_ENV, mode)
        .output()
        .expect("重新拉起测试二进制失败");
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.success(),
    )
}

#[test]
fn test_i216_stats_clock_parse_failure_warns_and_falls_back() {
    match std::env::var(CHILD_ENV).as_deref() {
        // ── 子进程档 ①：坏钟 ⇒ 解析失败回落 Local（行为保留），且统计真跑得出结果 ──
        Ok("bad") => {
            let _scope = jeeflow_core::clock::ClockScope::injected(|| BAD_CLOCK.to_string());
            // 种子＝Local 当日 ⇒ 只有回落支真用上 Local 才能盘出 todayNew=1
            let local_now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
            let facade = facade_with_instance(&local_now);
            let resp = facade
                .stats_overview(&HashMap::new())
                .expect("坏钟不许让统计腿 panic/变红（回落面保留）");
            assert_eq!(resp["total"], 1, "实例计数与钟无关，实得 {resp}");
            assert_eq!(
                resp["todayNew"], 1,
                "解析失败后须回落 chrono::Local：种子＝Local 当日必须盘出 1（统计被废掉/清零则红），实得 {resp}"
            );
        }
        // ── 子进程档 ②：好钟（注入日 2025-01-10）⇒ 注入钟真生效 ──
        Ok("good") => {
            let _scope =
                jeeflow_core::clock::ClockScope::injected(|| "2025-01-10 12:00:00".to_string());
            let facade = facade_with_instance("2025-01-10 10:00:00");
            let resp = facade.stats_overview(&HashMap::new()).unwrap();
            assert_eq!(resp["total"], 1, "实例计数与钟无关，实得 {resp}");
            assert_eq!(
                resp["todayNew"], 1,
                "好钟档须用注入钟算当日窗（真实今天盘不出这枚 2025 种子），实得 {resp}"
            );
        }
        _ => {
            // ── 父进程腿 ①：坏钟 ⇒ WARN 真拿到，且归因件齐（raw 原串／expect 格式／回落后果）──
            let (stdout, stderr, ok) = rerun_child("bad");
            assert!(ok, "坏钟档子进程须跑绿: stdout={stdout} stderr={stderr}");
            assert!(
                stdout.contains("1 passed"),
                "自证管道：子进程得真跑到这枚测试（防 --exact 名字漂了 0 用例静默绿）: {stdout}"
            );
            assert!(
                stderr.contains(WARN_MARK),
                "解析失败必须落一条可诊断日志（静默换钟就是本案要收的那条），实得 stderr: {stderr}"
            );
            for piece in [
                "WARN".to_string(),
                format!("raw=\"{BAD_CLOCK}\""),
                "expect=".to_string(),
                "%Y-%m-%d %H:%M:%S".to_string(),
                "chrono::Local".to_string(),
            ] {
                assert!(
                    stderr.contains(&piece),
                    "日志缺归因件 {piece:?}（缺一件等于没落），实得 stderr: {stderr}"
                );
            }

            // ── 父进程腿 ②：好钟 ⇒ 不得出现该 WARN（挂在失败支上，不是每次统计都刷屏）──
            let (stdout2, stderr2, ok2) = rerun_child("good");
            assert!(ok2, "好钟档子进程须跑绿: stdout={stdout2} stderr={stderr2}");
            assert!(
                stdout2.contains("1 passed"),
                "自证管道：好钟档子进程得真跑到这枚测试: {stdout2}"
            );
            assert!(
                !stderr2.contains(WARN_MARK),
                "正常路径不该出现解析失败日志: {stderr2}"
            );
        }
    }
}
