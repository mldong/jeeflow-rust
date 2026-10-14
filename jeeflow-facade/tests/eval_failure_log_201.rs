//! issues/201 · 门面 `highLight` 的决策边**求值失败**必须留日志（rust 腿）。
//!
//! audit8 队列 R79·owner 拍 C＝只统一「求值器自身抛异常 ⇒ 必须落一条带原因的可诊断日志」，
//! **遍历宽度不钉**（本栈维持"该边按 false 处理、其余边继续遍历"，与修前逐字同形）。
//!
//! 修前形状＝`eval_decision_expr` 末尾的 `matches!(evaluator.eval(..), Ok(Bool(true)))`：
//! `Err` 与「求值为假」被压成同一个 `false` ⇒ 表达式坏／SPI 抛错在对外读数上与"这条分支没走"
//! 完全同形、零线索。本栈 162 腿已给"解码失败"那条落了 WARN，本案收的是同一族里的第二条静默腿。
//!
//! 日志「真拿到」的取回口径照 162 的 rust 腿＝**自再执行**（进程内没有捕获 stderr 的稳定 API）：
//! 把本测试二进制以 `--exact <本测试> --nocapture` 重新拉起当子进程，`eprintln!` 落到它真实的
//! stderr，父进程整段捕获。子进程档位由 `I201_CHILD=fail|good|false` 选择。
//!
//! 三枚判据分工＝①失败⇒有日志且归因齐；③合法判假⇒**不许**有日志（两档必须分得开，否则 WARN
//! 会变成每次高亮都刷屏）；②正常⇒零日志且边集真产出 e3（阳性对照，防夹具没走到决策腿就自证绿）。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::error::{JeeflowError, JeeflowResult};
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::json::JsonValue;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::spi::{ExpressionEvaluator, ProcessExtRepository, ProcessRepository};
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const CHILD_ENV: &str = "I201_CHILD";
const TEST_NAME: &str = "test_i201_eval_failure_warns_with_instance_define_and_reason";
const WARN_MARK: &str = "决策边表达式求值失败";

/// 三档可切换求值器（正常／合法判假＝返回 false 不抛／求值失败＝抛）。
/// 建单与办 task1 固定走正常档（引擎运行时路由不受影响），切档只发生在 highLight 之前。
struct SwappableEval {
    fail: AtomicBool,
    all_false: AtomicBool,
}

impl SwappableEval {
    fn new() -> Self {
        SwappableEval {
            fail: AtomicBool::new(false),
            all_false: AtomicBool::new(false),
        }
    }
}

impl ExpressionEvaluator for SwappableEval {
    fn eval(&self, expression: &str, context: &HashMap<String, JsonValue>) -> JeeflowResult<JsonValue> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(JeeflowError::Internal(format!(
                "i201 夹具注入的求值失败（expr={expression}）"
            )));
        }
        if self.all_false.load(Ordering::SeqCst) {
            return Ok(JsonValue::Bool(false));
        }
        let amt = context.get("amount").and_then(|v| v.as_f64()).unwrap_or(0.0);
        Ok(JsonValue::Bool(match expression {
            "amount > 1000" => amt > 1000.0,
            "amount <= 1000" => amt <= 1000.0,
            _ => false,
        }))
    }
}

fn facade_with(ev: Arc<SwappableEval>) -> (JeeflowFacade, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)))
        .with_expression_evaluator(ev as Arc<dyn ExpressionEvaluator>);
    (JeeflowFacade::new(ctx), repo)
}

fn load_shared_flow(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../flows")
        .join(format!("{name}.json"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("读取共享夹具失败 {}: {e}", path.display()))
}

/// 夹具＝03-decision-expr.json：start → apply → task1 → decision1 →(e3 amount>1000 → task2 / e4 → task3) → end。
/// 返回 (实例号, 定义号, 门面, 求值器)；调用方在 highLight 之前切档。
async fn setup_stopped_at_task2() -> (i64, i64, JeeflowFacade, Arc<SwappableEval>) {
    let ev = Arc::new(SwappableEval::new());
    let (facade, repo) = facade_with(ev.clone());
    let mut define = jeeflow_core::model::ProcessDefine {
        id: 0,
        name: "decision-expr-i201".into(),
        display_name: "决策表达式流程（201）".into(),
        define_type: "approval".into(),
        state: 1,
        content: load_shared_flow("03-decision-expr").as_bytes().to_vec(),
        version: 1,
        create_time: None,
        create_user: Some("applicant".into()),
        update_time: None,
        update_user: None,
    };
    facade.repo().save_define(&mut define).unwrap();
    let did = define.id;

    let mut start_args: HashMap<String, Json> = HashMap::new();
    start_args.insert("processDefineId".into(), json!(did));
    start_args.insert("operator".into(), json!("applicant"));
    start_args.insert("amount".into(), json!(5000));
    let started = facade.flow("processInstance/startAndExecute", &start_args).await;
    assert_eq!(started["code"], 0, "发起失败: {started}");
    let iid = started["data"]["processInstanceId"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();

    let task = repo
        .find_doing_tasks(iid, &[])
        .unwrap()
        .into_iter()
        .find(|t| t.task_name == "task1")
        .expect("发起后 task1 应进行中");
    let mut exec_args: HashMap<String, Json> = HashMap::new();
    exec_args.insert("processTaskId".into(), json!(task.task_id));
    exec_args.insert("operator".into(), json!("leader"));
    exec_args.insert("submitType".into(), json!(1));
    let resp = facade.flow("processTask/execute", &exec_args).await;
    assert_eq!(resp["code"], 0, "办理 task1 失败: {resp}");

    // 夹具形状自证：必须停在 task2 活跃，否则递归到不了 decision1 的两条出边（②③档恒真）
    let doing: Vec<String> = repo
        .find_doing_tasks(iid, &[])
        .unwrap()
        .iter()
        .map(|t| t.task_name.clone())
        .collect();
    assert_eq!(doing, vec!["task2".to_string()], "实例应停在分支后的活跃 task2");

    (iid, did, facade, ev)
}

fn edges_of(hl: &Json) -> Vec<String> {
    hl["data"]["historyEdgeNames"]
        .as_array()
        .expect("historyEdgeNames 应为数组")
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect()
}

async fn high_light(facade: &JeeflowFacade, iid: i64) -> Json {
    let mut args: HashMap<String, Json> = HashMap::new();
    args.insert("id".into(), json!(iid));
    let hl = facade.flow("processInstance/highLight", &args).await;
    assert_eq!(
        hl["code"], 0,
        "拍 C 口径＝降级面保留，求值失败只许留痕不许变红: {hl}"
    );
    hl
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

fn field_of(stdout: &str, key: &str) -> String {
    stdout
        .split_whitespace()
        .find_map(|tok| tok.strip_prefix(&format!("{key}=")))
        .unwrap_or_else(|| panic!("子进程 stdout 缺 {key}= 读数: {stdout}"))
        .to_string()
}

#[tokio::test]
async fn test_i201_eval_failure_warns_with_instance_define_and_reason() {
    match std::env::var(CHILD_ENV).as_deref() {
        // ── ① 子进程档：求值器抛错 ⇒ 落 WARN，边集按 false 不收（宽度＝修前形状）──
        Ok("fail") => {
            let (iid, did, facade, ev) = setup_stopped_at_task2().await;
            ev.fail.store(true, Ordering::SeqCst);
            let hl = high_light(&facade, iid).await;
            let edges = edges_of(&hl);
            assert!(
                !edges.contains(&"e3".to_string()) && !edges.contains(&"e4".to_string()),
                "本栈宽度＝求值失败按 false 处理，e3/e4 都不该收（拍 C 不钉宽度，但行为变了就是越界），实得 {edges:?}"
            );
            let active: Vec<String> = hl["data"]["activeNodeNames"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap_or_default().to_string())
                .collect();
            assert!(
                active.contains(&"task2".to_string()),
                "失败只许影响模型补全腿，活跃任务行仍须是 task2，实得 {active:?}"
            );
            println!("I201_IDS iid={iid} did={did}");
        }
        // ── ③ 子进程档：合法判假（返回 false、不抛）⇒ 同样不收 e3，但**不许**落求值失败日志 ──
        Ok("false") => {
            let (iid, did, facade, ev) = setup_stopped_at_task2().await;
            ev.all_false.store(true, Ordering::SeqCst);
            let hl = high_light(&facade, iid).await;
            let edges = edges_of(&hl);
            assert!(
                !edges.contains(&"e3".to_string()),
                "判假档 e3 不该收（夹具自证），实得 {edges:?}"
            );
            println!("I201_IDS iid={iid} did={did}");
        }
        // ── ② 子进程档：正常求值 ⇒ 边集真产出 e3（阳性对照）──
        Ok("good") => {
            let (iid, did, facade, _ev) = setup_stopped_at_task2().await;
            let hl = high_light(&facade, iid).await;
            let edges = edges_of(&hl);
            assert!(
                edges.contains(&"e3".to_string()),
                "夹具未走到决策边求值（本档自证失效），期望收 e3，实得 {edges:?}"
            );
            println!("I201_IDS iid={iid} did={did}");
        }
        _ => {
            // ── 父进程腿 ①：求值失败 ⇒ 日志真拿到，且归因件齐 ──
            let (stdout, stderr, ok) = rerun_child("fail");
            assert!(ok, "失败档子进程须跑绿: stdout={stdout} stderr={stderr}");
            assert!(
                stdout.contains("1 passed"),
                "自证管道：子进程得真跑到这枚测试（防 --exact 名字漂了 0 用例静默绿）: {stdout}"
            );
            let iid = field_of(&stdout, "iid");
            let did = field_of(&stdout, "did");
            assert!(
                stderr.contains(WARN_MARK),
                "求值失败必须落一条可诊断日志（静默通道就是本案要收的那条），实得 stderr: {stderr}"
            );
            for piece in [
                format!("instanceId={iid}"),
                format!("defineId={did}"),
                "nodeId=decision1".to_string(),
                "expr=".to_string(),
                "err=".to_string(),
            ] {
                assert!(
                    stderr.contains(&piece),
                    "日志缺归因件 {piece:?}（缺一件等于没落），实得 stderr: {stderr}"
                );
            }

            // ── 父进程腿 ②：正常路径 ⇒ 不得出现该 WARN（挂在失败支上，不是每次高亮都刷屏）──
            let (stdout2, stderr2, ok2) = rerun_child("good");
            assert!(ok2, "正常档子进程须跑绿: stdout={stdout2} stderr={stderr2}");
            assert!(
                stdout2.contains("1 passed"),
                "自证管道：正常档子进程得真跑到这枚测试: {stdout2}"
            );
            assert!(
                !stderr2.contains(WARN_MARK),
                "正常路径不该出现求值失败日志: {stderr2}"
            );

            // ── 父进程腿 ③：合法判假 ⇒ 不收边但也不落"求值失败"日志（两档必须分得开）──
            let (stdout3, stderr3, ok3) = rerun_child("false");
            assert!(ok3, "判假档子进程须跑绿: stdout={stdout3} stderr={stderr3}");
            assert!(
                stdout3.contains("1 passed"),
                "自证管道：判假档子进程得真跑到这枚测试: {stdout3}"
            );
            assert!(
                !stderr3.contains(WARN_MARK),
                "合法判假不是失败，不该落日志（本案只统一\"失败必留痕\"）: {stderr3}"
            );
        }
    }
}
