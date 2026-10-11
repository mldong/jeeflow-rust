//! issues/162 · 门面 `highLight` 模型补全腿降级必须留日志（rust 腿）。
//!
//! audit8 队列 R3 搭车：162 §6.3 点名 rust 的搭车轮含 R3，判据形状照 §6.1 的 go v1.8.43 那枚。
//! 口径＝**保留降级面＋落一条可诊断日志**（156/160 两例同法）：坏定义只许降级模型补全腿
//! （边集/节点补全/nodeProgress；实例与任务行不受影响、对外仍 code=0），但不再静默——
//! rust 旧形状是 `if let Ok(model) = ModelParser::parse(...)` 门、失败支无动作，
//! 这条静默通道先后藏住过 156（php）与 160（python）两个真分叉。
//!
//! 日志「真拿到」的取回口径：进程内 Rust 没有捕获 stderr 的稳定 API（libtest 的
//! `set_output_capture` 是 nightly 内部件），所以本文件用**自再执行**——把当前测试二进制以
//! `--exact <本测试> --nocapture` 重新拉起当子进程，子进程跑同一场景、`eprintln!` 落到它真实
//! 的 stderr，父进程 `.output()` 整段捕获。与 go 那枚直接捕获 `log` 输出同判据，
//! 不接受"注释自称已落日志"；子进程的选项支由环境变量 `I162_CHILD=broken|good` 选择。
//!
//! 判据两向（各起新栈，互相遮不住）：
//! ① 坏定义 ⇒ code=0＋边集空＋nodeProgress 空对象＋活跃任务行仍是 task1＋**日志真拿到**
//!   （三件齐：instanceId=真号／defineId=真号／err=）；
//! ② 好定义 ⇒ 补全腿产出非空边集（夹具自证）＋**不得出现该 WARN**（证明它挂在失败支上，
//!   不是每次高亮都刷屏）。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::ProcessDefine;
use jeeflow_core::spi::{ProcessExtRepository, ProcessRepository};
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::Arc;

const CHILD_ENV: &str = "I162_CHILD";
const TEST_NAME: &str = "test_i162_highlight_degrade_warns_with_instance_define_and_reason";

fn facade_mem() -> (JeeflowFacade, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
    (JeeflowFacade::new(ctx), repo)
}

/// 起一单 01-simple（共享夹具，八语言同一份）：startAndExecute 自动跑完 apply ⇒ 停在 task1。
/// broken=true 时把定义 content 换成坏 JSON（直写仓储，绕开 deploy 的解析腿——那条腿本来就会拒）。
async fn run_scenario(broken: bool) -> (i64, i64) {
    let (facade, repo) = facade_mem();
    let content = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../flows/01-simple.json"),
    )
    .expect("读取共享夹具 01-simple.json");
    let mut define = ProcessDefine {
        id: 0,
        name: "simple".into(),
        display_name: "简单审批流程".into(),
        define_type: "approval".into(),
        state: 1,
        content: content.as_bytes().to_vec(),
        version: 1,
        create_time: None,
        create_user: None,
        update_time: None,
        update_user: None,
    };
    facade.repo().save_define(&mut define).unwrap();
    let did = define.id;

    let mut start_args: HashMap<String, Json> = HashMap::new();
    start_args.insert("processDefineId".into(), json!(did));
    start_args.insert("operator".into(), json!("zhangsan"));
    let started = facade.flow("processInstance/startAndExecute", &start_args).await;
    assert_eq!(started["code"], 0, "发起失败: {started}");
    let iid = started["data"]["processInstanceId"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap();

    if broken {
        let mut def = repo
            .find_define_by_id(did)
            .unwrap()
            .expect("前置：定义取得到");
        def.content = b"{ \"cells\": [ this is broken json".to_vec();
        repo.save_define(&mut def).unwrap();
    }

    let mut hl_args: HashMap<String, Json> = HashMap::new();
    hl_args.insert("id".into(), json!(iid));
    let hl = facade.flow("processInstance/highLight", &hl_args).await;
    assert_eq!(
        hl["code"], 0,
        "162 口径＝降级面保留（坏定义不该让整条 highLight 变红）: {hl}"
    );

    let active: Vec<&str> = hl["data"]["activeNodeNames"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    assert!(
        active.contains(&"task1"),
        "降级只许影响模型补全腿，活跃任务行仍须是 task1，实得 {active:?}"
    );

    let edges: Vec<&str> = hl["data"]["historyEdgeNames"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    if broken {
        assert!(edges.is_empty(), "降级档边集应为空，实得 {edges:?}");
        assert_eq!(
            hl["data"]["nodeProgress"],
            json!({}),
            "降级档 nodeProgress 应为空对象: {hl}"
        );
    } else {
        assert!(
            !edges.is_empty(),
            "好定义下补全腿应产出边集（本档若空＝夹具没走到模型腿，②档自证失效）: {hl}"
        );
    }

    (iid, did)
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
async fn test_i162_highlight_degrade_warns_with_instance_define_and_reason() {
    match std::env::var(CHILD_ENV).as_deref() {
        Ok("broken") => {
            let (iid, did) = run_scenario(true).await;
            // 父进程要拿真号来钉日志三件，这里把 ids 送到 stdout
            println!("I162_IDS iid={iid} did={did}");
        }
        Ok("good") => {
            run_scenario(false).await;
            println!("I162_IDS good-leg-done");
        }
        _ => {
            // ── 父进程腿 ①：坏定义 ⇒ 日志三件齐（子进程 stderr 真捕获）──
            let (stdout, stderr, ok) = rerun_child("broken");
            assert!(
                ok,
                "子进程须整套跑绿（场景＋断言都在子进程里）: stdout={stdout} stderr={stderr}"
            );
            assert!(
                stdout.contains("1 passed"),
                "自证管道：子进程得真跑到这枚测试（防 --exact 名字漂了 0 用例静默绿）: {stdout}"
            );
            let iid = field_of(&stdout, "iid");
            let did = field_of(&stdout, "did");
            assert!(
                stderr.contains("模型补全腿解码失败"),
                "降级必须落一条可诊断日志（静默通道就是本案要收的那条），实得 stderr: {stderr}"
            );
            assert!(
                stderr.contains(&format!("instanceId={iid}"))
                    && stderr.contains(&format!("defineId={did}"))
                    && stderr.contains("err="),
                "日志要带可归因的三件（实例/定义号取对/原因，缺一件等于没落），实得 stderr: {stderr}（期望 iid={iid} did={did}）"
            );

            // ── 父进程腿 ②：好定义 ⇒ 不得出现该 WARN（挂在失败支上，不是每次高亮都刷屏）──
            let (stdout2, stderr2, ok2) = rerun_child("good");
            assert!(ok2, "好定义腿子进程须跑绿: stdout={stdout2} stderr={stderr2}");
            assert!(
                stdout2.contains("1 passed"),
                "自证管道：好定义腿子进程得真跑到这枚测试: {stdout2}"
            );
            assert!(
                !stderr2.contains("模型补全腿解码失败"),
                "正常路径不该出现降级日志: {stderr2}"
            );
        }
    }
}
