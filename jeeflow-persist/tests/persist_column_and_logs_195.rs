//! issues/195 · audit8 R29 C 案三条真缺陷的判据（rust persist 腿）。
//!
//! 覆盖三档，每档都配"修前会红"的形状：
//! - **④ 列匹配**（spec/09「列匹配（1.6.4）」）：默认宽松归一（驼峰表单键 → 蛇形表列，写入用
//!   表列原名）＋ `with_strict_columns(true)` 严格档逃生门 ＋ 撞列时的确定性胜者规则。
//!   修前只有逐字相等一档 ⇒ 驼峰键**静默丢列**，无报错无日志。
//! - **② 两条静默不落腿**：写了 persistMode 却不认识、字段过滤后空集 ⇒ 各落一条可诊断 WARN
//!   （issues/162 家族形状：纯函数文案＋`eprintln!` 落点）。日志"真拿到"用**自再执行**捕获子进程
//!   stderr（与 `jeeflow-facade/tests/highlight_degrade_log_162.rs` 同法，不接受"注释自称已落日志"），
//!   并配"正常路径不得出现该 WARN"的反向腿（证明它挂在失败支上，不是每次落库都刷屏）。
//! - **③ 时间基准**：persist 审计列改接 core 的 `clock::current_time_str()` 单源出口
//!   （issues/120 路 1）——注入档取宿主基准、未注入档取 core 默认基准（UTC），两腿都不许自取 `chrono::Local`。

use jeeflow_core::clock;
use jeeflow_core::engine::Execution;
use jeeflow_core::json::{FlowData, JsonValue};
use jeeflow_core::model::{ProcessDefine, ProcessInstance};
use jeeflow_core::parser::{NodeModel, NodeType, ProcessModel};
use jeeflow_core::spi::FlowInterceptor;
use jeeflow_persist::{
    column_names_equal, empty_persist_row_warning, find_data_key, normalize_column,
    unknown_persist_mode_warning, FieldMeta, InMemoryMetaProvider, InMemoryTableWriter,
    PersistPostInterceptor, TableMeta,
};
use std::collections::HashMap;
use std::sync::Arc;

const CHILD_ENV: &str = "I195_CHILD";

// ─── 夹具（与 lib.rs 内私有 `make_exec` 同形，集成测试拿不到私有件故复刻一份最小的） ───

fn merchant_meta() -> TableMeta {
    TableMeta::new("merchant_settlement", "结算表")
        .with_field(FieldMeta::new("company_name", "VARCHAR(255)", "公司"))
        .with_field(FieldMeta::new("contact_name", "VARCHAR(100)", "联系人"))
        .with_field(FieldMeta::new("contact_phone", "VARCHAR(20)", "电话"))
        .with_field(FieldMeta::new("process_instance_id", "BIGINT", "实例ID"))
        .with_field(FieldMeta::new("apply_user_id", "BIGINT", "申请人"))
}

/// 一张"与表单键名完全对不上"的表（② 空集腿的夹具）。
fn unrelated_meta() -> TableMeta {
    TableMeta::new("merchant_settlement", "结算表")
        .with_field(FieldMeta::new("zzz_only", "VARCHAR(255)", "无关列"))
}

fn exec(
    node_id: &str,
    node_type: NodeType,
    persist_mode: Option<&str>,
    table: Option<&str>,
    vars: FlowData,
    state: i32,
    submit_type: Option<i64>,
) -> Execution {
    let model = ProcessModel {
        name: "proc".to_string(),
        display_name: "proc".to_string(),
        model_type: "6".to_string(),
        expire_time: None,
        persist_mode: persist_mode.map(|s| s.to_string()),
        rel_table_name: table.map(|s| s.to_string()),
        nodes: vec![NodeModel {
            id: node_id.to_string(),
            node_type,
            display_name: node_id.to_string(),
            properties: HashMap::new(),
        }],
        edges: vec![],
        ..Default::default()
    };
    let instance = ProcessInstance {
        instance_id: 9001,
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
    let mut e = Execution::new(instance, model, define, "user1", {
        let mut args = FlowData::new();
        if let Some(st) = submit_type {
            args.insert_i64("submitType", st);
        }
        args
    });
    e.current_node = Some(e.process_model.nodes[0].clone());
    e
}

fn camel_vars() -> FlowData {
    let mut v = FlowData::new();
    v.insert_str("f_companyName", "Acme");
    v.insert_str("f_contactName", "Bob");
    v.insert_str("f_contact_phone", "13900000000");
    v
}

fn interceptor(
    meta: TableMeta,
    writer: Arc<InMemoryTableWriter>,
    strict: bool,
) -> PersistPostInterceptor {
    let mut provider = InMemoryMetaProvider::new();
    provider.register(meta);
    PersistPostInterceptor::new(Arc::new(provider), writer).with_strict_columns(strict)
}

fn row_of(writer: &InMemoryTableWriter) -> Option<HashMap<String, JsonValue>> {
    writer.get_row_by_key("merchant_settlement", "process_instance_id", 9001)
}

// ─── ④ 列匹配 ───

/// 尺子本体两档（逐字照 java `normalizeColumn` / `findColumn`）。
#[test]
fn test_195_column_name_matching_rules() {
    assert_eq!(normalize_column("companyName"), "companyname");
    assert_eq!(normalize_column("COMPANY_NAME"), "companyname");
    assert!(column_names_equal("companyName", "company_name", false));
    assert!(column_names_equal("ContactPhone", "contact_phone", false));
    // 严格档＝忽略大小写的逐字相等 ⇒ 下划线差异不再被容忍
    assert!(column_names_equal("COMPANY_NAME", "company_name", true));
    assert!(!column_names_equal("companyName", "company_name", true));
}

/// ④ 正向：SYNC 发起 INSERT，驼峰表单键落到蛇形表列，且**写入用表列原名**（spec/09 判据表）。
/// 修前此条为红：`filter_editable` 只 `data.get(&column_name)` ⇒ 驼峰两列被静默丢弃。
#[test]
fn test_195_sync_camel_key_lands_on_snake_column() {
    let writer = Arc::new(InMemoryTableWriter::new());
    let itc = interceptor(merchant_meta(), writer.clone(), false);
    let mut e = exec("start", NodeType::Start, Some("SYNC"), Some("merchant_settlement"), camel_vars(), 10, None);
    itc.intercept(&mut e).unwrap();

    let row = row_of(&writer).expect("驼峰键应按宽松归一落到蛇形列");
    assert_eq!(row.get("company_name").and_then(|v| v.as_str()), Some("Acme"));
    assert_eq!(row.get("contact_name").and_then(|v| v.as_str()), Some("Bob"));
    assert_eq!(row.get("contact_phone").and_then(|v| v.as_str()), Some("13900000000"));
    // 不许把驼峰原键当列名写出去
    assert!(!row.contains_key("companyName"));
    assert!(!row.contains_key("f_companyName"));
}

/// ④ 负向：严格档只认逐字（忽略大小写）相等 ⇒ 驼峰键不落，蛇形键照落。
#[test]
fn test_195_strict_column_mode_keeps_verbatim_only() {
    let writer = Arc::new(InMemoryTableWriter::new());
    let itc = interceptor(merchant_meta(), writer.clone(), true);
    let mut e = exec("start", NodeType::Start, Some("SYNC"), Some("merchant_settlement"), camel_vars(), 10, None);
    itc.intercept(&mut e).unwrap();

    let row = row_of(&writer).expect("严格档下 f_contact_phone 仍应落库");
    assert_eq!(row.get("contact_phone").and_then(|v| v.as_str()), Some("13900000000"));
    assert!(!row.contains_key("company_name"), "严格档不该收驼峰键: {row:?}");
    assert!(!row.contains_key("contact_name"), "严格档不该收驼峰键: {row:?}");
}

/// ④ 确定性：同一列被两个键命中（逐字蛇形 ＋ 驼峰同义）时，**逐字相等者优先**。
/// 这一档保证"键本来就等于列名"的既有形状与修前逐字节一致（回归腿），且 rust `HashMap`
/// 无插入序 ⇒ 胜者必须确定，否则同一份 data 两次运行可换值。
#[test]
fn test_195_verbatim_key_wins_when_both_forms_present() {
    let mut data = HashMap::new();
    data.insert("company_name".to_string(), JsonValue::Str("SNAKE".to_string()));
    data.insert("companyName".to_string(), JsonValue::Str("CAMEL".to_string()));
    assert_eq!(find_data_key(&data, "company_name", false).map(String::as_str), Some("company_name"));

    let writer = Arc::new(InMemoryTableWriter::new());
    let itc = interceptor(merchant_meta(), writer.clone(), false);
    let mut vars = FlowData::new();
    vars.insert_str("f_company_name", "SNAKE");
    vars.insert_str("f_companyName", "CAMEL");
    let mut e = exec("start", NodeType::Start, Some("SYNC"), Some("merchant_settlement"), vars, 10, None);
    itc.intercept(&mut e).unwrap();
    assert_eq!(row_of(&writer).unwrap().get("company_name").and_then(|v| v.as_str()), Some("SNAKE"));
}

// ─── ③ 时间基准 ───

/// ③ 正向：注入时钟 ⇒ persist 审计列取**宿主基准**（修前自取 `chrono::Local` ⇒  sentinel 对不上）。
#[test]
fn test_195_system_time_follows_injected_clock() {
    let _scope = clock::ClockScope::injected(|| "2026-01-02 03:04:05".to_string());
    let writer = Arc::new(InMemoryTableWriter::new());
    let itc = interceptor(merchant_meta(), writer.clone(), false);
    let mut vars = FlowData::new();
    vars.insert_str("f_company_name", "Acme");
    let mut e = exec("start", NodeType::Start, Some("SYNC"), Some("merchant_settlement"), vars, 10, None);
    itc.intercept(&mut e).unwrap();

    let row = row_of(&writer).unwrap();
    assert_eq!(row.get("create_time").and_then(|v| v.as_str()), Some("2026-01-02 03:04:05"));
    assert_eq!(row.get("update_time").and_then(|v| v.as_str()), Some("2026-01-02 03:04:05"));
}

/// ③ 回归：未注入 ⇒ 与 core 默认基准同尺（`clock::current_time_str()` 的 UTC 档）。
/// 若本方法回到 `chrono::Local`，这里会差出整时区偏移（本机 UTC+8）而红。
#[test]
fn test_195_system_time_falls_back_to_core_default_clock() {
    let _guard = clock::lock_scope();
    let writer = Arc::new(InMemoryTableWriter::new());
    let itc = interceptor(merchant_meta(), writer.clone(), false);
    let mut vars = FlowData::new();
    vars.insert_str("f_company_name", "Acme");
    let mut e = exec("start", NodeType::Start, Some("SYNC"), Some("merchant_settlement"), vars, 10, None);

    let before = clock::current_time_str();
    itc.intercept(&mut e).unwrap();
    let after = clock::current_time_str();

    let got = row_of(&writer).unwrap();
    let got = got.get("create_time").and_then(|v| v.as_str()).unwrap_or_default().to_string();
    assert!(got == before || got == after, "审计列须落在 core 默认基准的两拍之间: got={got} before={before} after={after}");
}

// ─── ② 两条静默不落腿 ⇒ 可诊断 WARN ───

/// 自再执行：把当前测试二进制以 `--exact <本测试> --nocapture` 拉起来当子进程，
/// 子进程跑场景、`eprintln!` 落到它真实的 stderr，父进程整段捕获（进程内 Rust 没有
/// 捕获 stderr 的稳定 API）。返回 (stdout, stderr, 是否跑绿)。
fn rerun_child(mode: &str, test_name: &str) -> (String, String, bool) {
    let out = std::process::Command::new(std::env::current_exe().expect("current_exe"))
        .args(["--exact", test_name, "--nocapture", "--test-threads=1"])
        .env(CHILD_ENV, mode)
        .output()
        .expect("子进程测试应可拉起");
    (
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
        out.status.success(),
    )
}

/// ② 未知 persistMode：不落库（流转不受影响）＋ WARN 三件齐（实得串／表名／后果）。
#[test]
fn test_195_unknown_persist_mode_warns_and_skips() {
    const TEST: &str = "test_195_unknown_persist_mode_warns_and_skips";
    match std::env::var(CHILD_ENV).as_deref() {
        // 子进程 ①：拼错的模式串（ARCHIVE + 尾空格 trim 后仍不认识）
        Ok("typo") => {
            let writer = Arc::new(InMemoryTableWriter::new());
            let itc = interceptor(merchant_meta(), writer.clone(), false);
            let mut vars = FlowData::new();
            vars.insert_str("f_company_name", "Acme");
            // 结束＋FINISHED＋同意：模式认得的话这一腿必然落库
            let mut e = exec("end", NodeType::End, Some("ARCHVIE"), Some("merchant_settlement"), vars, 20, Some(1));
            itc.intercept(&mut e).unwrap();
            assert!(row_of(&writer).is_none(), "未知模式不应落库（本条只加日志，不改落库裁定）");
            println!("I195_CHILD_DONE typo");
        }
        // 子进程 ②：正常模式 ⇒ 落库成功
        Ok("ok") => {
            let writer = Arc::new(InMemoryTableWriter::new());
            let itc = interceptor(merchant_meta(), writer.clone(), false);
            let mut vars = FlowData::new();
            vars.insert_str("f_company_name", "Acme");
            let mut e = exec("end", NodeType::End, Some("ARCHIVE"), Some("merchant_settlement"), vars, 20, Some(1));
            itc.intercept(&mut e).unwrap();
            assert!(row_of(&writer).is_some(), "正常模式应落库（夹具自证）");
            println!("I195_CHILD_DONE ok");
        }
        // 父进程
        _ => {
            let (stdout, stderr, ok) = rerun_child("typo", TEST);
            assert!(ok, "子进程 typo 腿须跑绿: stdout={stdout} stderr={stderr}");
            assert!(stdout.contains("1 passed"), "自证管道：子进程得真跑到这枚测试（防名字漂了 0 用例静默绿）: {stdout}");
            let expect = unknown_persist_mode_warning("ARCHVIE", "merchant_settlement");
            assert!(stderr.contains(&expect), "未知 persistMode 必须留可诊断 WARN，实得 stderr:\n{stderr}\n期望含:\n{expect}");
            for piece in ["WARN", "ARCHVIE", "merchant_settlement", "不落库"] {
                assert!(expect.contains(piece), "WARN 文案自证四件齐（缺件＝日志不可归因）: {piece}");
            }

            let (stdout2, stderr2, ok2) = rerun_child("ok", TEST);
            assert!(ok2, "子进程 ok 腿须跑绿: stdout={stdout2} stderr={stderr2}");
            assert!(stdout2.contains("1 passed"), "自证管道：ok 腿子进程得真跑到这枚测试: {stdout2}");
            assert!(!stderr2.contains("persistMode 不在模式表内"), "正常路径不该出现该 WARN（它挂在未知档支上）: {stderr2}");
        }
    }
}

/// ② 过滤后空集：不落库＋ WARN（表名／模式／后果）。
#[test]
fn test_195_empty_filtered_row_warns_and_skips() {
    const TEST: &str = "test_195_empty_filtered_row_warns_and_skips";
    match std::env::var(CHILD_ENV).as_deref() {
        // 子进程 ①：表列与表单键名完全对不上 ⇒ 空集
        Ok("empty") => {
            let writer = Arc::new(InMemoryTableWriter::new());
            let itc = interceptor(unrelated_meta(), writer.clone(), false);
            let mut vars = FlowData::new();
            vars.insert_str("f_company_name", "Acme");
            let mut e = exec("start", NodeType::Start, Some("SYNC"), Some("merchant_settlement"), vars, 10, None);
            itc.intercept(&mut e).unwrap();
            assert!(row_of(&writer).is_none(), "空集不应落库（本条只加日志）");
            println!("I195_CHILD_DONE empty");
        }
        // 子进程 ②：对得上 ⇒ 落库
        Ok("match") => {
            let writer = Arc::new(InMemoryTableWriter::new());
            let itc = interceptor(merchant_meta(), writer.clone(), false);
            let mut vars = FlowData::new();
            vars.insert_str("f_company_name", "Acme");
            let mut e = exec("start", NodeType::Start, Some("SYNC"), Some("merchant_settlement"), vars, 10, None);
            itc.intercept(&mut e).unwrap();
            assert!(row_of(&writer).is_some(), "正常形状应落库（夹具自证）");
            println!("I195_CHILD_DONE match");
        }
        _ => {
            let (stdout, stderr, ok) = rerun_child("empty", TEST);
            assert!(ok, "子进程 empty 腿须跑绿: stdout={stdout} stderr={stderr}");
            assert!(stdout.contains("1 passed"), "自证管道：子进程得真跑到这枚测试: {stdout}");
            let expect = empty_persist_row_warning("merchant_settlement", "SYNC");
            assert!(stderr.contains(&expect), "空集必须留可诊断 WARN，实得 stderr:\n{stderr}\n期望含:\n{expect}");
            for piece in ["WARN", "merchant_settlement", "SYNC", "不落库"] {
                assert!(expect.contains(piece), "WARN 文案自证四件齐（缺件＝日志不可归因）: {piece}");
            }

            let (stdout2, stderr2, ok2) = rerun_child("match", TEST);
            assert!(ok2, "子进程 match 腿须跑绿: stdout={stdout2} stderr={stderr2}");
            assert!(stdout2.contains("1 passed"), "自证管道：match 腿子进程得真跑到这枚测试: {stdout2}");
            assert!(!stderr2.contains("无可写字段"), "落库成功时不该出现空集 WARN: {stderr2}");
        }
    }
}

/// ② 第三条腿（trim 档）：`"ARCHIVE "` 尾空格经 trim 后**认得**，既不落 WARN 也照常落库。
/// 这条钉的是"尾空格不该整单不落"这一后果面（java 靠非 SYNC 兜底臂容下它，本栈靠 trim）。
#[test]
fn test_195_trailing_space_mode_persists() {
    let writer = Arc::new(InMemoryTableWriter::new());
    let itc = interceptor(merchant_meta(), writer.clone(), false);
    let mut vars = FlowData::new();
    vars.insert_str("f_company_name", "Acme");
    let mut e = exec("end", NodeType::End, Some("  ARCHIVE  "), Some("merchant_settlement"), vars, 20, Some(1));
    itc.intercept(&mut e).unwrap();
    assert_eq!(row_of(&writer).unwrap().get("company_name").and_then(|v| v.as_str()), Some("Acme"));
}
