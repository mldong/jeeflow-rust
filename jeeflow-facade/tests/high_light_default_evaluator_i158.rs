//! issues/158 · 门面 `highLight` 的决策边求值必须与引擎运行时**同一条求值通道**（rust 栈）。
//!
//! 立案事实（活栈实测）：跨栈门禁 `L2-39` 打在 salvo 生产镜像上 47/1，唯一红是档③——
//! 实例确实走了 `e_dec_yes`（`nodeProgress` 里有 `task2`、`nodes` 里有 `decision1`），
//! 但 `historyEdgeNames` 缺 `e_dec_yes`；同一枚夹具打在 csharp 上 48/0 PASS。
//!
//! 两栈的差别只有一处，且**不在集成壳**：salvo 与 csharp 的壳都没注册 `IExpressionEvaluator`
//! （`mldong-csharp-jeeflow` 全仓对 evaluator 零命中，`mldong-salvo-jeeflow/src/modules/wf/core/wf_factory.rs`
//! 的 `ServiceContext` 链同样没有 `with_expression_evaluator`）。csharp 过是因为引擎自带默认求值器
//! `DefaultExpressionEvaluator`，并由 `ServiceContext.ExpressionEvaluatorOrDefault` 让**运行时与门面共用它**；
//! rust 的引擎也有内置求值（`jeeflow-core/src/engine.rs` 的 `simple_eval`），但只有运行时那条腿走，
//! 门面的 `eval_decision_expr` 在 SPI 为 `None` 时整档判 false ⇒ 同一个实例，运行时"走了"、门面"没走"。
//!
//! 本文件钉两件事（都是**没有 SPI** 的这一档，逐格自证夹具形状，不用线性流）：
//! ① 门面必须用引擎内置默认求值器，把"通向活跃节点那一跳"的决策边收进来（`e3`）；
//! ② 内置默认求值器本身必须做**变量代入 ＋ 数值优先比较**。rust 旧内置只代入 `${var}`/`#var`，
//!    裸变量名不代入 ⇒ `amount > 1000` 是靠 `"amount".cmp("1000")`＝Greater **蒙对**方向的，
//!    而 `amount <= 1000` 同一条比较返回 `Greater != Less`＝**也为 true**。
//!    所以 ① 单独改门面会让两条 expr 边一起亮（＝门禁档③ 的另一半、也即 issues/159 照 moon 的那个形状）。
//!
//! 判据基准＝spec/06 §4.6 义务 2；求值器语义基准＝jeeflow-csharp `DefaultExpressionEvaluator`
//! （其自身以 PHP `WfExpressionEvaluator` 为最小基准不超集：变量代入＋比较运算＋布尔字面量，**不含 `&&`/`||`**）。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::spi::{ProcessExtRepository, ProcessRepository};
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::Arc;

/// 与 salvo 壳逐字同形的装配：只有仓储＋id 生成器，**不注册表达式 SPI**。
fn facade_like_salvo() -> (JeeflowFacade, Arc<MemoryRepository>) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
    (JeeflowFacade::new(ctx), repo)
}

/// 读仓内共享夹具（八语言同一份，编辑源在 jeeflow-java，本仓 `flows/` 是镜像副本）。
/// 夹具形状：`start → apply(applicant) → task1(leader) → decision1 →(e3 amount>1000) task2(manager)`
/// 与 `decision1 →(e4 amount<=1000) task3(director) → end`。
fn load_shared_flow(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../flows")
        .join(format!("{name}.json"));
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("读取共享夹具失败 {}: {e}", path.display()))
}

async fn deploy(facade: &JeeflowFacade) -> i64 {
    let content = load_shared_flow("03-decision-expr");
    let mut define = jeeflow_core::model::ProcessDefine {
        id: 0,
        name: "decision-expr-i158".into(),
        display_name: "决策表达式流程（158）".into(),
        define_type: "approval".into(),
        state: 1,
        content: content.as_bytes().to_vec(),
        version: 1,
        create_time: None,
        create_user: Some("applicant".into()),
        update_time: None,
        update_user: None,
    };
    facade.repo().save_define(&mut define).unwrap();
    define.id
}

async fn start_and_do_task1(facade: &JeeflowFacade, repo: &Arc<MemoryRepository>, define_id: i64, amount: i64) -> i64 {
    let mut start_args: HashMap<String, Json> = HashMap::new();
    start_args.insert("processDefineId".into(), json!(define_id));
    start_args.insert("operator".into(), json!("applicant"));
    start_args.insert("amount".into(), json!(amount));
    let started = facade.flow("processInstance/startAndExecute", &start_args).await;
    assert_eq!(started["code"], 0, "发起失败: {started}");
    let iid = started["data"]["processInstanceId"].as_str().unwrap().parse::<i64>().unwrap();

    // 办掉 task1 ⇒ 越过 decision1，实例应落在分支后的那个待办上
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
    iid
}

fn edges_of(hl: &Json) -> Vec<String> {
    hl["data"]["historyEdgeNames"]
        .as_array()
        .expect("historyEdgeNames 应为数组")
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect()
}

fn nodes_of(hl: &Json) -> Vec<String> {
    hl["data"]["historyNodeNames"]
        .as_array()
        .expect("historyNodeNames 应为数组")
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect()
}

async fn high_light(facade: &JeeflowFacade, iid: i64) -> Json {
    let mut args: HashMap<String, Json> = HashMap::new();
    args.insert("id".into(), json!(iid));
    let hl = facade.flow("processInstance/highLight", &args).await;
    assert_eq!(hl["code"], 0, "highLight 应成功: {hl}");
    hl
}

/// ① amount=5000 且**不注册 SPI**：运行时走 `e3` 落在活跃 `task2`，门面必须把这一跳的边名收进来。
#[tokio::test]
async fn i158_facade_uses_engine_default_evaluator_when_host_has_none() {
    let (facade, repo) = facade_like_salvo();
    let define_id = deploy(&facade).await;
    let iid = start_and_do_task1(&facade, &repo, define_id, 5000).await;

    // 夹具形状自证（先钉"停在分支上"，防它哪天退化成"流已走完"的恒真形状）
    let doing: Vec<String> = repo
        .find_doing_tasks(iid, &[])
        .unwrap()
        .iter()
        .map(|t| t.task_name.clone())
        .collect();
    assert_eq!(doing, vec!["task2".to_string()], "实例应停在分支后的活跃 task2");

    let hl = high_light(&facade, iid).await;
    let edges = edges_of(&hl);
    for need in ["e0", "e_apply_1", "e2", "e3"] {
        assert!(edges.contains(&need.to_string()), "走过的边 {need} 必须高亮，实得 {edges:?}");
    }
    assert!(!edges.contains(&"e4".to_string()), "没走的分支边 e4 不得高亮（档③ 另一半），实得 {edges:?}");
    assert!(!edges.contains(&"e5".to_string()), "活跃节点自己的出边 e5 不得高亮，实得 {edges:?}");
    assert!(!nodes_of(&hl).contains(&"task3".to_string()), "未走分支的 task3 不得进节点集（义务 2 节点侧）");
}

/// ② 反方向抽样：同一个内置求值器必须把 `amount <= 1000` 判 false（旧内置靠字符串比较蒙成 true）。
#[tokio::test]
async fn i158_default_evaluator_is_numeric_not_string_guess() {
    let (facade, repo) = facade_like_salvo();
    let define_id = deploy(&facade).await;
    let iid = start_and_do_task1(&facade, &repo, define_id, 500).await;

    let doing: Vec<String> = repo
        .find_doing_tasks(iid, &[])
        .unwrap()
        .iter()
        .map(|t| t.task_name.clone())
        .collect();
    assert_eq!(doing, vec!["task3".to_string()], "amount=500 应走 e4 落在 task3，实得 {doing:?}");

    let hl = high_light(&facade, iid).await;
    let edges = edges_of(&hl);
    assert!(edges.contains(&"e4".to_string()), "求值为 true 的 e4 必须高亮，实得 {edges:?}");
    assert!(!edges.contains(&"e3".to_string()), "求值为 false 的 e3 不得高亮，实得 {edges:?}");
    // e6 是**活跃节点 task3 自己的出边**：参考实现在活跃节点只停下钻也不收它的出边
    // （与 ① 里 `e5` 那一档对称；153 那格之所以有 e6，是因为它把 task3 也办完了）
    assert!(!edges.contains(&"e6".to_string()), "活跃节点自己的出边 e6 不得高亮，实得 {edges:?}");
    assert!(!nodes_of(&hl).contains(&"task2".to_string()), "未走分支的 task2 不得进节点集");
}

// ③ 内置默认求值器自身的真值表在 `jeeflow-core/src/default_evaluator.rs` 的
// `i158_builtin_is_numeric_not_lexicographic`（不借流程，正负对照都在那格）。
