//! issues/141 G2 ＋ issues/139（rust 腿，门面出口这一支）。
//!
//! **G2 · 手动抄送腿只 fire 实际新建的子集**
//! 立法＝spec 06 §4「写侧判重＝幂等空操作」＋ spec 11.2 原则 1「码值表达发生了什么事实」。
//! 同一 `(实例, 被抄送人)` 已有 cc 行时再次抄送：①不新增行 ②不重置未读 ③不更新原行时间
//! ④**不发 CC_CREATE(4)**。三条入口（发起 `f_ccActors`／办理 `tf_ccActors`／门面手动
//! `processInstance/createCCInstance`）共用同一条判据（spec §11.7），本文件打的是手动那条。
//! 引擎两条腿的同判据用例在 `jeeflow-core/src/engine.rs`（`test_i141_g2_*`），
//! 仓储两仓（内存／sqlx 真库）那一支在 `jeeflow-core/src/memory.rs`（`cc_i141_tests`）与
//! `jeeflow-repository-sqlx`（`cc_i141_*`）。
//!
//! **issues/139 · 解析失败的对外 msg 用 java 逐字原文**
//! 旧形状 `JeeflowError::ParseError(format!("JSON parse error: {}", e))` 既拼底层 serde/解析器
//! 文本又是英文，经门面 `error_response(&e.message())` 逐字透出到 `msg`（违反 121
//! 「内部文案不进 msg」那条口径）。基准＝jeeflow-java `ModelParser.java`
//! `throw new RuntimeException("读取流程定义 JSON 失败", e)`：**msg 只有这一句**，
//! 原始异常留在错误链上（本仓经 [`std::error::Error::source`]）。
//! 门面这边钉的是"三条腿共用一个收口"：deploy／redeploy／designRedeploy 的 msg 都来自
//! `ModelParser::parse`，改一处即三处同答案（改前实测三处都带英文底层文本）。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::event::{ProcessEvent, ProcessEventType};
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::spi::{ProcessEventListener, ProcessExtRepository, ProcessRepository};
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const OPERATOR: &str = "i141_sender";
const ACTOR_A: &str = "i141_actor_a";
const ACTOR_B: &str = "i141_actor_b";
const ACTOR_C: &str = "i141_actor_c";
/// java 逐字原文（`ModelParser.java` 的 RuntimeException message）。
const PARSE_FAIL_MSG: &str = "读取流程定义 JSON 失败";

#[derive(Default)]
struct CcCapture {
    events: Mutex<Vec<(i64, Option<String>)>>,
}
impl ProcessEventListener for CcCapture {
    fn on_event(&self, event: &ProcessEvent) {
        if event.event_type == ProcessEventType::CcCreate {
            self.events.lock().unwrap().push((event.source_id, event.cc_actor_id.clone()));
        }
    }
}

fn facade_with_capture() -> (JeeflowFacade, Arc<MemoryRepository>, Arc<CcCapture>) {
    let repo = Arc::new(MemoryRepository::new());
    let mut ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
    let capture = Arc::new(CcCapture::default());
    ctx.register_event_listener(capture.clone());
    (JeeflowFacade::new(ctx), repo, capture)
}

async fn manual_cc(facade: &JeeflowFacade, iid: i64, actors: &[&str]) -> Json {
    let mut args: HashMap<String, Json> = HashMap::new();
    args.insert("processInstanceId".into(), json!(iid));
    args.insert("operator".into(), json!(OPERATOR));
    args.insert("actorIds".into(), json!(actors));
    let resp = facade.flow("processInstance/createCCInstance", &args).await;
    assert_eq!(resp["code"], 0, "手动抄送应成功：{resp}");
    resp
}

fn fired(capture: &Arc<CcCapture>) -> Vec<String> {
    capture.events.lock().unwrap().iter()
        .map(|(_, a)| a.clone().unwrap_or_default()).collect()
}

/// 手动腿正向对照：全新的一批照旧逐人建行、逐人 fire（本轮只收"重复"那一档，不动这里）。
#[tokio::test]
async fn test_i141_g2_manual_leg_first_batch_still_creates_and_fires() {
    let (facade, repo, capture) = facade_with_capture();
    manual_cc(&facade, 9001, &[ACTOR_A, ACTOR_B]).await;

    assert_eq!(fired(&capture), vec![ACTOR_A.to_string(), ACTOR_B.to_string()],
        "全新抄送应逐人 fire CC_CREATE");
    assert_eq!(repo.find_cc_actor_ids(9001).unwrap(),
        vec![ACTOR_A.to_string(), ACTOR_B.to_string()]);
}

/// ①＋④：同一批人连抄两次 ⇒ 第二趟不新增行、**一支码 4 都不发**
/// （改前手动腿照旧按原始请求全量 fire，实测多出发 A/B 两支）。
#[tokio::test]
async fn test_i141_g2_manual_leg_repeat_fires_nothing() {
    let (facade, repo, capture) = facade_with_capture();
    manual_cc(&facade, 9002, &[ACTOR_A]).await;
    assert_eq!(fired(&capture), vec![ACTOR_A.to_string()]);
    let before = repo.find_cc_actor_ids(9002).unwrap();

    capture.events.lock().unwrap().clear();
    manual_cc(&facade, 9002, &[ACTOR_A]).await;

    assert!(fired(&capture).is_empty(),
        "子集为空 ⇒ 整支不 fire（spec 11.2 原则 1「码=事实」），实得 {:?}", fired(&capture));
    assert_eq!(repo.find_cc_actor_ids(9002).unwrap(), before, "①重复抄送不得新增行");
    assert_eq!(before, vec![ACTOR_A.to_string()]);
}

/// ④的子集档：第二次给「已知人＋新人」⇒ 只为新人建行、只为新人 fire（新人顺序随入参）。
#[tokio::test]
async fn test_i141_g2_manual_leg_fires_only_new_subset() {
    let (facade, repo, capture) = facade_with_capture();
    manual_cc(&facade, 9003, &[ACTOR_A, ACTOR_B]).await;
    capture.events.lock().unwrap().clear();

    manual_cc(&facade, 9003, &[ACTOR_A, ACTOR_C]).await;

    assert_eq!(fired(&capture), vec![ACTOR_C.to_string()],
        "逐人 fire 的入参应是实际新建的子集，已知人 A 不得混进去");
    assert_eq!(repo.find_cc_actor_ids(9003).unwrap(),
        vec![ACTOR_A.to_string(), ACTOR_B.to_string(), ACTOR_C.to_string()],
        "落库只多 C 那一行");
}

/// 同一次调用里重复给同一个人 ⇒ 折叠成一行一次提醒（手动腿入参是数组，最容易踩这一档）。
#[tokio::test]
async fn test_i141_g2_manual_leg_dup_within_one_call_collapses() {
    let (facade, repo, capture) = facade_with_capture();
    manual_cc(&facade, 9004, &[ACTOR_A, ACTOR_A]).await;

    assert_eq!(fired(&capture), vec![ACTOR_A.to_string()], "同一次调用内的重复只 fire 一次");
    assert_eq!(repo.find_cc_actor_ids(9004).unwrap(), vec![ACTOR_A.to_string()]);
}

/// 反向哨兵：判重按实例作用域——换个实例，同一个人照旧建行照旧 fire。
#[tokio::test]
async fn test_i141_g2_manual_leg_dedup_is_scoped_to_instance() {
    let (facade, repo, capture) = facade_with_capture();
    manual_cc(&facade, 9005, &[ACTOR_A]).await;
    manual_cc(&facade, 9006, &[ACTOR_A]).await;

    assert_eq!(fired(&capture), vec![ACTOR_A.to_string(), ACTOR_A.to_string()],
        "两个实例各 fire 一次");
    assert_eq!(repo.find_cc_actor_ids(9005).unwrap(), vec![ACTOR_A.to_string()]);
    assert_eq!(repo.find_cc_actor_ids(9006).unwrap(), vec![ACTOR_A.to_string()]);
}

/// ②＋③的手面门面档：已读后再抄同一人 ⇒ state 与时间列都不动（走 ccList 看不到的那一列，
/// 直连仓储断）。
#[tokio::test]
async fn test_i141_g2_manual_leg_keeps_state_and_times() {
    let (facade, repo, _capture) = facade_with_capture();
    manual_cc(&facade, 9007, &[ACTOR_A]).await;

    let mut a: HashMap<String, Json> = HashMap::new();
    a.insert("processInstanceId".into(), json!(9007));
    a.insert("operator".into(), json!(ACTOR_A));
    assert_eq!(facade.flow("processInstance/updateCCStatus", &a).await["code"], 0);

    let before = repo.find_cc_actor_ids(9007).unwrap();
    manual_cc(&facade, 9007, &[ACTOR_A]).await;
    assert_eq!(repo.find_cc_actor_ids(9007).unwrap(), before, "重复抄送后人员集合不变");
}

// ─────────── issues/139 · 解析失败 msg 逐字＝java 原文 ───────────

async fn deploy_garbage(action: &str) -> Json {
    let (facade, _repo, _capture) = facade_with_capture();
    let mut args: HashMap<String, Json> = HashMap::new();
    args.insert("content".into(), json!("{ 这不是合法的流程定义 JSON "));
    args.insert("operator".into(), json!("i139_test"));
    if action == "processDefine/redeploy" {
        args.insert("processDefineId".into(), json!(1));
    }
    facade.flow(action, &args).await
}

/// 正向：`processDefine/deploy` 的解析失败出口 msg 只有 java 那一句，
/// 既不带英文也不拼底层解析器文本（改前实测 `解析错误: JSON parse error: …`）。
#[tokio::test]
async fn test_i139_deploy_failure_msg_is_java_verbatim() {
    let resp = deploy_garbage("processDefine/deploy").await;
    assert_eq!(resp["code"], 99999999, "解析失败仍走业务失败码：{resp}");
    assert_eq!(resp["msg"], PARSE_FAIL_MSG,
        "对外 msg 必须逐字＝java 原文，底层异常不得拼进来：实得 {:?}", resp["msg"]);
    let msg = resp["msg"].as_str().unwrap_or_default();
    assert!(!msg.contains("JSON parse error"), "msg 里不许有底层英文原文：{msg}");
    assert!(!msg.contains("解析错误"), "msg 里不许有包装前缀：{msg}");
}

/// 三条腿共用一个收口：`processDefine/redeploy` 同判据（go 那边点名的 deploy/redeploy/designRedeploy
/// 三条腿在 rust 都走 `ModelParser::parse`，改一处三处同答案）。
#[tokio::test]
async fn test_i139_redeploy_failure_msg_same_verbatim() {
    let resp = deploy_garbage("processDefine/redeploy").await;
    assert_eq!(resp["code"], 99999999, "{resp}");
    assert_eq!(resp["msg"], PARSE_FAIL_MSG, "redeploy 腿与 deploy 腿同一条 msg：实得 {:?}", resp["msg"]);
}
