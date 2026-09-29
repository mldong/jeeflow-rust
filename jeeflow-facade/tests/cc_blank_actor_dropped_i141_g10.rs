//! issues/141 G10 · 空抄送人不建 cc 行（**门面手动腿**这一支，rust 栈）。
//!
//! 立法逐字依据＝spec 06-facade.md §2.10（基准＝jeeflow-java `5fbd5ac`）：
//! 三条入口（发起 `f_ccActors`／办理 `tf_ccActors`／门面手动 `processInstance/createCCInstance`）
//! 解析抄送人集合时，**空串、纯空白、数组里的空元素一律丢弃**；丢完为空 ⇒ 不建任何 cc 行、
//! 也**不 fire `CC_CREATE`(码 4)**；逗号串与数组两种形态必须同判据；落库与比较值取 **trim 后的串**
//! （与 issues/141 G2 写侧判重咬合）；手动腿丢完为空时**与本仓既有的"空集合"档同判**
//! （沿用既有 `actorIds 缺失` 文案，不新造错误码/语义）。
//!
//! rust 的旧形状（普查读数）：门面 `arg_actor_ids` 的**数组腿**只做 `filter(!is_empty())`、
//! **不 trim** ⇒ `["   "]`／`["\t"]` 这类"全空白抄送人"真落一条 `actor_id='   '` 的行并照旧 fire，
//! 而同一支的逗号串腿早就 trim＋丢空——两条腿两个答案。引擎两条腿的同判据用例在
//! `jeeflow-core/src/engine.rs`（`test_i141_g10_*`），两仓写侧兜底在
//! `jeeflow-core/src/memory.rs`（`cc_i141_tests`）与 `jeeflow-repository-sqlx`（真库）。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::event::{ProcessEvent, ProcessEventType};
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::spi::{ProcessEventListener, ProcessExtRepository, ProcessRepository};
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

const OPERATOR: &str = "i141g10_sender";
/// 本仓既有的"空集合"档文案（G10 要求手动腿丢完为空时与它同判，不许新造）。
const EMPTY_SET_MSG: &str = "actorIds 缺失";

#[derive(Default)]
struct CcCapture {
    events: Mutex<Vec<Option<String>>>,
}
impl ProcessEventListener for CcCapture {
    fn on_event(&self, event: &ProcessEvent) {
        if event.event_type == ProcessEventType::CcCreate {
            self.events.lock().unwrap().push(event.cc_actor_id.clone());
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

/// 手动腿原始返回（全空白档要看它是否与"空集合"同档，不能假定成功）。
async fn manual_cc_raw(facade: &JeeflowFacade, iid: i64, actors: Json) -> Json {
    let mut args: HashMap<String, Json> = HashMap::new();
    args.insert("processInstanceId".into(), json!(iid));
    args.insert("operator".into(), json!(OPERATOR));
    args.insert("actorIds".into(), actors);
    facade.flow("processInstance/createCCInstance", &args).await
}

async fn manual_cc(facade: &JeeflowFacade, iid: i64, actors: Json) -> Json {
    let resp = manual_cc_raw(facade, iid, actors).await;
    assert_eq!(resp["code"], 0, "手动抄送应成功：{resp}");
    resp
}

fn fired(capture: &Arc<CcCapture>) -> Vec<String> {
    capture.events.lock().unwrap().iter()
        .map(|a| a.clone().unwrap_or_default()).collect()
}

// ═══ 正向对照 ═══

/// 非空抄送人照旧逐人建行＋逐人 fire（这一格按设计改前也不红，钉的是归一不吃正常值）。
#[tokio::test]
async fn test_i141_g10_manual_leg_positive_control_unchanged() {
    let (facade, repo, capture) = facade_with_capture();
    manual_cc(&facade, 9101, json!(["7501", "7502"])).await;

    assert_eq!(repo.find_cc_actor_ids(9101).unwrap(), vec!["7501".to_string(), "7502".to_string()],
        "正向对照：非空抄送人照旧逐人落行");
    assert_eq!(fired(&capture), vec!["7501".to_string(), "7502".to_string()],
        "正向对照：照旧逐人 fire 码 4");
}

// ═══ 全空白 ⇒ 与"空集合"同档 ═══

/// 全空白集合 ⇒ 零行、零 fire，并且**与空集合返回逐字同档**（spec §2.10 实现要求③）。
/// 改前实测：`["   "]`／`["\t"]` 返回 `{code:0,msg:"成功"}` 并落一条 `actor_id='   '` 的行。
#[tokio::test]
async fn test_i141_g10_all_blank_manual_leg_same_bucket_as_empty_set() {
    for actors in [json!([""]), json!(["   "]), json!(["\t"]), json!(["", "  ", "\t"]),
                   json!("  "), json!(" , , ")] {
        let (facade, repo, capture) = facade_with_capture();
        let resp = manual_cc_raw(&facade, 9102, actors.clone()).await;

        assert_eq!(resp["msg"], EMPTY_SET_MSG,
            "G10：全空白（{actors}）必须与既有\"空集合\"档同判，实得 {resp}");
        assert_eq!(resp["code"], 99999999, "G10：同档＝同一个码，不新造错误语义：{resp}");
        assert!(repo.find_cc_actor_ids(9102).unwrap().is_empty(),
            "G10：全空白不得建 cc 行（{actors}）");
        assert!(fired(&capture).is_empty(), "G10：全空白不得 fire 码 4（{actors}）");

        // 与真·空集合逐字同答案（同一支错误，不是"看起来差不多"）
        let (facade2, _repo2, _c2) = facade_with_capture();
        let empty = manual_cc_raw(&facade2, 9102, json!([])).await;
        assert_eq!(resp, empty, "G10：全空白与空集合必须返回逐字一致");
    }
}

// ═══ 混给只丢空 / 数组含空元素 / 逗号串空元素 ═══

/// 数组里混空元素 ⇒ 只丢空的，有效的人照旧建行＋fire。
#[tokio::test]
async fn test_i141_g10_manual_leg_array_drops_blank_elements() {
    let (facade, repo, capture) = facade_with_capture();
    manual_cc(&facade, 9103, json!(["7601", "", "  ", "7602"])).await;

    assert_eq!(repo.find_cc_actor_ids(9103).unwrap(), vec!["7601".to_string(), "7602".to_string()],
        "G10：数组里的空元素丢弃、有效元素保留");
    assert_eq!(fired(&capture), vec!["7601".to_string(), "7602".to_string()],
        "G10：fire 的入参只含有效的人");
}

/// 逗号串与数组**同判据**（spec §2.10：别只修一条腿）——两种写法给出同一个落库集合。
#[tokio::test]
async fn test_i141_g10_manual_leg_csv_and_array_same_judgement() {
    let cases = [
        ("7701,,7702", vec!["7701", "7702"]),
        ("8201,", vec!["8201"]),
    ];
    for (i, (csv, want)) in cases.into_iter().enumerate() {
        let iid = 9104 + i as i64;
        let (facade, repo, capture) = facade_with_capture();
        manual_cc(&facade, iid, json!(csv)).await;
        assert_eq!(repo.find_cc_actor_ids(iid).unwrap(),
            want.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            "G10：逗号串形态（第 {i} 档）空段丢弃");
        assert_eq!(fired(&capture),
            want.iter().map(|s| s.to_string()).collect::<Vec<_>>(), "G10：只逐有效人 fire");

        // 同一批人换数组写法 ⇒ 逐字同答案
        let (facade2, repo2, _c2) = facade_with_capture();
        manual_cc(&facade2, iid, json!(want)).await;
        assert_eq!(repo2.find_cc_actor_ids(iid).unwrap(),
            repo.find_cc_actor_ids(iid).unwrap(),
            "G10：逗号串与数组两形必须同判据（第 {i} 档）");
    }
}

// ═══ trim 后同值＝同一个人（与 G2 判重咬合）═══

/// 落库值取 trim 后的串：`" 8301 "` 与 `"8301"` 是同一个人。
#[tokio::test]
async fn test_i141_g10_manual_leg_values_are_trimmed() {
    let (facade, repo, _capture) = facade_with_capture();
    manual_cc(&facade, 9106, json!([" 8301 ", "8302"])).await;

    assert_eq!(repo.find_cc_actor_ids(9106).unwrap(), vec!["8301".to_string(), "8302".to_string()],
        "G10：入库值应是 trim 后的串");
}

/// 先抄 `"8401"` 再抄 `" 8401 "` ⇒ 判重命中：仍是 1 行、0 新 fire（不 trim 就把 G2 打穿）。
#[tokio::test]
async fn test_i141_g10_padded_value_hits_g2_dedup() {
    let (facade, repo, capture) = facade_with_capture();
    manual_cc(&facade, 9107, json!(["8401"])).await;
    capture.events.lock().unwrap().clear();

    manual_cc(&facade, 9107, json!([" 8401 "])).await;

    assert_eq!(repo.find_cc_actor_ids(9107).unwrap(), vec!["8401".to_string()],
        "G10：带空格的同一人不得再建第二行");
    assert!(fired(&capture).is_empty(), "G10：判重命中 ⇒ 不 fire 码 4");
}

// ═══ 反向哨兵 ═══

/// 判据只吃空值，不吃 `"0"` 这类"看起来像空"的正常 id。
#[tokio::test]
async fn test_i141_g10_zero_actor_id_is_not_treated_as_blank() {
    let (facade, repo, capture) = facade_with_capture();
    manual_cc(&facade, 9108, json!(["0", "user-1"])).await;

    assert_eq!(repo.find_cc_actor_ids(9108).unwrap(), vec!["0".to_string(), "user-1".to_string()],
        "G10 只丢空串/纯空白：'0' 这类正常 id 不得被吃掉");
    assert_eq!(fired(&capture).len(), 2, "反向哨兵：照旧逐人 fire");
}

/// 哨兵＋空值混给 ⇒ 只丢空的，`"0"` 留下（丢完为空与不丢完之间的边界）。
#[tokio::test]
async fn test_i141_g10_sentinel_mixed_with_blanks_keeps_zero() {
    let (facade, repo, _capture) = facade_with_capture();
    manual_cc(&facade, 9109, json!(["", "0", "  "])).await;
    assert_eq!(repo.find_cc_actor_ids(9109).unwrap(), vec!["0".to_string()],
        "G10：混给只丢空值，'0' 必须留下");
}
