//! issues/138 · ccList 行形状三要件（内存仓储这一支）——打在**门面出口 JSON**上的一格。
//!
//! 判据与集成层门禁格 `jeeflow-integrations/verify/contract/runner.py::judge_cc_row_shape`
//! 逐字同判（三个判点全部打在"读回来的那一行"上，不自创第四种形状）：
//!   ① 行来自实例表 ⇒ 必须有主键**键名** `id`（值＝实例 id）；投成 cc 表原样
//!      （只有 `processInstanceId`、没有 `id`）＝不合格；
//!   ② 必须有 `operator` 键；
//!   ③ `operator` 的值＝**流程发起人**，且**不得等于查询者本人**
//!      （等于查询者＝投了 `cc.actor_id`，那一列在被抄送人自己的查询里恒等于他自己，
//!       用"非空"当判据永远照不出来 ⇒ 判据必须是值对到具体那一方）。
//!
//! 内存仓内部（`jeeflow-core::memory`）的 `InstanceRow` 字段是必填的，"缺键"在那里表达不出来；
//! 键名维度只有在出口 JSON 上才照得出来，故本文件补的正是 `jeeflow-core/src/memory.rs`
//! 里 `cc_row_shape_i138_tests` 照不到的那一半。基准＝jeeflow-java 参考实现
//! `JdbcProcessRepository.pageInstances(cc = true)`（`FROM wf_process_instance t … SELECT … t.*`）；
//! `jeeflow-repository-sqlx` 侧本就合规（`SELECT pi.* … INNER JOIN wf_process_cc_instance cc`），
//! 本文件同时钉住"两仓形状一致"这条红线：内存仓这一支改坏就会与 SQL 仓分叉。

use jeeflow_core::context::ServiceContext;
use jeeflow_core::id_gen::AtomicIdGenerator;
use jeeflow_core::memory::MemoryRepository;
use jeeflow_core::model::*;
use jeeflow_core::spi::{ProcessExtRepository, ProcessRepository};
use jeeflow_facade::JeeflowFacade;
use serde_json::{json, Value as Json};
use std::collections::HashMap;
use std::sync::Arc;

const APPLICANT: &str = "i138_applicant";
const CC_ACTOR: &str = "i138_cc_actor";
/// 发起这次抄送的人（cc 行自己的 create_user）＝条文点名的错法②落点，三方两两不同才照得出值。
const CC_SENDER: &str = "i138_cc_sender";

/// 夹具：定义 + 实例（发起人 APPLICANT）+ 一条 cc 行（接收人 CC_ACTOR）。
/// 返回 (facade, 实例 id)——要件①要的就是"主键值＝实例 id"，与 cc 表主键必然是两个值。
async fn seed_i138() -> (JeeflowFacade, i64) {
    let repo = Arc::new(MemoryRepository::new());
    let ctx = ServiceContext::new()
        .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
        .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
        .with_id_generator(Arc::new(AtomicIdGenerator::new(100000)));
    let facade = JeeflowFacade::new(ctx);

    let mut define = ProcessDefine {
        id: 0, name: "i138-facade-flow".into(), display_name: "I138 Facade Flow".into(),
        define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
        version: 3, create_time: None, create_user: None,
        update_time: None, update_user: None,
    };
    facade.repo().save_define(&mut define).unwrap();

    let mut inst = ProcessInstance {
        instance_id: 0, parent_id: None, define_id: define.id, state: 10,
        parent_node_name: None, business_no: Some("i138-facade-biz".into()),
        operator: APPLICANT.into(), expire_time: None,
        variables: jeeflow_core::json::FlowData::new(),
        tasks: vec![], create_time: Some("2026-09-29 10:00:00".into()),
        create_user: Some(APPLICANT.into()), update_time: None, update_user: None,
        define: None,
    };
    facade.repo().save_instance(&mut inst).unwrap();
    let instance_id = inst.instance_id;

    facade
        .repo()
        .create_cc_instance(instance_id, CC_SENDER, &[CC_ACTOR.into()])
        .unwrap();
    (facade, instance_id)
}

/// 与 runner.py 同判的三要件判据（行＝出口 JSON 的那一行）。返回不合格原因，空＝合格。
fn judge_cc_row_shape(row: Option<&Json>, instance_id: i64, applicant: &str, cc_actor: &str) -> Vec<String> {
    let row = match row {
        Some(Json::Object(m)) if !m.is_empty() => m,
        _ => return vec!["ccList 里找不到这一行（抄送数据腿本身没建 cc 行，形状无从判）".to_string()],
    };
    let mut reasons = Vec::new();
    // 要件①：键名必须是 `id`，且值＝实例 id（php 旧形状出的是 `processInstanceId`）。
    match row.get("id") {
        None => reasons.push(format!(
            "行主键没有 `id` 键（实得键集合 {:?}）⇒ 行来自 cc 表而不是实例表",
            {
                let mut ks: Vec<String> = row.keys().cloned().collect();
                ks.sort();
                ks
            })),
        Some(v) => {
            let got = v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()));
            if got != Some(instance_id) {
                reasons.push(format!("行主键 id={:?} 不是实例 id={} ⇒ 投的是 cc 表主键", v, instance_id));
            }
        }
    }
    // 要件②＋③：键名 `operator` 必须在，值必须是流程发起人且不得等于查询者本人。
    match row.get("operator") {
        None => reasons.push("行里没有 `operator` 键 ⇒ 出口把 cc 行原样投出来了".to_string()),
        Some(v) => {
            let op = v.as_str().unwrap_or_default().to_string();
            if op == cc_actor {
                reasons.push(format!(
                    "operator={} 是**被抄送人**（＝查询者本人，恒等）——基准要的是流程发起人；\
                     投 `cc.actor_id` 的栈在这一档永远自等，所以判据只能比「值对到具体那一方」", op));
            } else if !applicant.is_empty() && op != applicant {
                reasons.push(format!(
                    "operator={} 既不是流程发起人 {}、也不是被抄送人 ⇒ 投了别的列（多半是 cc.create_user）",
                    op, applicant));
            }
        }
    }
    reasons
}

async fn cc_list_row(facade: &JeeflowFacade, actor: &str) -> Json {
    let mut args: HashMap<String, Json> = HashMap::new();
    args.insert("operator".into(), json!(actor));
    args.insert("pageNum".into(), json!(1));
    args.insert("pageSize".into(), json!(50));
    let resp = facade.flow("processInstance/ccList", &args).await;
    assert_eq!(resp["code"], 0, "ccList 应成功，实得 {:?}", resp);
    resp["data"]["rows"][0].clone()
}

/// 正向：三要件打在出口 JSON 的那一行上，全过。
#[tokio::test]
async fn test_i138_facade_cc_list_row_shape_three_requirements() {
    let (facade, instance_id) = seed_i138().await;
    let row = cc_list_row(&facade, CC_ACTOR).await;
    let reasons = judge_cc_row_shape(Some(&row), instance_id, APPLICANT, CC_ACTOR);
    assert!(reasons.is_empty(), "ccList 出口行形状三要件不合格：{:?}｜实得 {:?}", reasons, row);
    assert!(instance_id > 0, "夹具前提：实例 id 是有效主键");

    // 「rows 同 processInstance/page 行结构」：与发起人的 processInstance/page 行同键集同主键。
    let mut pargs: HashMap<String, Json> = HashMap::new();
    pargs.insert("operator".into(), json!(APPLICANT));
    pargs.insert("pageNum".into(), json!(1));
    pargs.insert("pageSize".into(), json!(50));
    let resp = facade.flow("processInstance/page", &pargs).await;
    assert_eq!(resp["code"], 0, "processInstance/page 应成功，实得 {:?}", resp);
    let mine = &resp["data"]["rows"][0];
    let mut cc_keys: Vec<&str> = row.as_object().unwrap().keys().map(String::as_str).collect();
    let mut page_keys: Vec<&str> = mine.as_object().unwrap().keys().map(String::as_str).collect();
    cc_keys.sort();
    page_keys.sort();
    assert_eq!(cc_keys, page_keys, "ccList 行必须与 processInstance/page 行同键集");
    assert_eq!(row["id"], mine["id"], "同一实例两张列表的主键必须同为实例 id");
    assert_eq!(row["operator"], mine["operator"], "operator 两处同源＝流程发起人");
    assert_eq!(row["state"], mine["state"], "state 是实例状态，不是 cc 的已读位");
}

/// 负向：判据敢报红（对应门禁格 `--selftest-l2-31` 的四支）。
/// 这是"本格有牙"的离线自证——基准形状绿，三种错法形状各自红，零行也红。
#[tokio::test]
async fn test_i138_facade_cc_list_judge_rejects_wrong_shapes() {
    let (facade, instance_id) = seed_i138().await;
    let iid = json!(instance_id.to_string());
    let ok = json!({"id": iid, "operator": APPLICANT, "state": 10});
    let m1_actor = json!({"id": iid, "operator": CC_ACTOR, "state": 10});
    let m2_no_id = json!({"processInstanceId": iid, "operator": APPLICANT});
    let m2b_cc_pk = json!({"id": "777777777", "operator": APPLICANT});
    let third = json!({"id": iid, "operator": CC_SENDER});

    assert!(judge_cc_row_shape(Some(&ok), instance_id, APPLICANT, CC_ACTOR).is_empty(), "基准形状必须绿");
    assert!(!judge_cc_row_shape(Some(&m1_actor), instance_id, APPLICANT, CC_ACTOR).is_empty(),
        "错法①（operator=被抄送人）必须红");
    assert!(!judge_cc_row_shape(Some(&m2_no_id), instance_id, APPLICANT, CC_ACTOR).is_empty(),
        "错法③（主键出 processInstanceId 而没有 id）必须红");
    assert!(!judge_cc_row_shape(Some(&m2b_cc_pk), instance_id, APPLICANT, CC_ACTOR).is_empty(),
        "错法③变体（id 投成 cc 表主键）必须红");
    assert!(!judge_cc_row_shape(Some(&third), instance_id, APPLICANT, CC_ACTOR).is_empty(),
        "错法②（operator=cc.create_user 第三者）必须红");
    assert!(!judge_cc_row_shape(None, instance_id, APPLICANT, CC_ACTOR).is_empty(), "零行必须红");

    // 真出口也得报红：换一个查不到的人 ⇒ rows 为空 ⇒ 判据不装绿。
    let none = cc_list_row(&facade, "i138_nobody").await;
    assert!(!judge_cc_row_shape(Some(&none), instance_id, APPLICANT, CC_ACTOR).is_empty(),
        "查不到行时判据必须红（不得把零行折叠成绿）");
}
