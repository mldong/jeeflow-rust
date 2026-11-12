//! issues/252 · 节点级 `taskType=1`（协办）**解析期禁配**（Rust 栈 T0）。
//!
//! 契约出处：spec 02 §4.4 末条 bullet ＋ §4 字段表 `taskType` 行。协办档今后只由运行期
//! `processTask/aidant` 派生（issues/253，本轮不做）；节点上配协办在 snaker 原义下是死路——
//! 协办任务办结不产生执行对象、跳转吃同一道闸 ⇒ 实例仍运行中却零待办无后继（流程悬挂）。
//! 所以这一档在**解析期**就拦，不是运行时降级。
//!
//! ⚠️ **本栈容错集合现读**（与 java/c# 不同，判据按本栈实际折值写）：
//! `NodeModel::task_type()` 走 `prop_i64` → `JsonValue::as_i64`（Number 取整 / Str 按
//! `parse::<i64>`），**没有 codeOf 别名表**（`model::TaskType` 只有 `from_code`）。
//! ⇒ 能折成协办(1) 的只有数字档：`1`、`"1"`、`"+1"`、`"01"`、`1.9`。
//! ⇒ `"Aidant"`／`"SECONDARY"`／`"协办"` 解不出整数 ⇒ 仍静默回落主办(0)，按 issues/252 §5-3
//!    边界**不得扩成 fail-fast**（issues/245 口径）。本文件把这条边界钉成回归格。
//!
//! 夹具纪律：全部内联 JSON，**不碰共享 `flows/`**（同 `dynamic_countersign.rs` 顶部那条说明）。

use jeeflow_core::parser::{ModelParser, NodeType};
use jeeflow_core::task_type;

/// 逐字契约文案：八栈同尺；改字面量要走 issue，不要顺手改。
const VERBATIM: &str = "节点[review]不允许配置协办任务类型（taskType=1）";

/// start→apply→review→final→end；`review_props` 是追加到 review 节点 properties 的片段。
fn chain(review_props: &str) -> String {
    let r = if review_props.is_empty() {
        String::new()
    } else {
        format!(",{}", review_props)
    };
    format!(
        r#"{{
            "name": "tt252-inline",
            "displayName": "协办禁配夹具",
            "type": "approval",
            "nodes": [
                {{"id":"start","type":"snaker:start","properties":{{}}}},
                {{"id":"apply","type":"snaker:task","properties":{{"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"}}}},
                {{"id":"review","type":"snaker:task","properties":{{"form":"form-r","assignee":"userA","taskType":0,"performType":0{r}}}}},
                {{"id":"final","type":"snaker:task","properties":{{"form":"form-f","assignee":"userF","taskType":0,"performType":0}}}},
                {{"id":"end","type":"snaker:end","properties":{{}}}}
            ],
            "edges": [
                {{"id":"e1","type":"snaker:transition","sourceNodeId":"start","targetNodeId":"apply","properties":{{}}}},
                {{"id":"e2","type":"snaker:transition","sourceNodeId":"apply","targetNodeId":"review","properties":{{}}}},
                {{"id":"e3","type":"snaker:transition","sourceNodeId":"review","targetNodeId":"final","properties":{{}}}},
                {{"id":"e4","type":"snaker:transition","sourceNodeId":"final","targetNodeId":"end","properties":{{}}}}
            ]
        }}"#
    )
}

/// review 节点 properties 里**只留** taskType 一键（去掉夹具自带的 taskType:0，避免重复键）。
fn chain_only_task_type(raw: &str) -> String {
    chain("").replace(
        r#"{"form":"form-r","assignee":"userA","taskType":0,"performType":0}"#,
        &format!(r#"{{"form":"form-r","assignee":"userA","performType":0,"taskType":{}}}"#, raw),
    )
}

/// 把 final 换成 custom 记录节点，供 RECORD/custom 不受影响那格。
fn chain_with_custom(raw_task_type: &str) -> String {
    chain("").replace(
        r#"{"id":"final","type":"snaker:task","properties":{"form":"form-f","assignee":"userF","taskType":0,"performType":0}}"#,
        &format!(
            r#"{{"id":"rec","type":"snaker:custom","properties":{{"clazz":"noopHandler","taskType":{}}}}}"#,
            raw_task_type
        ),
    ).replace(
        r#""sourceNodeId":"review","targetNodeId":"final""#,
        r#""sourceNodeId":"review","targetNodeId":"rec""#,
    ).replace(
        r#""sourceNodeId":"final","targetNodeId":"end""#,
        r#""sourceNodeId":"rec","targetNodeId":"end""#,
    )
}

// ═══ 正向：凡折成协办(1) 的输入形 ⇒ 解析期报错、零副作用 ═══

/// 本栈 `as_i64` 折值下能落到协办(1) 的全部输入形（JSON 原文片段）。
const ASSIST_FORMS: &[(&str, &str)] = &[
    ("int 1", "1"),
    ("string \"1\"", "\"1\""),
    ("string \"+1\"（Rust parse::<i64> 认前导加号）", "\"+1\""),
    ("string \"01\"", "\"01\""),
    ("float 1.9（Number as i64 截断成 1）", "1.9"),
];

#[test]
fn assist_forms_are_rejected_at_parse_time() {
    for (label, raw) in ASSIST_FORMS {
        let err = ModelParser::parse(&chain_only_task_type(raw))
            .expect_err(&format!("应拦：{}", label));
        assert_eq!(VERBATIM, err.message(), "{}", label);
        // 内部码不得外泄（门面逐字透出这一串）
        assert!(!err.message().contains("2001"), "{}", label);
    }
}

/// 报错发生在**解析**阶段：拿不到模型 ⇒ 后续建单/落库根本无从发生（零副作用）。
#[test]
fn rejection_yields_no_model() {
    let parsed = ModelParser::parse(&chain_only_task_type("1"));
    assert!(parsed.is_err(), "协办档必须解析期失败，不得返回模型");
}

/// 文案取节点 **id**，不取 display_name（与 add_sign/recall/collect 三族同尺）。
#[test]
fn message_uses_node_id_not_display_name() {
    let flow = chain_only_task_type("\"1\"").replace(
        r#"{"id":"review","type":"snaker:task","properties""#,
        r#"{"id":"review","type":"snaker:task","text":{"value":"法务审核"},"properties""#,
    );
    let err = ModelParser::parse(&flow).expect_err("应拦");
    assert_eq!(VERBATIM, err.message());
    assert!(!err.message().contains("法务审核"), "不得用 display_name 档：{}", err.message());
}

/// 申请节点（首任务节点）配协办同样拦——禁配不看节点位置，只看折值。
#[test]
fn apply_node_assist_is_also_rejected() {
    let flow = chain("").replace(
        r#"{"form":"apply-form","taskType":0,"performType":0,"assignee":"applicant"}"#,
        r#"{"form":"apply-form","taskType":1,"performType":0,"assignee":"applicant"}"#,
    );
    let err = ModelParser::parse(&flow).expect_err("应拦：申请节点协办");
    assert_eq!("节点[apply]不允许配置协办任务类型（taskType=1）", err.message());
}

/// 逐字文案常量自证（摘掉禁配判据 ⇒ 上面几格全红，本格钉住文案本身）。
#[test]
fn pinned_message_matches_spec() {
    assert_eq!(VERBATIM, task_type::msg_assist_forbidden("review"));
}

// ═══ 回归：其余输入形维持既有折值，不扩成 fail-fast ═══

/// 不可识别值（含设计器现状写的 `Aidant`）与主办档 ⇒ 解析成功且折值不变（0）。
const TOLERATED_FORMS: &[(&str, &str)] = &[
    ("int 0", "0"),
    ("string \"0\"", "\"0\""),
    ("string \"Major\"", "\"Major\""),
    ("string \"Aidant\"（设计器现状值，八栈都不认）", "\"Aidant\""),
    ("string \"SECONDARY\"（本栈无别名表）", "\"SECONDARY\""),
    ("string \"协办\"（本栈无中文别名）", "\"协办\""),
    ("string \"主办\"", "\"主办\""),
    ("空串", "\"\""),
    ("null", "null"),
];

#[test]
fn non_assist_forms_still_parse_and_fold_to_major() {
    for (label, raw) in TOLERATED_FORMS {
        let model = ModelParser::parse(&chain_only_task_type(raw))
            .unwrap_or_else(|e| panic!("{} 不该被拦：{}", label, e.message()));
        let review = model.get_node("review").expect("review 节点应在");
        assert_eq!(0, review.task_type(), "{}：既有静默回落口径不得被本轮改动", label);
    }
}

/// 缺省不写键 ⇒ task_type() 回落 0（本栈无 null 档，与改前逐字同形）。
#[test]
fn absent_key_folds_to_major() {
    let flow = chain("").replace(
        r#"{"form":"form-r","assignee":"userA","taskType":0,"performType":0}"#,
        r#"{"form":"form-r","assignee":"userA","performType":0}"#,
    );
    let model = ModelParser::parse(&flow).expect("缺键应解析成功");
    assert_eq!(0, model.get_node("review").unwrap().task_type());
}

/// RECORD(2) 档不受影响：数字与数字串都原样落 2。
#[test]
fn record_type_is_untouched() {
    for raw in ["2", "\"2\""] {
        let model = ModelParser::parse(&chain_only_task_type(raw)).expect("RECORD 档应放行");
        assert_eq!(2, model.get_node("review").unwrap().task_type());
    }
}

/// custom（记录类）节点不受影响：本校验只看 `NodeType::Task`，custom 上写 1 也放行。
#[test]
fn custom_node_with_assist_value_still_parses() {
    let model = ModelParser::parse(&chain_with_custom("1")).expect("custom 节点不该被任务节点那把刀拦");
    let rec = model.get_node("rec").expect("rec 节点应在");
    assert_eq!(NodeType::Custom, rec.node_type);
}