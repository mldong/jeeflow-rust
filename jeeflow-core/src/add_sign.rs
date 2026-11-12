//! 后加签（可重复）· 节点属性解析、写入口校验、轮次与账本读写。
//!
//! 移植基准＝jeeflow-java `core/AddSignSupport.java`（参考实现）；契约正文：
//! - spec/02 §4.3（两枚节点开关 ＋ 三条解析期校验）
//! - spec/04「后加签（可重复）」（三档推进闸、merged 判据顺序、建单规则、语义边界）
//! - spec/06 §2.7（协议键 `tf_addSignOperators` / `tf_addSignHistory` 与互斥判据）＋文末失败 msg 表
//!
//! 本模块只放**判据与簿记**；推进闸与建单的编排在 `engine.rs`（会签档、重提直返档同址，
//! 见 `execute_task_locked` 的「10. Countersign gate check」与 `execute_reapply`）。
//!
//! **为什么四道写入口判据（互斥／开关未开／加给自己／超上限）一律排在改动状态之前**：
//! 本栈宿主未注册事务模板时是**裸跑**（`engine.rs::in_tx` 的 None 分支），一旦先
//! `complete_task` 再判错，那条任务行就变成已完成而无人能改回来——"报错且零写入"是这条链的
//! 硬要求，所以 [`validate_submission`] 由引擎在办理/跳转入口、`complete_task` 之前调用，
//! 而不是放进建单腿（对齐 java 把它放在 `prepareExecution` 里的那一刀）。

use crate::engine::parse_actor_ids;
use crate::error::{JeeflowError, JeeflowResult};
use crate::json::{JsonValue, FlowData};
use crate::model::{current_time_str, ProcessInstance, ProcessTask, TaskState};
use crate::parser::{NodeType, ProcessModel};

// ═══════════════════════════════════════════════════════
// 键与逐字文案
// ═══════════════════════════════════════════════════════

/// 节点属性键：后加签开关（`properties` 平铺层，缺省 false；不接受 `ext` 嵌套，避免双源优先级）。
pub const PROP_ALLOW_ADD_SIGN: &str = "allowAddSign";
/// 节点属性键：加签轮次上限（整数 ≥0，0＝不限，缺省 0）。
pub const PROP_ADD_SIGN_MAX_ROUNDS: &str = "addSignMaxRounds";

/// 本次提交指定的**本节点**后继办理人（逗号串与数组两形同判据）。
pub const ADD_SIGN_OPERATORS: &str = "tf_addSignOperators";
/// 追加式账本（落已完成任务行变量，形状照 `tf_transferHistory` 那族）。
pub const ADD_SIGN_HISTORY: &str = "tf_addSignHistory";
/// 轮次号键前缀（拼 `_{节点名}`）。**不复用会签的 `loopCounter`**：后者是退回复活时
/// 被 `rollback_to_parent` 剔掉的会签簿记，加签链的轮次要跨"已办过的人"计数，两者语义不同。
pub const ADD_SIGN_ROUND: &str = "addSignRound";
/// 与 `tf_addSignOperators` 互斥的那枚既有键（spec 06 §2.7）。
pub const NEXT_NODE_OPERATOR: &str = "tf_nextNodeOperator";

/// 逐字判据文案（spec 06 文末失败 msg 表第一格）。
pub const MSG_MUTEX: &str = "tf_addSignOperators 与 tf_nextNodeOperator 互斥，不能同时提交";
/// 逐字判据文案（加签给自己）。
pub const MSG_SELF: &str = "不能把任务加签给自己";

/// 逐字判据文案（节点未开 `allowAddSign` 却传键）。
pub fn msg_switch_off(node_name: &str) -> String {
    format!("节点[{}]未开启 allowAddSign，不接受 tf_addSignOperators", node_name)
}

/// 逐字判据文案（超 `addSignMaxRounds`）。
pub fn msg_max_rounds(node_name: &str, max: i64) -> String {
    format!("节点[{}]的加签轮次已达上限 {}，本次提交被拒绝", node_name, max)
}

/// 配置非法文案：键名跟着入参走（java `parseAllow` 的 `"节点[" + nodeName + "]的 " + keyName + " 配置非法: " + v`）。
fn bad_allow_key(node_name: &str, key_name: &str, raw: &str) -> JeeflowError {
    JeeflowError::Business(format!(
        "节点[{}]的 {} 配置非法: {}",
        node_name, key_name, raw
    ))
}

fn bad_max_rounds(node_name: &str, raw: &str, suffix: &str) -> JeeflowError {
    JeeflowError::Business(format!(
        "节点[{}]的 {} 配置非法: {}{}",
        node_name, PROP_ADD_SIGN_MAX_ROUNDS, raw, suffix
    ))
}

// ═══════════════════════════════════════════════════════
// 解析期：两枚节点属性 ＋ 两条硬校验
// ═══════════════════════════════════════════════════════

/// 解析 `allowAddSign`：缺省 false；非法值**必须**报配置错误，不得静默当缺省
/// （校验纪律照本栈 `reapply::ReapplyMode::parse` 的先例，文案照 java `parseAllow`）。
///
/// 键名固定成本函数写死的那一枚；`allowRecall`／`dynamicCountersign` 两档走
/// [`parse_allow_keyed`]（java 侧同一个 `parseAllow` 收 `keyName` 参数，错误文案里的
/// 键名跟着换 ⇒ 三档同一把尺子，不发明第二种文案）。
pub fn parse_allow(value: Option<&JsonValue>, node_name: &str) -> JeeflowResult<bool> {
    parse_allow_keyed(value, node_name, PROP_ALLOW_ADD_SIGN)
}

/// 布尔开关的通用解析档（**同一把尺子**，只有错误文案里的键名跟着 `key_name` 走）：
/// 缺省 false；布尔原样取；串档 trim 后 `true`/`false` 不分大小写；空串＝缺省；
/// 其余档（其它串／数字／数组／对象）报配置错误，**不得静默当缺省**。
///
/// 对齐 java `AddSignSupport.parseAllow(Object, String, String)`——java 的注释就写明
/// "allowAddSign/allowRecall 同尺"，本栈三档（`allowAddSign`／`allowRecall`／
/// `dynamicCountersign`）全部收口到这里。
pub fn parse_allow_keyed(
    value: Option<&JsonValue>,
    node_name: &str,
    key_name: &str,
) -> JeeflowResult<bool> {
    let raw = match value {
        None | Some(JsonValue::Null) => return Ok(false),
        Some(JsonValue::Bool(b)) => return Ok(*b),
        Some(v) => match v.as_str() {
            Some(s) => s.trim().to_string(),
            // 其余档（数字/数组/对象）按字面进错误信息，与 java `String.valueOf` 同形
            None => v.to_json_string(),
        },
    };
    if raw.is_empty() {
        return Ok(false);
    }
    if raw.eq_ignore_ascii_case("true") {
        return Ok(true);
    }
    if raw.eq_ignore_ascii_case("false") {
        return Ok(false);
    }
    Err(bad_allow_key(node_name, key_name, &raw))
}

/// 解析 `addSignMaxRounds`：缺省 0＝不限；非整数或负数报配置错误（同样不得静默当缺省）。
pub fn parse_max_rounds(value: Option<&JsonValue>, node_name: &str) -> JeeflowResult<i64> {
    let raw = match value {
        None | Some(JsonValue::Null) => return Ok(0),
        Some(JsonValue::Number(n)) => {
            let i = *n as i64;
            if *n != i as f64 {
                return Err(bad_max_rounds(
                    node_name,
                    &JsonValue::Number(*n).to_json_string(),
                    "（必须是整数）",
                ));
            }
            if i < 0 {
                return Err(bad_max_rounds(node_name, &i.to_string(), "（不得为负，0 表示不限）"));
            }
            return Ok(i);
        }
        Some(v) => match v.as_str() {
            Some(s) => s.trim().to_string(),
            None => v.to_json_string(),
        },
    };
    if raw.is_empty() {
        return Ok(0);
    }
    match raw.parse::<i64>() {
        Ok(n) if n >= 0 => Ok(n),
        Ok(n) => Err(bad_max_rounds(node_name, &n.to_string(), "（不得为负，0 表示不限）")),
        Err(_) => Err(bad_max_rounds(node_name, &raw, "（必须是整数，0 表示不限）")),
    }
}

/// 两条解析期硬校验（spec 02 §4.3）——配了就直接报错，不做运行时降级：
/// 静默跑下去会让配置者以为开关生效，实际两档推进闸各裁各的。
///
/// 由 `parser::ModelParser::parse` 在节点与边都接好之后调用（"是不是首任务节点"要沿
/// start 的出边看），位置与文案逐字对齐 java `ModelParser` 收尾那一段。
pub fn validate_model(model: &ProcessModel) -> JeeflowResult<()> {
    for node in model
        .nodes
        .iter()
        .filter(|n| n.node_type == NodeType::Task && n.allow_add_sign)
    {
        if node.perform_type() == 1 {
            return Err(JeeflowError::Business(format!(
                "会签节点[{}]不允许配置 allowAddSign：会签池内加人与会签之后加人是两种语义，推进闸会打架",
                node.id
            )));
        }
        if model.is_first_task_node(&node.id) {
            return Err(JeeflowError::Business(format!(
                "首任务节点[{}]不允许配置 allowAddSign：申请节点的办理是发起/重提语义，与加签路由打架",
                node.id
            )));
        }
    }
    Ok(())
}

// ═══════════════════════════════════════════════════════
// 写入口判据（必须排在任何状态变更之前）
// ═══════════════════════════════════════════════════════

/// 本次提交指定的加签人（逗号串与数组两形共用 §2.11 那枚归一单点
/// [`crate::engine::parse_actor_ids`]——逐项 trim、空串/纯空白/`null` 丢弃、
/// 丢完为空＝没传；本栈不写第二份拆形判据）。
pub fn operators(args: &FlowData) -> Vec<String> {
    parse_actor_ids(args.get(ADD_SIGN_OPERATORS))
}

/// `tf_nextNodeOperator` 是否"给了"（判据与 java 同尺：非 null 且标量档非纯空白）。
///
/// issues/247 起这是**互斥判据的唯一实现**，三个调用点共用：后加签写入口
/// [`validate_submission`]、门面动态会签发起分流、以及门面闸门对成员行携带拉人的判据。
/// 抬成 public 而不是在门面重写一份——"不另造第二把尺子"（php 那边留了第三份同判据实现，
/// 本栈收在一处）。
pub fn next_node_present(args: &FlowData) -> bool {
    match args.get(NEXT_NODE_OPERATOR) {
        None | Some(JsonValue::Null) => false,
        Some(JsonValue::Str(s)) => !s.trim().is_empty(),
        Some(_) => true,
    }
}

/// 写入口校验（互斥／开关未开／加给自己／超上限）。**必须在改动任何状态之前调用。**
///
/// 互斥这一条的理由不是语义矛盾（一个管本节点再来一轮、一个管流出后交给谁，逻辑可共存），
/// 而是**参数生命周期**：`tf_*` 键只在当次请求的执行参数与已完成任务行变量里可读，
/// 加签使令牌停留本节点、流出发生在后续请求，届时先传的 `tf_nextNodeOperator` 已不可读
/// ——同传时它是死参数。静默择一＝用户选的"下一节点处理人"无声消失，报错是把它变响话。
pub fn validate_submission(
    model: &ProcessModel,
    task: &ProcessTask,
    args: &FlowData,
    operator: &str,
) -> JeeflowResult<()> {
    let ops = operators(args);
    if !ops.is_empty() && next_node_present(args) {
        return Err(JeeflowError::Business(MSG_MUTEX.to_string()));
    }
    if ops.is_empty() {
        // 没点人＝正常流转档，其余判据不涉及（也不要求节点开开关）
        return Ok(());
    }
    let node = match model.get_node(&task.task_name) {
        Some(n) if n.node_type == NodeType::Task && n.allow_add_sign => n,
        _ => return Err(JeeflowError::Business(msg_switch_off(&task.task_name))),
    };
    let me = operator.trim();
    if !me.is_empty() && ops.iter().any(|a| a == me) {
        return Err(JeeflowError::Business(MSG_SELF.to_string()));
    }
    let max = node.add_sign_max_rounds;
    if max > 0 && round_of(task, &node.id) + 1 > max {
        return Err(JeeflowError::Business(msg_max_rounds(&node.id, max)));
    }
    Ok(())
}

// ═══════════════════════════════════════════════════════
// 推进闸 · 废弃 · 轮次 · 账本
// ═══════════════════════════════════════════════════════

/// 本节点是否还有未完成待办（刚办结那条此刻已是 FINISHED，不计入）。
///
/// merged 判据三档里的第二档：**先判这个**（顺序不可反）——给了人却本节点还有兄弟待办时
/// 建单那一条仍在同圈，两档都落空才放行出边。
pub fn has_doing_on_node(instance: &ProcessInstance, node_name: &str) -> bool {
    // issues/253：协办行（task_type=1）不算"兄弟待办"——它不驱动流程、不是接力链的一环，
    // 算进来会让 waitCompletion=false 的主办在加签/征询节点上被协办行挡住推进（spec/04
    // 「缺省 false 主办可随时推进」）。协办行的等待由 [`crate::aidant::gate_wait`] 单独裁决。
    instance.tasks.iter().any(|t| {
        t.task_name == node_name
            && t.task_state == TaskState::Doing.code()
            && !crate::aidant::is_aidant_row(t)
    })
}

/// 废弃本节点未完成的加签任务（拒绝／退回发起人时调用，不留孤儿待办）。
/// 只对开了 `allowAddSign` **或** `dynamicCountersign` 的节点动手（issues/247：征询成员行同属
/// 本节点 DOING，实例撤回/退回路径一并清场）；会签节点的兄弟任务由既有会签腿管，不在此扩面。
///
/// 返回实际废弃的行 id，由调用方把改过的行逐条写回仓储（本栈聚合根与仓储行是两份拷贝）。
pub fn abandon_doing_on_node(
    model: &ProcessModel,
    instance: &mut ProcessInstance,
    node_name: &str,
    operator: &str,
) -> Vec<i64> {
    let allowed = match model.get_node(node_name) {
        Some(n) => {
            n.node_type == NodeType::Task && (n.allow_add_sign || n.dynamic_countersign)
        }
        None => false,
    };
    if !allowed {
        return Vec::new();
    }
    let mut abandoned = Vec::new();
    for t in &mut instance.tasks {
        if t.task_name == node_name && t.task_state == TaskState::Doing.code() {
            if t.abandon().is_ok() {
                t.update_user = Some(operator.to_string());
                abandoned.push(t.task_id);
            }
        }
    }
    abandoned
}

/// 加签轮次变量键（`addSignRound_{节点名}`）。
pub fn round_key(node_name: &str) -> String {
    format!("{}_{}", ADD_SIGN_ROUND, node_name)
}

/// 读轮次号：首圈（普通建单产生的那条）没有该键 ⇒ 0。
pub fn round_of(task: &ProcessTask, node_name: &str) -> i64 {
    task.variables.get_i64_or(&round_key(node_name), 0)
}

/// 账本追加（只追加不覆盖），落已完成任务行变量；`tf_` 前缀使血缘回退腿天然剔掉它
/// （见 `engine.rs::rollback_to_parent` 里 `k.starts_with("tf_")` 那一支）。
///
/// 五键固定 camelCase：`round / fromActor / toActors / time / operator`；
/// `time` 为 `"yyyy-MM-dd HH:mm:ss"`（spec §2.4 全生态时间格式，账本跨栈同形）——
/// 走本引擎的共用时钟 [`crate::model::current_time_str`]，与其余留痕同一把尺子。
pub fn append_history(completed: &mut ProcessTask, operator: &str, to_actors: &[String], round: i64) {
    let mut history: Vec<JsonValue> = match completed.variables.get(ADD_SIGN_HISTORY) {
        Some(JsonValue::Array(items)) => items.clone(),
        _ => Vec::new(),
    };
    history.push(JsonValue::Object(vec![
        ("round".to_string(), JsonValue::Number(round as f64)),
        ("fromActor".to_string(), JsonValue::Str(operator.to_string())),
        (
            "toActors".to_string(),
            JsonValue::Array(to_actors.iter().map(|a| JsonValue::Str(a.clone())).collect()),
        ),
        ("time".to_string(), JsonValue::Str(current_time_str())),
        ("operator".to_string(), JsonValue::Str(operator.to_string())),
    ]));
    completed
        .variables
        .insert(ADD_SIGN_HISTORY.to_string(), JsonValue::Array(history));
}

/// 读账本（测试与投影用）。
pub fn history_of(task: &ProcessTask) -> Vec<JsonValue> {
    match task.variables.get(ADD_SIGN_HISTORY) {
        Some(JsonValue::Array(items)) => items.clone(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::ModelParser;

    fn raw(json: &str) -> JsonValue {
        crate::json::parse_json(json).unwrap()
    }

    #[test]
    fn test_parse_allow_default_and_illegal() {
        assert!(!parse_allow(None, "n").unwrap());
        assert!(!parse_allow(Some(&JsonValue::Null), "n").unwrap());
        assert!(!parse_allow(Some(&JsonValue::Str("   ".into())), "n").unwrap());
        assert!(parse_allow(Some(&JsonValue::Bool(true)), "n").unwrap());
        assert!(parse_allow(Some(&JsonValue::Str("TRUE".into())), "n").unwrap());
        assert!(!parse_allow(Some(&JsonValue::Str("false".into())), "n").unwrap());
        let err = parse_allow(Some(&JsonValue::Str("yes".into())), "review").unwrap_err();
        assert_eq!(err.message(), "节点[review]的 allowAddSign 配置非法: yes");
        // 数字档：java `String.valueOf(1)` ＝ "1"，同样落"配置非法"
        let err = parse_allow(Some(&JsonValue::Number(1.0)), "review").unwrap_err();
        assert_eq!(err.message(), "节点[review]的 allowAddSign 配置非法: 1");
    }

    #[test]
    fn test_parse_max_rounds_three_tiers() {
        assert_eq!(parse_max_rounds(None, "n").unwrap(), 0);
        assert_eq!(parse_max_rounds(Some(&JsonValue::Number(3.0)), "n").unwrap(), 3);
        assert_eq!(parse_max_rounds(Some(&JsonValue::Str("2".into())), "n").unwrap(), 2);
        assert_eq!(parse_max_rounds(Some(&JsonValue::Str("  ".into())), "n").unwrap(), 0);
        let err = parse_max_rounds(Some(&JsonValue::Number(1.5)), "review").unwrap_err();
        assert_eq!(err.message(), "节点[review]的 addSignMaxRounds 配置非法: 1.5（必须是整数）");
        let err = parse_max_rounds(Some(&JsonValue::Str("-1".into())), "review").unwrap_err();
        assert_eq!(
            err.message(),
            "节点[review]的 addSignMaxRounds 配置非法: -1（不得为负，0 表示不限）"
        );
        let err = parse_max_rounds(Some(&raw("\"1.5\"")), "review").unwrap_err();
        assert_eq!(
            err.message(),
            "节点[review]的 addSignMaxRounds 配置非法: 1.5（必须是整数，0 表示不限）"
        );
        let err = parse_max_rounds(Some(&JsonValue::Bool(true)), "review").unwrap_err();
        assert_eq!(
            err.message(),
            "节点[review]的 addSignMaxRounds 配置非法: true（必须是整数，0 表示不限）"
        );
    }

    #[test]
    fn test_operators_two_forms_share_one_normalizer() {
        let mut args = FlowData::new();
        args.insert(ADD_SIGN_OPERATORS.into(), JsonValue::Str("b, c ,, ".into()));
        assert_eq!(operators(&args), vec!["b".to_string(), "c".to_string()]);

        let mut arr = FlowData::new();
        arr.insert(
            ADD_SIGN_OPERATORS.into(),
            JsonValue::Array(vec![JsonValue::Str(" d ".into()), JsonValue::Null, JsonValue::Number(9.0)]),
        );
        assert_eq!(operators(&arr), vec!["d".to_string(), "9".to_string()]);

        // 丢完为空＝没传
        let mut empty = FlowData::new();
        empty.insert(ADD_SIGN_OPERATORS.into(), JsonValue::Str(" , ,".into()));
        assert!(operators(&empty).is_empty());
    }

    #[test]
    fn test_next_node_present_tiers() {
        let mut args = FlowData::new();
        assert!(!next_node_present(&args));
        args.insert(NEXT_NODE_OPERATOR.into(), JsonValue::Str("   ".into()));
        assert!(!next_node_present(&args), "纯空白＝没给");
        args.insert(NEXT_NODE_OPERATOR.into(), JsonValue::Array(vec![]));
        assert!(next_node_present(&args), "数组档只要给了就算（与 java String.valueOf 同尺）");
    }

    #[test]
    fn test_countersign_and_first_task_node_rejected_at_parse() {
        // 会签节点配开关 ⇒ 解析期报错（spec 02 §4.3）
        let cs = r#"{"name":"f1","nodes":[
            {"id":"start","type":"snaker:start"},
            {"id":"apply","type":"snaker:task","properties":{"assignee":"applicant"}},
            {"id":"review","type":"snaker:task","properties":{"assignee":"userA","allowAddSign":true,"performType":1}},
            {"id":"end","type":"snaker:end"}
        ],"edges":[
            {"sourceNodeId":"start","targetNodeId":"apply"},
            {"sourceNodeId":"apply","targetNodeId":"review"},
            {"sourceNodeId":"review","targetNodeId":"end"}
        ]}"#;
        let err = ModelParser::parse(cs).unwrap_err();
        assert!(err.message().contains("会签节点[review]不允许配置 allowAddSign"), "{}", err.message());

        // 首任务节点配开关 ⇒ 解析期报错
        let first = cs.replace(r#""id":"apply","type":"snaker:task","properties":{"assignee":"applicant"}"#,
            r#""id":"apply","type":"snaker:task","properties":{"assignee":"applicant","allowAddSign":true}"#);
        let err = ModelParser::parse(&first).unwrap_err();
        assert!(err.message().contains("首任务节点[apply]不允许配置 allowAddSign"), "{}", err.message());
    }

    #[test]
    fn test_history_append_is_additive_only() {
        let mut task = ProcessTask {
            task_id: 1,
            process_instance_id: 1,
            task_name: "review".into(),
            display_name: "审批".into(),
            task_type: 0,
            perform_type: 0,
            task_state: 20,
            actor_id: Some("userA".into()),
            actor_ids: vec!["userA".into()],
            finish_time: None,
            expire_time: None,
            form_key: None,
            parent_task_id: None,
            variables: FlowData::new(),
            create_time: None,
            create_user: None,
            update_time: None,
            update_user: None,
        };
        append_history(&mut task, "userA", &["userB".to_string()], 1);
        append_history(&mut task, "userB", &["userC".to_string()], 2);
        let hist = history_of(&task);
        assert_eq!(hist.len(), 2, "只追加不覆盖");
        let first = hist[0].as_object().unwrap();
        let keys: Vec<&str> = first.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, vec!["round", "fromActor", "toActors", "time", "operator"]);
        let time = first.iter().find(|(k, _)| k == "time").unwrap().1.as_str().unwrap().to_string();
        // "yyyy-MM-dd HH:mm:ss"＝全生态时间格式（19 个字符，逐位是数字与固定分隔符）
        let b = time.as_bytes();
        assert_eq!(b.len(), 19, "时间须是 yyyy-MM-dd HH:mm:ss，实得 {}", time);
        assert!(b[4] == b'-' && b[7] == b'-' && b[10] == b' ' && b[13] == b':' && b[16] == b':');
        assert!(time.chars().enumerate().all(|(i, c)| matches!(i, 4 | 7 | 10 | 13 | 16) || c.is_ascii_digit()));
    }

    #[test]
    fn test_round_of_defaults_to_zero() {
        let mut task = ProcessTask {
            task_id: 2,
            process_instance_id: 1,
            task_name: "review".into(),
            display_name: "审批".into(),
            task_type: 0,
            perform_type: 0,
            task_state: 20,
            actor_id: None,
            actor_ids: vec!["userA".into()],
            finish_time: None,
            expire_time: None,
            form_key: None,
            parent_task_id: None,
            variables: FlowData::new(),
            create_time: None,
            create_user: None,
            update_time: None,
            update_user: None,
        };
        assert_eq!(round_of(&task, "review"), 0, "首圈没有轮次键 ⇒ 0");
        task.variables.insert_i64(round_key("review"), 3);
        assert_eq!(round_of(&task, "review"), 3);
        assert_eq!(round_of(&task, "other"), 0, "轮次键按节点分档，不串味");
    }
}
