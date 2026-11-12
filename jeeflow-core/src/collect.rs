//! 动态会签（征询模式 `dynamicCountersign`，issues/247）· 成员行判据、账本与投影。
//!
//! 移植基准＝jeeflow-java 参考实现 `core/CollectSupport.java`（HEAD 09c2afe）＋
//! `facade/JeeflowFacade.tryDynamicCountersign / gateDynamicCountersign / taskRevokeCountersign`；
//! 契约正文：
//! - spec/02 §4.4（节点开关 ＋ 三条解析期硬校验）
//! - spec/04「动态会签（征询模式）」（提交分流／成员行建单规则／撤回／语义边界）
//! - spec/06 §2.7（协议键复用 `tf_addSignOperators` ＋ `tf_addSignReason` ＋ 投影
//!   `ext.dynamicCountersign`）＋ §processTask/revokeDynamicCountersign（逐字失败文案）
//!
//! 本模块只放**判据、账本与投影**（全部只读／纯函数）；发起分流与闸门由门面在聚合变更之前调用，
//! 建单腿由引擎收口（`engine.rs::create_collect_member_rows`，走既有 `persist_tasks`＝
//! save_task ＋ 全量写参与者 ＋ fire 码 3）。
//!
//! **三条承重判据**（java 测试照出来的，本栈同形，改动前先读这几条）：
//! 1. **成员行识别＝parent 且同节点同名**（[`member_rows`]）：主办办结推进后，下一节点行的
//!    parent 同样是主办行 id（建单不变量），只认 parent 会把主办推进后的下游行误当成员——
//!    第二次撤回归错文件、闸门也会挡错人；同名过滤不可省。
//! 2. **闸门收齐＝无 DOING 成员行**（[`pending_members`]），计数与投影一律**跳过 99（ABANDON）行**
//!    （[`count_members`]／[`build_projection`]）——撤回腿清场的行不再算在册人数。
//! 3. **撤回的参与人判断按名册**（`actor_ids`），不能用只认 DOING 行的 `ProcessTask::is_allowed`——
//!    主办行办结后会把合法撤回判成无权（落点在门面 `process_task_revoke_countersign`）。

use crate::add_sign;
use crate::error::{JeeflowError, JeeflowResult};
use crate::json::JsonValue;
use crate::model::{current_time_str, ProcessTask, TaskState};
use crate::parser::{NodeModel, NodeType, ProcessModel};

// ═══════════════════════════════════════════════════════
// 键与逐字文案
// ═══════════════════════════════════════════════════════

/// 节点属性键：动态会签（征询模式）开关（`properties` 平铺层，缺省 false）。
pub const PROP_DYNAMIC_COUNTERSIGN: &str = "dynamicCountersign";

/// 发起该轮会签的原因说明（选填，字符串）：随账本条目落库并盖到每条成员行（单行出证自足，
/// 不回查主办行）。＝ java `FlowConst.ADD_SIGN_REASON`。
pub const KEY_ADD_SIGN_REASON: &str = "tf_addSignReason";

/// 逐字判据文案（闸门·未收齐；spec 04／06 钉死，`{已提交}/{总数}` 两枚计数）。
pub fn msg_pending(node_name: &str, submitted: usize, total: usize) -> String {
    format!(
        "节点[{}]动态会签意见未收齐（已提交 {}/{}），不能办理",
        node_name, submitted, total
    )
}

/// 逐字判据文案（重复拉仍在待答的那位）。
pub fn msg_duplicate(actor: &str, node_name: &str) -> String {
    format!("操作人[{}]已在节点[{}]的动态会签中", actor, node_name)
}

/// 逐字判据文案（成员行携带拉人参数——拉人只限主办行）。
pub const MSG_MEMBER_PULL: &str = "动态会签成员任务不能发起加签";

/// 逐字判据文案（撤回档·无待答成员行；＝「收齐后本 action 报错」那一格）。
pub fn msg_no_pending(node_name: &str) -> String {
    format!("节点[{}]没有进行中的动态会签", node_name)
}

/// 逐字判据文案（撤回档·`operator` 非主办行参与者且非特权）。
pub const MSG_NO_PERMISSION_REVOKE: &str = "无权限撤回该动态会签";

/// 逐字判据文案（解析期禁配 · 与 `allowAddSign` 双开，spec 02 §4.4）。
pub fn msg_mutex_with_add_sign(node_name: &str) -> String {
    format!(
        "节点[{}]不允许同时配置 allowAddSign 与 dynamicCountersign：\
         两档共用 tf_addSignOperators，行为靠节点属性分流，双开＝行为歧义",
        node_name
    )
}

/// 逐字判据文案（解析期禁配 · 会签节点，spec 02 §4.4）。
pub fn msg_countersign_forbidden(node_name: &str) -> String {
    format!(
        "会签节点[{}]不允许配置 dynamicCountersign：会签池推进语义与征询挂起语义打架",
        node_name
    )
}

/// 逐字判据文案（解析期禁配 · 申请节点＝start 直接后继，spec 02 §4.4）。
pub fn msg_apply_forbidden(node_name: &str) -> String {
    format!(
        "申请节点[{}]不允许配置 dynamicCountersign：申请节点那手走实例撤回＋重新发起，不需要征询挂起",
        node_name
    )
}

/// 逐字判据文案（`operator` 不是主办行参与人）——java 这一支抛 `WfErrEnum.NOT_ALLOWED_EXECUTE`，
/// 本栈门面把它落成同一条逐字串（普通 execute 腿的既有文案不在本笔范围，不动）。
pub const MSG_NOT_ALLOWED: &str = "当前参与者不能执行该流程任务";

// ═══════════════════════════════════════════════════════
// 节点开关 ＋ 解析期硬校验（spec 02 §4.4，三条）
// ═══════════════════════════════════════════════════════

/// 节点是**任务节点**且开了 `dynamicCountersign`。
pub fn node_enabled(node: Option<&NodeModel>) -> bool {
    matches!(node, Some(n) if n.node_type == NodeType::Task && n.dynamic_countersign)
}

/// 三条解析期硬校验，**位序照 java `ModelParser` 收尾那一段**（双开 → 会签节点 → 申请节点）：
/// 与 `allowAddSign` 双开＝共用协议键的行为歧义；会签节点＝池推进与征询挂起打架；
/// 申请节点＝那手走实例撤回＋重新发起，不需要征询挂起。
pub fn validate_model(model: &ProcessModel) -> JeeflowResult<()> {
    for node in model
        .nodes
        .iter()
        .filter(|n| n.node_type == NodeType::Task && n.dynamic_countersign)
    {
        if node.allow_add_sign {
            return Err(JeeflowError::Business(msg_mutex_with_add_sign(&node.id)));
        }
        if node.perform_type() == 1 {
            return Err(JeeflowError::Business(msg_countersign_forbidden(&node.id)));
        }
        if model.is_first_task_node(&node.id) {
            return Err(JeeflowError::Business(msg_apply_forbidden(&node.id)));
        }
    }
    Ok(())
}

// ═══════════════════════════════════════════════════════
// 成员行判据（承重判据 1 ＋ 2）
// ═══════════════════════════════════════════════════════

/// 成员行＝parent 指向主办行**且同节点同名**的行（成员不能再拉人 ⇒ 无更深层级）。
///
/// ⚠️ 同名过滤不可省（承重判据 1）：主办办结推进后，下一节点行的 `task_parent_id` 同样是
/// 主办行 id（建单不变量），只认 parent 会把主办推进后的下游行误当成员——撤回腿会误废弃它们、
/// 第二次撤回会归错文件。
///
/// `host.task_id == 0`（尚未落库的草稿行）一律不算主办 ⇒ 返回空表，避免把"parent＝0"的
/// 无父行读成成员。
pub fn member_rows(tasks: &[ProcessTask], host: &ProcessTask) -> Vec<ProcessTask> {
    if host.task_id == 0 {
        return Vec::new();
    }
    tasks
        .iter()
        .filter(|t| {
            t.parent_task_id == Some(host.task_id)
                && t.task_name == host.task_name
                // issues/253：协办行（task_type=1）也满足 parent＋同节点同名，但它属另一族
                // ——征询收齐闸只数征询成员行（task_type=0），两闸各数各的（spec/04「与征询并存」）。
                && !crate::aidant::is_aidant_row(t)
        })
        .cloned()
        .collect()
}

/// 成员行判定：parent 指向**同节点同名**行（issues/247 §成员行建单规则；"成员行禁带拉人参数"的判据）。
/// issues/253：协办行（task_type=1）不是征询成员行——两族靠 task_type 分辨。
pub fn is_member_row(tasks: &[ProcessTask], row: &ProcessTask) -> bool {
    if crate::aidant::is_aidant_row(row) {
        return false;
    }
    match row.parent_task_id {
        None | Some(0) => false,
        Some(parent) => tasks
            .iter()
            .any(|t| t.task_id == parent && t.task_name == row.task_name),
    }
}

/// 闸门原料：在册成员行总数与已提交（FINISHED）数——**已废弃(99) 行（撤回腿清场的）不算在册**
/// （承重判据 2）；空集＝本行不是征询主办行。
pub fn count_members(tasks: &[ProcessTask], host: &ProcessTask) -> (usize, usize) {
    let mut total = 0usize;
    let mut submitted = 0usize;
    for t in member_rows(tasks, host) {
        if t.task_state == TaskState::Abandon.code() {
            continue;
        }
        total += 1;
        if t.task_state == TaskState::Finished.code() {
            submitted += 1;
        }
    }
    (total, submitted)
}

/// 未答成员行（DOING）＝闸门判据（"还有无 DOING 成员行"）与撤回接口的废弃集合。
pub fn pending_members(tasks: &[ProcessTask], host: &ProcessTask) -> Vec<ProcessTask> {
    member_rows(tasks, host)
        .into_iter()
        .filter(|t| t.task_state == TaskState::Doing.code())
        .collect()
}

// ═══════════════════════════════════════════════════════
// 轮次与账本
// ═══════════════════════════════════════════════════════

/// 主办行当前轮次＝账本条数末条的 `round`（每次拉人追加一条＝轮次推进；**撤回不消耗**）。
///
/// 读的是主办行自己的 `tf_addSignHistory`——征询模式下主办行未办结，行变量随行活到办结后；
/// 与后加签 [`add_sign::round_of`]（读成员行轮次戳）的写法对称。
pub fn last_round(host: &ProcessTask) -> i64 {
    let history = add_sign::history_of(host);
    match history.last() {
        Some(entry) => entry
            .as_object()
            .and_then(|entries| {
                entries
                    .iter()
                    .find(|(k, _)| k == "round")
                    .map(|(_, v)| v.clone())
            })
            .and_then(|v| match v {
                JsonValue::Number(n) => Some(n as i64),
                JsonValue::Str(s) => s.parse::<i64>().ok(),
                _ => None,
            }),
        None => None,
    }
    .unwrap_or(0)
}

/// 发起会签账本追加（只追加不覆盖），落**主办行**；`reason` 非空时进条目。
///
/// ⚠️ 调用方随后必须把主办行**再落一次库**（本栈聚合根与仓储行是两份拷贝，与
/// [`add_sign::append_history`] 同一条纪律；不补这一刀账本就丢）。
pub fn append_collect_history(
    host: &mut ProcessTask,
    operator: &str,
    to_actors: &[String],
    round: i64,
    reason: &str,
) {
    let mut entry = base_entry(operator, to_actors, round);
    if !reason.is_empty() {
        entry.push(("reason".to_string(), JsonValue::Str(reason.to_string())));
    }
    append(host, entry);
}

/// 撤回会签账本追加：`revoked＝true` 标记本条是撤回（未答废弃），`toActors`＝被撤的人；
/// 轮次不消耗（本条记当轮，不推进轮次）。
pub fn append_revoke_history(
    host: &mut ProcessTask,
    operator: &str,
    revoked_actors: &[String],
    round: i64,
    reason: &str,
) {
    let mut entry = base_entry(operator, revoked_actors, round);
    entry.push(("revoked".to_string(), JsonValue::Bool(true)));
    if !reason.is_empty() {
        entry.push(("reason".to_string(), JsonValue::Str(reason.to_string())));
    }
    append(host, entry);
}

/// 账本条目五键固定 camelCase（`round / fromActor / toActors / time / operator`），
/// 形状与 [`add_sign::append_history`] 逐字同族（同一本 `tf_addSignHistory`）。
fn base_entry(operator: &str, to_actors: &[String], round: i64) -> Vec<(String, JsonValue)> {
    vec![
        ("round".to_string(), JsonValue::Number(round as f64)),
        ("fromActor".to_string(), JsonValue::Str(operator.to_string())),
        (
            "toActors".to_string(),
            JsonValue::Array(to_actors.iter().map(|a| JsonValue::Str(a.clone())).collect()),
        ),
        ("time".to_string(), JsonValue::Str(current_time_str())),
        ("operator".to_string(), JsonValue::Str(operator.to_string())),
    ]
}

fn append(host: &mut ProcessTask, entry: Vec<(String, JsonValue)>) {
    let mut history: Vec<JsonValue> = match host.variables.get(add_sign::ADD_SIGN_HISTORY) {
        Some(JsonValue::Array(items)) => items.clone(),
        _ => Vec::new(),
    };
    history.push(JsonValue::Object(entry));
    host.variables
        .insert(add_sign::ADD_SIGN_HISTORY.to_string(), JsonValue::Array(history));
}

// ═══════════════════════════════════════════════════════
// 投影（spec 06 §2.7：主办行 ext.dynamicCountersign）
// ═══════════════════════════════════════════════════════

/// 征询主办行的 `ext.dynamicCountersign` 投影＝
/// `{active, round, total, submitted, actors:[{actor, taskId, state}]}`；**无成员行 ⇒ None**（不写键）。
///
/// 从 `task_parent_id` 现算、不落新存储；撤回清场（99）的行不进投影（承重判据 2），
/// 前端据 `active` 置灰执行按钮（未收齐时全部 submitType 置灰，唯一出口＝撤回接口）。
pub fn build_projection(tasks: &[ProcessTask], host: &ProcessTask) -> Option<JsonValue> {
    let members = member_rows(tasks, host);
    if members.is_empty() {
        return None;
    }
    let mut total = 0usize;
    let mut submitted = 0usize;
    let mut actors: Vec<JsonValue> = Vec::new();
    for m in members {
        if m.task_state == TaskState::Abandon.code() {
            continue;
        }
        total += 1;
        if m.task_state == TaskState::Finished.code() {
            submitted += 1;
        }
        actors.push(JsonValue::Object(vec![
            (
                "actor".to_string(),
                m.actor_ids
                    .first()
                    .map(|a| JsonValue::Str(a.clone()))
                    .unwrap_or(JsonValue::Null),
            ),
            ("taskId".to_string(), JsonValue::Number(m.task_id as f64)),
            ("state".to_string(), JsonValue::Number(m.task_state as f64)),
        ]));
    }
    Some(JsonValue::Object(vec![
        ("active".to_string(), JsonValue::Bool(submitted < total)),
        ("round".to_string(), JsonValue::Number(last_round(host) as f64)),
        ("total".to_string(), JsonValue::Number(total as f64)),
        ("submitted".to_string(), JsonValue::Number(submitted as f64)),
        ("actors".to_string(), JsonValue::Array(actors)),
    ]))
}

/// 读成员行上的征询原因戳（单行出证自足那一条；用例与投影都用这一支）。
pub fn reason_of(task: &ProcessTask) -> Option<String> {
    task.variables.get_str(KEY_ADD_SIGN_REASON).map(|s| s.to_string())
}
