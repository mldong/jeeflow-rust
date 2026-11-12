//! 动态协办（aidant，issues/253）· 协办行判据、账本与投影。
//!
//! 移植基准＝jeeflow-java 参考实现 `core/AidantSupport.java`（issues/253 实施轮）＋
//! `core/JeeflowEngineImpl.aidantTask / completeAidantWithoutDrive`；契约正文：
//! - spec/04「动态协办（aidant）」（派生规则／推进闸与等待闸／能力边界／留痕与投影）
//! - spec/06 §2.7（协议键 `tf_aidantHistory`／`tf_aidantReason`／`tf_aidantWait` ＋ 投影
//!   `ext.aidant`）＋ §processTask/aidant（协办六行逐字失败文案）
//!
//! 本模块只放**判据、账本与投影**（全部只读／纯函数）；派生建单由引擎收口
//! （`engine.rs::aidant_task_locked`，走既有 `persist_tasks`＝save_task＋全量写参与者＋委托并入＋fire 码 3），
//! 推进闸/等待闸/离开节点清场由引擎四条办理入口调用。
//!
//! **与征询（[`crate::collect`]）的分辨判据＝task_type**：两族子行都是 parent 指向主办行＋
//! 同节点同名，协办行 task_type=1（[`TaskType::Assistant`]）、征询成员行 task_type=0。
//! 等待闸只数协办行、征询收齐闸只数成员行，各数各的（spec/04「与征询允许同节点并存」）。
//!
//! **无节点开关**：协办档只由运行期 `processTask/aidant` 派生，节点级 taskType=1 解析期禁配
//! （issues/252，见 [`crate::task_type`]）。

use crate::error::{JeeflowError, JeeflowResult};
use crate::json::JsonValue;
use crate::model::{current_time_str, ProcessInstance, ProcessTask, TaskState, TaskType};

// ═══════════════════════════════════════════════════════
// 协议键与逐字文案
// ═══════════════════════════════════════════════════════

/// 协办账本（追加式，落**主办行**变量）：每次派生 append
/// `{round, fromActor, toActors, reason, time, operator}`；离开节点清场时追加一条带
/// `abandoned=true` 的条目（照征询撤回的 `revoked`）。＝java `FlowConst.AIDANT_HISTORY`。
pub const AIDANT_HISTORY: &str = "tf_aidantHistory";

/// 派生原因（选填）盖每条协办行（单行出证自足）；**不占 `tf_approvalComment`**。
pub const AIDANT_REASON: &str = "tf_aidantReason";

/// 等待闸开关：派生时把 `waitCompletion` 盖在主办行变量上，缺省 false＝不等。
/// `tf_` 前缀使实例合并自动剥键（不传播）；投影 `ext.aidant.wait` 同源。
pub const AIDANT_WAIT: &str = "tf_aidantWait";

/// 逐字判据文案（跨栈统一；spec/06 失败文案表「协办六行」）。
pub const MSG_NO_ACTORS: &str = "actorIds 必填";
pub const MSG_NO_PERMISSION: &str = "无权限派生协办任务";
pub const MSG_NOT_DOING: &str = "任务非进行中，不可派生协办";
pub const MSG_DUPLICATE: &str = "该人已有未办结的协办任务";
pub const MSG_NO_RECURSION: &str = "协办任务不能再派生协办";
pub const MSG_TASK_NOT_FOUND: &str = "任务不存在";

/// 等待闸未齐逐字文案（`{已办结}/{总数}` 两枚计数，跳过 99 行）。
pub fn msg_wait_pending(node_name: &str, submitted: usize, total: usize) -> String {
    format!(
        "节点[{}]协办未办结（已办结 {}/{}），不能办理",
        node_name, submitted, total
    )
}

// ═══════════════════════════════════════════════════════
// 协办行判据
// ═══════════════════════════════════════════════════════

/// 协办行判定＝task_type=1（Assistant）。这是两族子行唯一的分辨判据。
pub fn is_aidant_row(row: &ProcessTask) -> bool {
    row.task_type == TaskType::Assistant.code()
}

/// 协办行＝parent 指向主办行**且同节点同名且 task_type=1** 的行。
///
/// ⚠️ 同名过滤不可省（照 [`crate::collect::member_rows`] 的理由）：主办办结推进后，下一节点行的
/// parent 同样是主办行 id（建单不变量），只认 parent 会把下游行误当协办行。task_type 过滤同样
/// 不可省：征询成员行也满足 parent＋同名，混进来会让等待闸把"意见未收齐"误报成"协办未办结"。
///
/// `host.task_id == 0`（尚未落库的草稿行）一律不算主办 ⇒ 返回空表。
pub fn aidant_rows(tasks: &[ProcessTask], host: &ProcessTask) -> Vec<ProcessTask> {
    if host.task_id == 0 {
        return Vec::new();
    }
    let mut rows: Vec<ProcessTask> = tasks
        .iter()
        .filter(|t| {
            is_aidant_row(t)
                && t.parent_task_id == Some(host.task_id)
                && t.task_name == host.task_name
        })
        .cloned()
        .collect();
    // 行序钉为建单序：MemoryRepository 遍历序不定，投影/账本对位按 actorIds 入参序才跨栈同形
    rows.sort_by_key(|t| t.task_id);
    rows
}

/// 等待闸原料：在册协办行总数与已办结（FINISHED）数——**已废弃(99) 行（离开节点清场的）不算在册**
/// （spec/04「计数跳过已废弃(99) 行」）。
pub fn count_aidant(tasks: &[ProcessTask], host: &ProcessTask) -> (usize, usize) {
    let mut total = 0usize;
    let mut submitted = 0usize;
    for t in aidant_rows(tasks, host) {
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

/// 未办结协办行（DOING）＝等待闸判据集合，也是离开节点时的清场集合。
pub fn pending_aidant(tasks: &[ProcessTask], host: &ProcessTask) -> Vec<ProcessTask> {
    aidant_rows(tasks, host)
        .into_iter()
        .filter(|t| t.task_state == TaskState::Doing.code())
        .collect()
}

/// 等待闸开关（`waitCompletion`）：派生时盖在**主办行**变量上，缺省 false＝不等。
/// 多轮派生时后一次的值生效（投影 `ext.aidant.wait` 同源）。布尔与 "true"/"false" 串两形都认。
pub fn wait_of(host: &ProcessTask) -> bool {
    match host.variables.get(AIDANT_WAIT) {
        Some(JsonValue::Bool(b)) => *b,
        Some(JsonValue::Str(s)) => s.trim().eq_ignore_ascii_case("true"),
        _ => false,
    }
}

/// 等待闸（spec/04「推进闸与等待闸」）：`waitCompletion=true` 时主办行办结前判"不存在 DOING 协办行"，
/// 未齐 ⇒ 逐字文案报错、零写入。协办行自身不受本闸约束（它办结本来就不驱动流程）；
/// `waitCompletion=false`（缺省）原样放行。
///
/// ⚠️ 必须由引擎排在 `complete_task` **之前**——本栈宿主未注册事务模板时是裸跑，
/// 判据后置时"报错"那一次调用结束后主办待办已经消失（与后加签/退回守卫同一规矩）。
pub fn gate_wait(tasks: &[ProcessTask], host: &ProcessTask) -> JeeflowResult<()> {
    if is_aidant_row(host) || !wait_of(host) {
        return Ok(());
    }
    if pending_aidant(tasks, host).is_empty() {
        return Ok(());
    }
    let (total, submitted) = count_aidant(tasks, host);
    Err(JeeflowError::Business(msg_wait_pending(
        &host.task_name,
        submitted,
        total,
    )))
}

/// 离开节点时清场未办结协办行（spec/04：`waitCompletion=false` 时主办可随时推进，
/// 未办结协办行随废弃腿清场、账本记废弃，不留孤儿待办）。
///
/// 只对协办行（task_type=1）动手，且**不要求节点开任何开关**——协办无节点开关，普通节点上的
/// 协办行同样要清。按 parent 分组，废弃事实追加到各自主办行的账本上。幂等：已废弃(99)/已办结(20)
/// 的行不再命中，重复调用不重复记账。
///
/// 返回**改过的行 id**（被废弃的协办行 ＋ 记了账的主办行）：本栈聚合根与仓储行是两份拷贝，
/// 调用方须把这些行逐条 `update_task` 写回（与 [`crate::add_sign::abandon_doing_on_node`] 同纪律）。
pub fn abandon_pending_on_node(
    instance: &mut ProcessInstance,
    node_name: &str,
    operator: &str,
) -> Vec<i64> {
    // 本节点待清场的协办行下标（行序）
    let targets: Vec<usize> = instance
        .tasks
        .iter()
        .enumerate()
        .filter(|(_, t)| t.task_name == node_name && t.is_doing() && is_aidant_row(t))
        .map(|(i, _)| i)
        .collect();
    if targets.is_empty() {
        return Vec::new();
    }
    // 按 parent 分组（保序＝java LinkedHashMap 的行序）
    let mut groups: Vec<(Option<i64>, Vec<usize>)> = Vec::new();
    for i in targets {
        let parent = instance.tasks[i].parent_task_id;
        if let Some(g) = groups.iter_mut().find(|(p, _)| *p == parent) {
            g.1.push(i);
        } else {
            groups.push((parent, vec![i]));
        }
    }
    let mut changed: Vec<i64> = Vec::new();
    for (parent, idxs) in groups {
        let mut actors: Vec<String> = Vec::new();
        for i in &idxs {
            let t = &mut instance.tasks[*i];
            let _ = t.abandon();
            t.update_user = Some(operator.to_string());
            actors.push(t.actor_ids.first().cloned().unwrap_or_default());
            changed.push(t.task_id);
        }
        if let Some(p) = parent {
            if let Some(h) = instance.tasks.iter_mut().find(|t| t.task_id == p) {
                let round = last_round(h);
                append_abandon_history(h, operator, &actors, round);
                changed.push(h.task_id);
            }
        }
    }
    changed
}

// ═══════════════════════════════════════════════════════
// 轮次与账本
// ═══════════════════════════════════════════════════════

/// 主办行当前轮次＝账本条数末条的 `round`（每次派生追加一条＝轮次推进；**清场记账不消耗轮次**）。
pub fn last_round(host: &ProcessTask) -> i64 {
    let history = history_of(host);
    for entry in history.iter().rev() {
        if let Some(obj) = entry.as_object() {
            for (k, v) in obj {
                if k == "round" {
                    match v {
                        JsonValue::Number(n) => return *n as i64,
                        JsonValue::Str(s) => {
                            if let Ok(n) = s.parse::<i64>() {
                                return n;
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    0
}

/// 派生账本追加（只追加不覆盖），落**主办行**变量；键序锁定
/// `{round, fromActor, toActors, reason, time, operator}`（spec/06 §2.7），reason 空串兜底、
/// time 一律 §2.4 字符串。主办行派生时不办结，行变量合并序保证账本活到办结后。
///
/// ⚠️ 调用方随后必须把主办行**再落一次库**（聚合根与仓储行两份拷贝，与
/// [`crate::collect::append_collect_history`] 同一条纪律）。
pub fn append_aidant_history(
    host: &mut ProcessTask,
    operator: &str,
    to_actors: &[String],
    round: i64,
    reason: &str,
) {
    append(host, base_entry(operator, to_actors, round, reason));
}

/// 清场账本追加：`abandoned=true` 标记本条是"离开节点废弃"（形状照征询撤回条目的 `revoked=true`），
/// 六键原序不动，`toActors`＝被废弃的人。
pub fn append_abandon_history(
    host: &mut ProcessTask,
    operator: &str,
    abandoned_actors: &[String],
    round: i64,
) {
    let mut entry = base_entry(operator, abandoned_actors, round, "");
    entry.push(("abandoned".to_string(), JsonValue::Bool(true)));
    append(host, entry);
}

/// 账本条目六键固定 camelCase（`round / fromActor / toActors / reason / time / operator`），
/// 键序逐字照 java LinkedHashMap（reason 在 time 之前）。
fn base_entry(
    operator: &str,
    to_actors: &[String],
    round: i64,
    reason: &str,
) -> Vec<(String, JsonValue)> {
    vec![
        ("round".to_string(), JsonValue::Number(round as f64)),
        ("fromActor".to_string(), JsonValue::Str(operator.to_string())),
        (
            "toActors".to_string(),
            JsonValue::Array(to_actors.iter().map(|a| JsonValue::Str(a.clone())).collect()),
        ),
        ("reason".to_string(), JsonValue::Str(reason.to_string())),
        ("time".to_string(), JsonValue::Str(current_time_str())),
        ("operator".to_string(), JsonValue::Str(operator.to_string())),
    ]
}

fn append(host: &mut ProcessTask, entry: Vec<(String, JsonValue)>) {
    let mut history: Vec<JsonValue> = match host.variables.get(AIDANT_HISTORY) {
        Some(JsonValue::Array(items)) => items.clone(),
        _ => Vec::new(),
    };
    history.push(JsonValue::Object(entry));
    host.variables
        .insert(AIDANT_HISTORY.to_string(), JsonValue::Array(history));
}

/// 读账本（测试与投影用）；无该键／脏值 ⇒ 空表。
pub fn history_of(task: &ProcessTask) -> Vec<JsonValue> {
    match task.variables.get(AIDANT_HISTORY) {
        Some(JsonValue::Array(items)) => items.clone(),
        _ => Vec::new(),
    }
}

/// 读协办行上的派生原因戳（单行出证自足那一条）。
pub fn reason_of(task: &ProcessTask) -> Option<String> {
    task.variables.get_str(AIDANT_REASON).map(|s| s.to_string())
}

// ═══════════════════════════════════════════════════════
// 投影（spec/06 §2.7：主办行 ext.aidant）
// ═══════════════════════════════════════════════════════

/// 主办行 `ext.aidant` 投影＝`{active, round, total, submitted, actors:[{actor, taskId, state}], wait}`；
/// **无协办行 ⇒ None**（不写键）。从 `task_parent_id`＋`task_type` 现算、不落新存储；
/// 清场（99）的行不进投影（与征询投影同口径）。
pub fn build_projection(tasks: &[ProcessTask], host: &ProcessTask) -> Option<JsonValue> {
    let rows = aidant_rows(tasks, host);
    if rows.is_empty() {
        return None;
    }
    let mut total = 0usize;
    let mut submitted = 0usize;
    let mut actors: Vec<JsonValue> = Vec::new();
    for t in rows {
        if t.task_state == TaskState::Abandon.code() {
            continue;
        }
        total += 1;
        if t.task_state == TaskState::Finished.code() {
            submitted += 1;
        }
        actors.push(JsonValue::Object(vec![
            (
                "actor".to_string(),
                t.actor_ids
                    .first()
                    .map(|a| JsonValue::Str(a.clone()))
                    .unwrap_or(JsonValue::Null),
            ),
            ("taskId".to_string(), JsonValue::Number(t.task_id as f64)),
            ("state".to_string(), JsonValue::Number(t.task_state as f64)),
        ]));
    }
    Some(JsonValue::Object(vec![
        ("active".to_string(), JsonValue::Bool(submitted < total)),
        ("round".to_string(), JsonValue::Number(last_round(host) as f64)),
        ("total".to_string(), JsonValue::Number(total as f64)),
        ("submitted".to_string(), JsonValue::Number(submitted as f64)),
        ("actors".to_string(), JsonValue::Array(actors)),
        ("wait".to_string(), JsonValue::Bool(wait_of(host))),
    ]))
}