//! 已办撤回（`allowRecall`，issues/246）· 窗口判据与撤回账本。
//!
//! 移植基准＝jeeflow-java 参考实现 `core/RecallSupport.java`（HEAD 09c2afe）＋
//! `core/JeeflowEngineImpl.recallTask`（判据位序＋落库序）＋ `domain/ProcessInstance.recallTask`；
//! 契约正文：
//! - spec/02 §4.4（节点开关 ＋ 两条解析期硬校验）
//! - spec/04「已办撤回」（前置与窗口判据／动作三件／事件／语义边界）
//! - spec/06 §2.7（协议键 `tf_recallHistory`）＋ §processTask/recall（五条逐字失败文案表）
//! - spec/11 §11.3 码 10 `TASK_RECALL`
//!
//! 本模块只放**判据与账本**（全部只读／纯函数）；聚合变更与落库编排在 `engine.rs::recall_task_sync`。
//!
//! **为什么判据一律排在任何状态变更之前**：本栈宿主未注册事务模板时是**裸跑**
//! （[`crate::engine::JeeflowEngineImpl::in_tx`] 的 None 分支），一旦先废弃下游行再判错，
//! 那些行就停在 99 而无人能改回来——"报错即零写入"与后加签写入口／退回守卫同一规矩
//! （对齐 java 把 `validateWindow` 放在任何 `updateTask` 之前的那一刀）。
//!
//! **窗口取宽口径**（owner 2026-11-03 拍，spec/04 §前置与窗口判据 第 3 条）：从目标行沿
//! `task_parent_id` 血缘产出的**全部下游行**中存在任何 FINISHED ⇒ 报错；否则返回其中仍 DOING
//! 的行供废弃。后加签行、动态会签成员行、并行分支多支都天然落在这条血缘闭包里，
//! 不必为它们各开一档判据；已废弃(99)／已撤回(30) 行既不拦也不重复废弃。

use crate::error::{JeeflowError, JeeflowResult};
use crate::json::JsonValue;
use crate::model::{current_time_str, ProcessTask, TaskState};
use crate::parser::{NodeModel, NodeType, ProcessModel};

// ═══════════════════════════════════════════════════════
// 键与逐字文案
// ═══════════════════════════════════════════════════════

/// 节点属性键：已办撤回开关（`properties` 平铺层，缺省 false；不接受 `ext` 嵌套，避免双源优先级）。
pub const PROP_ALLOW_RECALL: &str = "allowRecall";

/// 追加式账本键：每条撤回 append `{operator, time, recalledTaskId}`，落在**被撤回的旧行**变量上。
///
/// 原 FINISHED 行**一律不改写**（jeeflow 无独立历史表，任务行即审批历史，翻回 DOING＝改写
/// 已出证的记录），账本是那一行上唯一的追加痕迹；`recalledTaskId` 指向复活的新行
/// ⇒ 从历史记录可一步跳到唯一活待办。`tf_` 前缀使退回复活的血缘腿天然剔掉它
/// （见 `engine.rs::rollback_to_parent` 里 `k.starts_with("tf_")` 那一支）。
pub const KEY_RECALL_HISTORY: &str = "tf_recallHistory";

/// 逐字判据文案（spec 06 §processTask/recall 失败文案表；跨栈统一，一个字不许改）。
pub fn msg_window(task_name: &str) -> String {
    format!("任务[{}]的后续节点已办理，不能撤回", task_name)
}

/// 逐字判据文案（节点未开 `allowRecall`）。
pub fn msg_switch_off(node_name: &str) -> String {
    format!("节点[{}]未开启 allowRecall，不能撤回", node_name)
}

/// 逐字判据文案（目标行非 FINISHED）。
pub const MSG_NOT_FINISHED: &str = "任务非已完成状态，不可撤回";

/// 逐字判据文案（`operator` 非目标行参与者且非特权）。
pub const MSG_NO_PERMISSION: &str = "无权限撤回该任务";

/// 逐字判据文案（实例非 DOING）。
pub const MSG_INSTANCE_NOT_DOING: &str = "流程实例非进行中状态，不可撤回";

/// 逐字判据文案（实例行查不到）——与门面 `processInstance/withdraw` 一族同文案，不另造第二种。
pub const MSG_INSTANCE_NOT_FOUND: &str = "流程实例不存在";

/// 逐字判据文案（解析期禁配 · 会签节点，spec 02 §4.4）。
pub fn msg_countersign_forbidden(node_name: &str) -> String {
    format!("会签节点[{}]不允许配置 allowRecall：撤\"谁的一票\"语义不清", node_name)
}

/// 逐字判据文案（解析期禁配 · 申请节点＝start 直接后继，spec 02 §4.4）。
pub fn msg_first_task_forbidden(node_name: &str) -> String {
    format!(
        "首任务节点[{}]不允许配置 allowRecall：申请节点走实例撤回＋重新发起，不需要同实例复活",
        node_name
    )
}

// ═══════════════════════════════════════════════════════
// 节点开关
// ═══════════════════════════════════════════════════════

/// 节点是**任务节点**且开了 `allowRecall`（属性缺失／非任务节点 ⇒ false）。
///
/// 非法值的解析期拦截在 [`validate_model`]（值在模型里就已经是布尔，运行期这一判只是第二道），
/// 形状与 `add_sign::validate_model` 同族。
pub fn node_enabled(node: Option<&NodeModel>) -> bool {
    matches!(node, Some(n) if n.node_type == NodeType::Task && n.allow_recall)
}

// ═══════════════════════════════════════════════════════
// 解析期硬校验（spec 02 §4.4，两条）
// ═══════════════════════════════════════════════════════

/// 两条解析期硬校验：会签节点撤"谁的一票"语义不清；申请节点走实例撤回＋重新发起（既有通道），
/// 不需要同实例复活。配了就直接报错、不做运行时降级（静默跑下去会让配置者以为开关生效）。
///
/// 由 `parser::ModelParser::parse` 在节点与边都接好之后调用（"是不是首任务节点"要沿 start
/// 的出边看），位序与文案逐字对齐 java `ModelParser` 收尾的三段（后加签 → 已办撤回 → 动态会签）。
pub fn validate_model(model: &ProcessModel) -> JeeflowResult<()> {
    for node in model.nodes.iter().filter(|n| n.node_type == NodeType::Task && n.allow_recall) {
        if node.perform_type() == 1 {
            return Err(JeeflowError::Business(msg_countersign_forbidden(&node.id)));
        }
        if model.is_first_task_node(&node.id) {
            return Err(JeeflowError::Business(msg_first_task_forbidden(&node.id)));
        }
    }
    Ok(())
}

// ═══════════════════════════════════════════════════════
// 窗口判据（必须排在任何状态变更之前）
// ═══════════════════════════════════════════════════════

/// 按 `task_parent_id` 血缘收集 target 的全部后代行（对齐 java `collectDescendants`：
/// frontier 后进先出，收集的是**整棵子树**而非只下一层）。
///
/// 血缘在建单不变量里必写（0＝无父，见 `engine.rs::persist_tasks`），所以只沿 parent 走
/// 就覆盖到"该行办结产出的一切"——后加签续建行、动态会签成员行、并行分支的多支都在里面。
pub fn collect_descendants(tasks: &[ProcessTask], ancestor_id: i64) -> Vec<ProcessTask> {
    let mut result: Vec<ProcessTask> = Vec::new();
    let mut frontier: Vec<i64> = vec![ancestor_id];
    while let Some(parent) = frontier.pop() {
        for t in tasks {
            if t.parent_task_id == Some(parent) {
                result.push(t.clone());
                frontier.push(t.task_id);
            }
        }
    }
    result
}

/// 窗口判据（宽口径）：下游**存在任何 FINISHED** ⇒ 逐字报错；否则返回其中仍 DOING 的行（供废弃）。
///
/// 已废弃(99)／已撤回(30) 行既不拦也不重复废弃——判据只看"下一手有没有被办掉"。
pub fn validate_window(tasks: &[ProcessTask], target: &ProcessTask) -> JeeflowResult<Vec<ProcessTask>> {
    let mut doing: Vec<ProcessTask> = Vec::new();
    for t in collect_descendants(tasks, target.task_id) {
        if t.task_state == TaskState::Finished.code() {
            return Err(JeeflowError::Business(msg_window(&target.task_name)));
        }
        if t.task_state == TaskState::Doing.code() {
            doing.push(t);
        }
    }
    Ok(doing)
}

// ═══════════════════════════════════════════════════════
// 账本
// ═══════════════════════════════════════════════════════

/// 账本追加（只追加不覆盖），落**被撤回的旧行**变量。
///
/// 三键固定 camelCase：`operator / time / recalledTaskId`（形状照 `tf_transferHistory` 那族，
/// 时间 `"yyyy-MM-dd HH:mm:ss"`＝spec §2.4 全生态时间格式，与 `tf_addSignHistory` 同尺），
/// 走本引擎的共用时钟 [`crate::model::current_time_str`]。
pub fn append_recall_history(
    target: &mut ProcessTask,
    operator: &str,
    resurrected_task_id: i64,
) {
    let mut history: Vec<JsonValue> = match target.variables.get(KEY_RECALL_HISTORY) {
        Some(JsonValue::Array(items)) => items.clone(),
        _ => Vec::new(),
    };
    history.push(JsonValue::Object(vec![
        ("operator".to_string(), JsonValue::Str(operator.to_string())),
        ("time".to_string(), JsonValue::Str(current_time_str())),
        ("recalledTaskId".to_string(), JsonValue::Number(resurrected_task_id as f64)),
    ]));
    target
        .variables
        .insert(KEY_RECALL_HISTORY.to_string(), JsonValue::Array(history));
}

/// 读撤回账本（测试与投影用）。
pub fn recall_history_of(task: &ProcessTask) -> Vec<JsonValue> {
    match task.variables.get(KEY_RECALL_HISTORY) {
        Some(JsonValue::Array(items)) => items.clone(),
        _ => Vec::new(),
    }
}
