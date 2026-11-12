//! 节点级 `taskType` 的**解析期禁配**（spec 02 §4.4，issues/252）。
//!
//! 协办(1) 档只由运行期 `processTask/aidant` 派生（issues/253，尚未实现）；节点上配协办
//! 在 snaker 原义下是死路——协办任务办结**不产生执行对象**、跳转吃同一道闸 ⇒ 实例仍运行中
//! 却零待办无后继（流程悬挂）。所以这一档在解析期就拦，**不是运行时降级**。
//!
//! ⚠️ 本栈容错集合现读（与 java/c# 不同，判据按本栈实际折值写）：
//! [`crate::parser::NodeModel::task_type`] 走 `prop_i64` → [`crate::json::JsonValue::as_i64`]
//! （JSON 数字取整 / 串按 `parse::<i64>`），**没有 codeOf 别名表**（`model::TaskType` 只有
//! `from_code`，不认枚举名与中文名）。⇒ 能折成协办(1) 的只有数字档：`1`、`"1"`、`"+1"`、
//! `1.9`（Number 取整）这类；`"Aidant"`／`"SECONDARY"`／`"协办"` 解不出整数 ⇒ 仍静默回落
//! 主办(0)。
//!
//! 边界（issues/252 §5-3）：**只拦协办这一档**，其余不可识别值维持既有静默回落，
//! 不扩成 fail-fast（issues/245 口径：配置类错误优先静默降级）。
//! RECORD(2) 与 custom 记录行不受影响（本校验只看 [`NodeType::Task`]）。

use crate::error::{JeeflowError, JeeflowResult};
use crate::model::TaskType;
use crate::parser::{NodeType, ProcessModel};

/// 逐字判据文案（解析期禁配 · 节点级协办，spec 02 §4.4）。节点名取节点 **id**
/// （与 `add_sign`／`recall`／`collect` 三族同尺，不取 display_name）。
pub fn msg_assist_forbidden(node_name: &str) -> String {
    format!("节点[{}]不允许配置协办任务类型（taskType=1）", node_name)
}

/// 节点级协办禁配：任务节点 `taskType` 折成协办(1) 一律解析期报错。
///
/// 折值单点在 [`crate::parser::NodeModel::task_type`]，本函数**不写第二份判据**
/// （避免"禁配集合"与"实际折值集合"两处演进漂移）。
pub fn validate_model(model: &ProcessModel) -> JeeflowResult<()> {
    for node in model.nodes.iter().filter(|n| n.node_type == NodeType::Task) {
        if node.task_type() == TaskType::Assistant.code() {
            return Err(JeeflowError::Business(msg_assist_forbidden(&node.id)));
        }
    }
    Ok(())
}