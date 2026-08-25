//! Interceptors — 7 built-in AssignmentHandlers + field permission filtering.
//! Registration names = Java FQCN (cross-language flow JSON compatibility).

use crate::context::ServiceContext;
use crate::engine::Execution;
use crate::error::JeeflowResult;
use crate::json::FlowData;
use crate::spi::{AssignmentHandler, OrgUserProvider, UserProvider};
use std::collections::HashMap;
use std::sync::Arc;

// ═══════════════════════════════════════════════════════
// 7 Built-in AssignmentHandlers
// ═══════════════════════════════════════════════════════

/// 1. OperatorAssignmentHandler — returns the process initiator.
pub struct OperatorAssignmentHandler;

impl AssignmentHandler for OperatorAssignmentHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        Ok(exec.process_instance.operator.clone())
    }
}

// 组织维度 handler 公共依赖（对齐 Go orgBase / Java OrgUserAssignmentHandlers：
// 注册时捕获 provider，assign() 同步调用——引擎执行链在 tokio worker 线程上，
// 与 add_user_info 的 user_provider 同步调用同路径）。
#[derive(Clone)]
struct OrgAssignBase {
    user: Option<Arc<dyn UserProvider>>,
    org: Option<Arc<dyn OrgUserProvider>>,
}

impl OrgAssignBase {
    /// userId 的部门 id（查不到/出错 → 空串，对齐 Java/Go 返回 null → 节点跳过）。
    fn dept_id_of(&self, user_id: &str) -> String {
        if user_id.is_empty() {
            return String::new();
        }
        let Some(up) = &self.user else {
            return String::new();
        };
        match up.get_user(user_id) {
            Ok(Some(u)) => u.dept_id,
            _ => String::new(),
        }
    }

    /// 部门领导（main=true 分管领导）id 逗号串；查不到 → 空串。
    fn by_dept(&self, dept_id: &str, main: bool) -> String {
        if dept_id.is_empty() {
            return String::new();
        }
        let Some(org) = &self.org else {
            return String::new();
        };
        let ids = if main {
            org.find_dept_main_leaders(dept_id)
        } else {
            org.find_dept_leaders(dept_id)
        };
        match ids {
            Ok(v) if !v.is_empty() => v.join(","),
            _ => String::new(),
        }
    }
}

/// 2. ApplicantDeptLeaderAssignmentHandler — initiator's department leader.
pub struct ApplicantDeptLeaderAssignmentHandler {
    base: OrgAssignBase,
}

impl AssignmentHandler for ApplicantDeptLeaderAssignmentHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        let dept_id = self.base.dept_id_of(&exec.process_instance.operator);
        Ok(self.base.by_dept(&dept_id, false))
    }
}

/// 3. ApplicantDeptMainLeaderAssignmentHandler — initiator's department main leader.
pub struct ApplicantDeptMainLeaderAssignmentHandler {
    base: OrgAssignBase,
}

impl AssignmentHandler for ApplicantDeptMainLeaderAssignmentHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        let dept_id = self.base.dept_id_of(&exec.process_instance.operator);
        Ok(self.base.by_dept(&dept_id, true))
    }
}

/// 4. DeptLeaderAssignmentHandler — current operator's department leader.
pub struct DeptLeaderAssignmentHandler {
    base: OrgAssignBase,
}

impl AssignmentHandler for DeptLeaderAssignmentHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        let dept_id = self.base.dept_id_of(&exec.operator);
        Ok(self.base.by_dept(&dept_id, false))
    }
}

/// 5. DeptMainLeaderAssignmentHandler — current operator's department main leader.
pub struct DeptMainLeaderAssignmentHandler {
    base: OrgAssignBase,
}

impl AssignmentHandler for DeptMainLeaderAssignmentHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        let dept_id = self.base.dept_id_of(&exec.operator);
        Ok(self.base.by_dept(&dept_id, true))
    }
}

/// 6. FormFieldAssigneeHandler — assignee from form field value (variable name = node id).
pub struct FormFieldAssigneeHandler;

impl AssignmentHandler for FormFieldAssigneeHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        // 对齐 Go findFieldValue / Java：f_ 前缀优先、裸名兜底（issues/71 跨语言顺序）。
        // 引擎 resume 已把 instance.variables 并入 exec.args（见 engine.rs），
        // 故发起时提交的 f_<nodeId> 在此可达（L3 S11-B）。
        if let Some(node) = &exec.current_node {
            let f_key = format!("f_{}", node.id);
            if let Some(val) = exec.args.get_str(&f_key) {
                return Ok(val.to_string());
            }
            if let Some(val) = exec.args.get_str(&node.id) {
                return Ok(val.to_string());
            }
        }
        Ok(String::new())
    }
}

/// 7. TaskRoleAssigneeHandler — assignee by task node id as role code (对齐 Go FindByRole(node.ID)).
pub struct TaskRoleAssigneeHandler {
    org: Option<Arc<dyn OrgUserProvider>>,
}

impl AssignmentHandler for TaskRoleAssigneeHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        let Some(node) = &exec.current_node else {
            return Ok(String::new());
        };
        let Some(org) = &self.org else {
            return Ok(String::new());
        };
        match org.find_by_role(&node.id) {
            Ok(v) if !v.is_empty() => Ok(v.join(",")),
            _ => Ok(String::new()),
        }
    }
}

// ═══════════════════════════════════════════════════════
// Field permission filtering
// ═══════════════════════════════════════════════════════

/// Filter fields by permission settings (spec/09 §4.2).
/// Returns the filtered args (only editable fields pass through).
pub fn filter_fields_by_permission(args: &FlowData, permissions: &HashMap<String, i32>) -> FlowData {
    let mut filtered = FlowData::new();

    for (key, value) in args.iter() {
        if key.starts_with("f_") {
            // Check PERMISSION_f_{field}
            let perm_key = format!("PERMISSION_{}", key);
            let perm_key_no_prefix = format!("PERMISSION_{}", &key[2..]); // without f_

            let perm = permissions.get(&perm_key)
                .or_else(|| permissions.get(&perm_key_no_prefix))
                .copied()
                .unwrap_or(2); // default = editable

            match perm {
                1 => { /* read-only — skip */ }
                2 => { filtered.insert(key.clone(), value.clone()); } // editable
                3 => { /* hidden — skip */ }
                _ => { filtered.insert(key.clone(), value.clone()); }
            }
        } else if key.starts_with("tf_") {
            // Task form fields — always pass through
            filtered.insert(key.clone(), value.clone());
        } else {
            // Non-form fields — pass through
            filtered.insert(key.clone(), value.clone());
        }
    }

    filtered
}

/// Register all 7 built-in assignment handlers into a ServiceContext.
/// 组织维度 handler 在注册时捕获 user/org provider（对齐 Go RegisterBuiltinAssignments：
/// provider 必须先注册——wf_factory 的 with_user_provider/with_org_user_provider 在本函数之前）。
pub fn register_builtin_assignment_handlers(ctx: &mut ServiceContext) {
    let base = OrgAssignBase {
        user: ctx.user_provider.clone(),
        org: ctx.org_user_provider.clone(),
    };
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OperatorAssignmentHandler",
        Arc::new(OperatorAssignmentHandler));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptLeaderAssignmentHandler",
        Arc::new(ApplicantDeptLeaderAssignmentHandler { base: base.clone() }));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptMainLeaderAssignmentHandler",
        Arc::new(ApplicantDeptMainLeaderAssignmentHandler { base: base.clone() }));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptLeaderAssignmentHandler",
        Arc::new(DeptLeaderAssignmentHandler { base: base.clone() }));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptMainLeaderAssignmentHandler",
        Arc::new(DeptMainLeaderAssignmentHandler { base }));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.FormFieldAssigneeHandler",
        Arc::new(FormFieldAssigneeHandler));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskRoleAssigneeHandler",
        Arc::new(TaskRoleAssigneeHandler { org: ctx.org_user_provider.clone() }));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;
    use crate::parser::*;

    #[test]
    fn test_operator_assignment_handler() {
        let define = ProcessDefine {
            id: 1, name: "test".into(), display_name: "Test".into(),
            define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
            version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        let instance = ProcessInstance::create(&define, "user1", &FlowData::new());
        let model = ProcessModel {
            name: "test".into(), display_name: "Test".into(),
            model_type: "approval".into(), expire_time: None,
            persist_mode: None, rel_table_name: None,
            nodes: vec![], edges: vec![],
        };
        let exec = Execution::new(instance, model, define, "user1", FlowData::new());
        let handler = OperatorAssignmentHandler;
        let result = handler.assign(&exec).unwrap();
        assert_eq!(result, "user1");
    }

    #[test]
    fn test_form_field_assignee() {
        let define = ProcessDefine {
            id: 1, name: "test".into(), display_name: "Test".into(),
            define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
            version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        let instance = ProcessInstance::create(&define, "user1", &FlowData::new());
        let node = NodeModel {
            id: "node1".into(), node_type: NodeType::Task,
            display_name: "Node 1".into(),
            properties: std::collections::HashMap::new(),
        };
        let mut args = FlowData::new();
        args.insert_str("node1", "user2,user3");
        let model = ProcessModel {
            name: "test".into(), display_name: "Test".into(),
            model_type: "approval".into(), expire_time: None,
            persist_mode: None, rel_table_name: None,
            nodes: vec![], edges: vec![],
        };
        let mut exec = Execution::new(instance, model, define, "user1", args);
        exec.current_node = Some(node);
        let handler = FormFieldAssigneeHandler;
        let result = handler.assign(&exec).unwrap();
        assert_eq!(result, "user2,user3");
    }

    #[test]
    fn test_field_permission_filter() {
        let mut args = FlowData::new();
        args.insert_str("f_name", "张三");
        args.insert_str("f_amount", "1000");
        args.insert_str("f_status", "pending");
        args.insert_str("operator", "user1");

        let mut perms = HashMap::new();
        perms.insert("PERMISSION_f_name".to_string(), 2);    // editable
        perms.insert("PERMISSION_f_amount".to_string(), 1);   // read-only
        perms.insert("PERMISSION_f_status".to_string(), 3);   // hidden

        let filtered = filter_fields_by_permission(&args, &perms);
        assert!(filtered.contains_key("f_name"));     // editable passes
        assert!(!filtered.contains_key("f_amount"));  // read-only blocked
        assert!(!filtered.contains_key("f_status"));  // hidden blocked
        assert!(filtered.contains_key("operator"));   // non-form passes
    }

    #[test]
    fn test_register_builtins() {
        let mut ctx = ServiceContext::new();
        register_builtin_assignment_handlers(&mut ctx);
        assert_eq!(ctx.assignment_handlers.len(), 7);
        assert!(ctx.find_assignment_handler(
            "com.mldong.jeeflow.interceptor.impl.OperatorAssignmentHandler"
        ).is_some());
    }
}
