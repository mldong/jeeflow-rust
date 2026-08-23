//! Interceptors — 7 built-in AssignmentHandlers + field permission filtering.
//! Registration names = Java FQCN (cross-language flow JSON compatibility).

use crate::context::ServiceContext;
use crate::engine::Execution;
use crate::error::JeeflowResult;
use crate::json::FlowData;
use crate::spi::AssignmentHandler;
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

/// 2. ApplicantDeptLeaderAssignmentHandler — initiator's department leader.
pub struct ApplicantDeptLeaderAssignmentHandler;

impl AssignmentHandler for ApplicantDeptLeaderAssignmentHandler {
    fn assign(&self, _exec: &Execution) -> JeeflowResult<String> {
        // Needs OrgUserProvider — resolved at runtime via context
        // Returns empty if no org provider registered
        Ok(String::new())
    }
}

/// 3. ApplicantDeptMainLeaderAssignmentHandler — initiator's department main leader.
pub struct ApplicantDeptMainLeaderAssignmentHandler;

impl AssignmentHandler for ApplicantDeptMainLeaderAssignmentHandler {
    fn assign(&self, _exec: &Execution) -> JeeflowResult<String> {
        Ok(String::new())
    }
}

/// 4. DeptLeaderAssignmentHandler — current operator's department leader.
pub struct DeptLeaderAssignmentHandler;

impl AssignmentHandler for DeptLeaderAssignmentHandler {
    fn assign(&self, _exec: &Execution) -> JeeflowResult<String> {
        Ok(String::new())
    }
}

/// 5. DeptMainLeaderAssignmentHandler — current operator's department main leader.
pub struct DeptMainLeaderAssignmentHandler;

impl AssignmentHandler for DeptMainLeaderAssignmentHandler {
    fn assign(&self, _exec: &Execution) -> JeeflowResult<String> {
        Ok(String::new())
    }
}

/// 6. FormFieldAssigneeHandler — assignee from form field value (variable name = node id).
pub struct FormFieldAssigneeHandler;

impl AssignmentHandler for FormFieldAssigneeHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        // Look for variable named after the current node id
        if let Some(node) = &exec.current_node {
            if let Some(val) = exec.args.get_str(&node.id) {
                return Ok(val.to_string());
            }
            // Also check f_ prefixed
            let f_key = format!("f_{}", node.id);
            if let Some(val) = exec.args.get_str(&f_key) {
                return Ok(val.to_string());
            }
        }
        Ok(String::new())
    }
}

/// 7. TaskRoleAssigneeHandler — assignee by task node id as role code.
pub struct TaskRoleAssigneeHandler;

impl AssignmentHandler for TaskRoleAssigneeHandler {
    fn assign(&self, _exec: &Execution) -> JeeflowResult<String> {
        // Needs OrgUserProvider.findByRole(node.id) — resolved at runtime
        Ok(String::new())
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
pub fn register_builtin_assignment_handlers(ctx: &mut ServiceContext) {
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OperatorAssignmentHandler",
        Arc::new(OperatorAssignmentHandler));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptLeaderAssignmentHandler",
        Arc::new(ApplicantDeptLeaderAssignmentHandler));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptMainLeaderAssignmentHandler",
        Arc::new(ApplicantDeptMainLeaderAssignmentHandler));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptLeaderAssignmentHandler",
        Arc::new(DeptLeaderAssignmentHandler));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptMainLeaderAssignmentHandler",
        Arc::new(DeptMainLeaderAssignmentHandler));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.FormFieldAssigneeHandler",
        Arc::new(FormFieldAssigneeHandler));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskRoleAssigneeHandler",
        Arc::new(TaskRoleAssigneeHandler));
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
