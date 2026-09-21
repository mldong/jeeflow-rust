//! Metadata registries — EnumDictRegistry (7 dictionaries) + HandlerRegistry (spec/07, v1.4.0).

use std::collections::HashMap;

// ═══════════════════════════════════════════════════════
// EnumDictRegistry — 7 wf_* dictionaries
// ═══════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct DictItem {
    pub value: String,
    pub label: String,
}

/// Registry of all engine enum dictionaries.
pub struct EnumDictRegistry {
    dicts: HashMap<String, Vec<DictItem>>,
}

impl EnumDictRegistry {
    pub fn new() -> Self {
        let mut dicts = HashMap::new();

        // 1. wf_process_define_state
        dicts.insert("wf_process_define_state".to_string(), vec![
            DictItem { value: "0".into(), label: "禁用".into() },
            DictItem { value: "1".into(), label: "启用".into() },
        ]);

        // 2. wf_process_instance_state
        dicts.insert("wf_process_instance_state".to_string(), vec![
            DictItem { value: "10".into(), label: "进行中".into() },
            DictItem { value: "20".into(), label: "已完成".into() },
            DictItem { value: "30".into(), label: "已撤回".into() },
            DictItem { value: "40".into(), label: "强行终止".into() },
            DictItem { value: "45".into(), label: "已拒绝".into() },
            DictItem { value: "50".into(), label: "挂起".into() },
            DictItem { value: "99".into(), label: "已废弃".into() },
        ]);

        // 3. wf_process_submit_type
        dicts.insert("wf_process_submit_type".to_string(), vec![
            DictItem { value: "0".into(), label: "发起申请".into() },
            DictItem { value: "1".into(), label: "同意申请".into() },
            DictItem { value: "2".into(), label: "拒绝申请".into() },
            DictItem { value: "3".into(), label: "退回上一步".into() },
            DictItem { value: "4".into(), label: "跳转".into() },
            DictItem { value: "5".into(), label: "重新提交".into() },
            DictItem { value: "6".into(), label: "退回发起人".into() },
            DictItem { value: "7".into(), label: "转办".into() },
            DictItem { value: "20".into(), label: "会签拒绝".into() },
        ]);

        // 4. wf_process_task_state
        dicts.insert("wf_process_task_state".to_string(), vec![
            DictItem { value: "10".into(), label: "进行中".into() },
            DictItem { value: "20".into(), label: "已完成".into() },
            DictItem { value: "30".into(), label: "已撤回".into() },
            DictItem { value: "40".into(), label: "强行终止".into() },
            DictItem { value: "50".into(), label: "挂起".into() },
            DictItem { value: "99".into(), label: "已废弃".into() },
        ]);

        // 5. wf_process_task_type
        dicts.insert("wf_process_task_type".to_string(), vec![
            DictItem { value: "0".into(), label: "主办".into() },
            DictItem { value: "1".into(), label: "协办".into() },
            DictItem { value: "2".into(), label: "记录".into() },
        ]);

        // 6. wf_process_task_perform_type
        dicts.insert("wf_process_task_perform_type".to_string(), vec![
            DictItem { value: "0".into(), label: "普通参与".into() },
            DictItem { value: "1".into(), label: "会签参与".into() },
        ]);

        // 7. wf_countersign_type
        dicts.insert("wf_countersign_type".to_string(), vec![
            DictItem { value: "0".into(), label: "并行会签".into() },
            DictItem { value: "1".into(), label: "串行会签".into() },
        ]);

        EnumDictRegistry { dicts }
    }

    /// List all dictionary keys.
    pub fn list_dict_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.dicts.keys().cloned().collect();
        keys.sort();
        keys
    }

    /// Get dictionary items by key.
    pub fn get_dict(&self, key: &str) -> Option<&Vec<DictItem>> {
        self.dicts.get(key)
    }
}

impl Default for EnumDictRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ═══════════════════════════════════════════════════════
// HandlerRegistry — handler metadata (spec/07, v1.4.0)
// ═══════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct HandlerMeta {
    pub handler_type: String,    // e.g. "AssignmentHandler"
    pub class_name: String,      // Java FQCN or equivalent
    pub display_name: String,
    pub order: i32,
    pub group: String,
}

/// Registry of handler metadata.
pub struct HandlerRegistry {
    handlers: Vec<HandlerMeta>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        let mut reg = HandlerRegistry { handlers: Vec::new() };
        reg.register_built_in();
        reg
    }

    fn register_built_in(&mut self) {
        // 7 built-in AssignmentHandlers (registration name = Java FQCN)
        let ah = "AssignmentHandler";

        self.handlers.push(HandlerMeta {
            handler_type: ah.into(),
            class_name: "com.mldong.jeeflow.interceptor.impl.OperatorAssignmentHandler".into(),
            display_name: "流程发起人".into(),
            order: -9999,
            group: "built-in".into(),
        });

        self.handlers.push(HandlerMeta {
            handler_type: ah.into(),
            class_name: "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptLeaderAssignmentHandler".into(),
            display_name: "发起人所属部门经理".into(),
            order: 10,
            group: "built-in".into(),
        });

        self.handlers.push(HandlerMeta {
            handler_type: ah.into(),
            class_name: "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$ApplicantDeptMainLeaderAssignmentHandler".into(),
            display_name: "发起人所属部门分管领导".into(),
            order: 20,
            group: "built-in".into(),
        });

        self.handlers.push(HandlerMeta {
            handler_type: ah.into(),
            class_name: "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptLeaderAssignmentHandler".into(),
            display_name: "当前用户所属部门经理".into(),
            order: 30,
            group: "built-in".into(),
        });

        self.handlers.push(HandlerMeta {
            handler_type: ah.into(),
            class_name: "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$DeptMainLeaderAssignmentHandler".into(),
            display_name: "当前用户所属部门分管领导".into(),
            order: 40,
            group: "built-in".into(),
        });

        self.handlers.push(HandlerMeta {
            handler_type: ah.into(),
            class_name: "com.mldong.jeeflow.interceptor.impl.FormFieldAssigneeHandler".into(),
            display_name: "根据表单字段值分配参与者".into(),
            order: 50,
            group: "built-in".into(),
        });

        self.handlers.push(HandlerMeta {
            handler_type: ah.into(),
            class_name: "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskRoleAssigneeHandler".into(),
            display_name: "根据任务节点唯一编码关联角色分配参与者".into(),
            order: 60,
            group: "built-in".into(),
        });

        // Default ActionPermissionProvider
        self.handlers.push(HandlerMeta {
            handler_type: "ActionPermissionProvider".into(),
            class_name: "com.mldong.jeeflow.spi.impl.DefaultActionPermissionProvider".into(),
            display_name: "默认权限码提供者".into(),
            order: 0,
            group: "built-in".into(),
        });
    }

    pub fn register(&mut self, meta: HandlerMeta) {
        self.handlers.push(meta);
    }

    pub fn register_all(&mut self, metas: Vec<HandlerMeta>) {
        self.handlers.extend(metas);
    }

    /// List handlers by type, sorted by order.
    pub fn list_handlers(&self, handler_type: &str) -> Vec<&HandlerMeta> {
        let mut result: Vec<&HandlerMeta> = self.handlers.iter()
            .filter(|h| h.handler_type == handler_type)
            .collect();
        result.sort_by_key(|h| h.order);
        result
    }

    /// List handlers by type and group.
    pub fn list_handlers_group(&self, handler_type: &str, group: &str) -> Vec<&HandlerMeta> {
        let mut result: Vec<&HandlerMeta> = self.handlers.iter()
            .filter(|h| h.handler_type == handler_type && h.group == group)
            .collect();
        result.sort_by_key(|h| h.order);
        result
    }

    /// List all registered handler types.
    pub fn list_handler_types(&self) -> Vec<String> {
        let mut types: Vec<String> = self.handlers.iter()
            .map(|h| h.handler_type.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        types.sort();
        types
    }

    /// Get all handlers.
    pub fn all_handlers(&self) -> &[HandlerMeta] {
        &self.handlers
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enum_dict_registry() {
        let reg = EnumDictRegistry::new();
        let keys = reg.list_dict_keys();
        assert_eq!(keys.len(), 7);
        assert!(keys.contains(&"wf_process_define_state".to_string()));
        assert!(keys.contains(&"wf_process_instance_state".to_string()));
        assert!(keys.contains(&"wf_process_submit_type".to_string()));
        assert!(keys.contains(&"wf_process_task_state".to_string()));
        assert!(keys.contains(&"wf_process_task_type".to_string()));
        assert!(keys.contains(&"wf_process_task_perform_type".to_string()));
        assert!(keys.contains(&"wf_countersign_type".to_string()));
    }

    #[test]
    fn test_instance_state_dict() {
        let reg = EnumDictRegistry::new();
        let items = reg.get_dict("wf_process_instance_state").unwrap();
        assert_eq!(items.len(), 7);
        assert_eq!(items[0].value, "10");
        assert_eq!(items[0].label, "进行中");
    }

    #[test]
    fn test_submit_type_dict() {
        let reg = EnumDictRegistry::new();
        let items = reg.get_dict("wf_process_submit_type").unwrap();
        // issues/116：0/1/2/3/4/5/6/7/20 共 9 项（补 7 转办、20 会签拒绝）
        assert_eq!(items.len(), 9);
        let find = |v: &str| items.iter().find(|i| i.value == v).map(|i| i.label.clone());
        assert_eq!(find("7").as_deref(), Some("转办"));
        assert_eq!(find("20").as_deref(), Some("会签拒绝"));
        // 2 保持「拒绝申请」不动
        assert_eq!(find("2").as_deref(), Some("拒绝申请"));
    }

    #[test]
    fn test_unknown_dict() {
        let reg = EnumDictRegistry::new();
        assert!(reg.get_dict("unknown_key").is_none());
    }

    #[test]
    fn test_handler_registry() {
        let reg = HandlerRegistry::new();
        let handlers = reg.list_handlers("AssignmentHandler");
        assert_eq!(handlers.len(), 7);
        // First should be OperatorAssignmentHandler (order=-9999)
        assert_eq!(handlers[0].order, -9999);
        assert!(handlers[0].class_name.contains("OperatorAssignmentHandler"));
    }

    #[test]
    fn test_handler_types() {
        let reg = HandlerRegistry::new();
        let types = reg.list_handler_types();
        assert!(types.contains(&"AssignmentHandler".to_string()));
        assert!(types.contains(&"ActionPermissionProvider".to_string()));
    }

    #[test]
    fn test_handler_register_custom() {
        let mut reg = HandlerRegistry::new();
        reg.register(HandlerMeta {
            handler_type: "AssignmentHandler".into(),
            class_name: "com.custom.MyHandler".into(),
            display_name: "自定义处理器".into(),
            order: 100,
            group: "custom".into(),
        });
        let handlers = reg.list_handlers("AssignmentHandler");
        assert_eq!(handlers.len(), 8); // 7 built-in + 1 custom
    }
}
