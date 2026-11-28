//! Interceptors — 13 built-in AssignmentHandlers + field permission filtering.
//! Registration names = Java FQCN (cross-language flow JSON compatibility).

use crate::context::ServiceContext;
use crate::engine::Execution;
use crate::error::JeeflowResult;
use crate::json::{FlowData, JsonValue};
use crate::parser::{NodeType, ProcessModel};
use crate::spi::{AssignmentHandler, OrgUserProvider, PostUserProvider, UserProvider};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// ═══════════════════════════════════════════════════════
// 13 Built-in AssignmentHandlers
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

    /// 表单字段 → 用户 id → 该用户部门 → 领导（main=true 取分管领导）。
    /// 字段缺失／值为空／用户不存在／无部门／部门无领导／provider 未注册 ⇒ 空串，不报错。
    fn by_form_user(&self, exec: &Execution, main: bool) -> String {
        let Some(node) = &exec.current_node else {
            return String::new();
        };
        let user_id = first_form_user_id(find_form_user_field_value(&exec.args, &node.id));
        if user_id.is_empty() {
            return String::new();
        }
        let dept_id = self.dept_id_of(&user_id);
        self.by_dept(&dept_id, main)
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
///
/// 三档查找＝issues/48 的契约：`f_<id>` → 裸名 → `_NN` 去后缀后查**裸名 base**。
/// 第三档只查 base 裸名，`f_<base>` 是 [`find_form_user_field_value`]（issues/254 新类）的扩面。
pub struct FormFieldAssigneeHandler;

impl AssignmentHandler for FormFieldAssigneeHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        // 对齐 Go findFieldValue / Java：f_ 前缀优先、裸名兜底（issues/71 跨语言顺序），
        // 再去编号后缀（issues/255 补齐——此档此前只有 rust／moon 两语言缺）。
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
            if let Some(base) = number_suffix_base(&node.id) {
                if let Some(val) = exec.args.get_str(&base) {
                    return Ok(val.to_string());
                }
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

/// issues/259：任务节点唯一编码关联岗位（postCode = node id）。
///
/// 与 TaskRole 同形：保留 Provider 返回顺序；节点为空、SPI 未注册、岗位无成员或查询失败均返回空串。
pub struct TaskPostAssigneeHandler {
    post: Option<Arc<dyn PostUserProvider>>,
}

impl AssignmentHandler for TaskPostAssigneeHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        let Some(node) = &exec.current_node else {
            return Ok(String::new());
        };
        if node.id.is_empty() {
            return Ok(String::new());
        }
        let Some(post) = &self.post else {
            return Ok(String::new());
        };
        match post.find_by_post(&node.id) {
            Ok(v) if !v.is_empty() => Ok(v.join(",")),
            _ => Ok(String::new()),
        }
    }
}

/// 8. FormUserDeptLeaderAssignmentHandler — leader of the department of **the user named in the form**.
///
/// 场景（参照 mldong-fastapi-wf 提交 fe9233d）：HR 代员工提交、代理申请——表单里给出被代理人的
/// 用户 id，取该员工所属部门的经理，与当前操作人无关。
pub struct FormUserDeptLeaderAssignmentHandler {
    base: OrgAssignBase,
}

impl AssignmentHandler for FormUserDeptLeaderAssignmentHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        Ok(self.base.by_form_user(exec, false))
    }
}

/// 9. FormUserDeptMainLeaderAssignmentHandler — main leader of that same department.
pub struct FormUserDeptMainLeaderAssignmentHandler {
    base: OrgAssignBase,
}

impl AssignmentHandler for FormUserDeptMainLeaderAssignmentHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        Ok(self.base.by_form_user(exec, true))
    }
}

/// 10. FormDeptLeaderAssignmentHandler — leaders of 1–N departments named in the form.
pub struct FormDeptLeaderAssignmentHandler {
    base: FormDeptAssignBase,
}

impl AssignmentHandler for FormDeptLeaderAssignmentHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        Ok(self.base.by_form_dept(exec, false))
    }
}

/// 11. FormDeptMainLeaderAssignmentHandler — main leaders of those departments.
pub struct FormDeptMainLeaderAssignmentHandler {
    base: FormDeptAssignBase,
}

impl AssignmentHandler for FormDeptMainLeaderAssignmentHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        Ok(self.base.by_form_dept(exec, true))
    }
}

/// issues/259：表单指定 1–N 个岗位，逐岗位取成员。
///
/// 字段查找沿用 issues/254/258 的四档并集；字符串拆英文逗号，数组逐元素且不二次拆。
/// 岗位编码与最终 userId 均保序去重；单个岗位失败只跳过该岗位。
pub struct FormPostAssignmentHandler {
    post: Option<Arc<dyn PostUserProvider>>,
}

impl AssignmentHandler for FormPostAssignmentHandler {
    fn assign(&self, exec: &Execution) -> JeeflowResult<String> {
        let Some(node) = &exec.current_node else {
            return Ok(String::new());
        };
        if node.id.is_empty() {
            return Ok(String::new());
        }
        let post_codes = form_post_codes(find_form_user_field_value(&exec.args, &node.id));
        if post_codes.is_empty() {
            return Ok(String::new());
        }
        let Some(post) = &self.post else {
            return Ok(String::new());
        };

        let mut result: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for post_code in post_codes {
            let Ok(user_ids) = post.find_by_post(&post_code) else {
                continue;
            };
            for user_id in user_ids {
                let id = user_id.trim();
                if !id.is_empty() && seen.insert(id.to_string()) {
                    result.push(id.to_string());
                }
            }
        }
        Ok(result.join(","))
    }
}

/// issues/258：表单指定 1–N 个部门，逐部门取（分管）领导。
/// 与「表单指定用户」两枚 handler 完全分开：这里不查 UserProvider，也不做用户 → 部门转换。
#[derive(Clone)]
struct FormDeptAssignBase {
    org: Option<Arc<dyn OrgUserProvider>>,
}

impl FormDeptAssignBase {
    /// 字段缺失／空值／无节点／缺 SPI／部门无领导／SPI 出错 ⇒ 空串，不抛。
    /// 单个部门失败只跳过该部门；其余部门仍按输入顺序与 SPI 返回顺序合并。
    fn by_form_dept(&self, exec: &Execution, main: bool) -> String {
        let Some(node) = &exec.current_node else {
            return String::new();
        };
        // 四档查找与 issues/254 逐字同序；键存在即命中，null 不回退下一档。
        let dept_ids = form_dept_ids(find_form_user_field_value(&exec.args, &node.id));
        if dept_ids.is_empty() {
            return String::new();
        }
        let Some(org) = &self.org else {
            return String::new();
        };

        let mut result: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for dept_id in dept_ids {
            let leaders = if main {
                org.find_dept_main_leaders(&dept_id)
            } else {
                org.find_dept_leaders(&dept_id)
            };
            let Ok(leaders) = leaders else {
                continue;
            };
            for leader in leaders {
                let id = leader.trim();
                if !id.is_empty() && seen.insert(id.to_string()) {
                    result.push(id.to_string());
                }
            }
        }
        result.join(",")
    }
}

/// 「表单指定用户」两枚与「表单指定部门」两枚 handler 共用的四档并集查找（顺序即优先级）：
/// 1. `f_<nodeKey>` 2. `<nodeKey>` 3. 含 `#` 时取右半：先裸名、再 `f_` 4. 匹配 `_NN` 时取 base：先 `f_`、再裸名
///
/// 与 [`FormFieldAssigneeHandler`] 的差别只在 3、4 两档各补了 `f_` 变体（参照实现没有 f_ 分支，
/// jeeflow 家族变量存 `f_` 必须有）。**现存 handler 仍走它们自己的查找，本函数不改它们的行为。**
pub(crate) fn find_form_user_field_value<'a>(args: &'a FlowData, node_key: &str) -> Option<&'a JsonValue> {
    if node_key.is_empty() {
        return None;
    }
    if let Some(v) = args.get(&format!("f_{}", node_key)) {
        return Some(v);
    }
    if let Some(v) = args.get(node_key) {
        return Some(v);
    }
    if let Some(pos) = node_key.rfind('#') {
        let field = &node_key[pos + 1..];
        if !field.is_empty() {
            if let Some(v) = args.get(field) {
                return Some(v);
            }
            if let Some(v) = args.get(&format!("f_{}", field)) {
                return Some(v);
            }
        }
    }
    if let Some(base) = number_suffix_base(node_key) {
        if let Some(v) = args.get(&format!("f_{}", base)) {
            return Some(v);
        }
        if let Some(v) = args.get(&base) {
            return Some(v);
        }
    }
    None
}

/// `^(.+?)_(\d+)$` 的等价实现（不引 regex 依赖）：从左往右第一个「后面全是数字且非空」的下划线处切分。
pub(crate) fn number_suffix_base(node_key: &str) -> Option<String> {
    for (idx, _) in node_key.char_indices().filter(|(_, c)| *c == '_') {
        if idx == 0 {
            continue; // `^(.+?)_` 要求前缀至少一个字符，"_01" 不命中
        }
        let suffix = &node_key[idx + 1..];
        if !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_digit()) {
            return Some(node_key[..idx].to_string());
        }
    }
    None
}

/// 取**第一个**非空用户 id（逗号串／数组同样只取第一个，多值语义不在本轮承诺）。
/// 判空只比空串 ⇒ `"0"` 是合法 id（不被真值判据吃掉）。
pub(crate) fn first_form_user_id(value: Option<&JsonValue>) -> String {
    let Some(v) = value else {
        return String::new();
    };
    let tokens: Vec<String> = match v.as_array() {
        Some(arr) => arr.iter().map(json_scalar_to_string).collect(),
        None => json_scalar_to_string(v).split(',').map(|t| t.to_string()).collect(),
    };
    for token in tokens {
        let id = token.trim();
        if !id.is_empty() {
            return id.to_string();
        }
    }
    String::new()
}

/// 标量 → 字符串（对象/数组没有 id 形状，按空处理）。
fn json_scalar_to_string(v: &JsonValue) -> String {
    if let Some(s) = v.as_str() {
        return s.to_string();
    }
    if let Some(i) = v.as_i64() {
        return i.to_string();
    }
    if let Some(f) = v.as_f64() {
        return f.to_string();
    }
    if let Some(b) = v.as_bool() {
        return b.to_string();
    }
    String::new()
}

/// issues/258 部门 id 标量转换。数字直接按字面读（`0` 合法），对象／嵌套集合没有 id 形状。
/// 不复用 `json_scalar_to_string`：它会先把任意 f64 截成 i64，会把 `1.5` 错读成部门 `1`。
fn json_dept_scalar_to_string(v: &JsonValue) -> String {
    match v {
        JsonValue::Str(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => String::new(),
    }
}

/// 字符串按英文逗号拆；数组逐元素读取且不二次拆逗号。trim、丢空、部门去重均保序。
fn form_dept_ids(value: Option<&JsonValue>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };
    let tokens: Vec<String> = match value.as_array() {
        Some(items) => items.iter().map(json_dept_scalar_to_string).collect(),
        None => json_dept_scalar_to_string(value)
            .split(',')
            .map(str::to_string)
            .collect(),
    };

    let mut result: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    for token in tokens {
        let id = token.trim();
        if !id.is_empty() && seen.insert(id.to_string()) {
            result.push(id.to_string());
        }
    }
    result
}

/// issues/259 岗位编码标量转换。数字按字面读（`0` 合法），对象／嵌套集合没有编码形状。
fn json_post_scalar_to_string(v: &JsonValue) -> String {
    match v {
        JsonValue::Str(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => String::new(),
    }
}

/// 字符串按英文逗号拆；数组逐元素读取且不二次拆逗号。trim、丢空、岗位编码去重均保序。
fn form_post_codes(value: Option<&JsonValue>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };
    let tokens: Vec<String> = match value.as_array() {
        Some(items) => items.iter().map(json_post_scalar_to_string).collect(),
        None => json_post_scalar_to_string(value)
            .split(',')
            .map(str::to_string)
            .collect(),
    };

    let mut result: Vec<String> = Vec::new();
    let mut seen = HashSet::new();
    for token in tokens {
        let code = token.trim();
        if !code.is_empty() && seen.insert(code.to_string()) {
            result.push(code.to_string());
        }
    }
    result
}

// ═══════════════════════════════════════════════════════
// Field permission filtering
// ═══════════════════════════════════════════════════════/// Filter fields by permission settings (spec/09 §4.2).
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

/// 办理**入口**的字段权限过滤（spec/09 §4.2 1.8.2；issues/183／audit8 R17）。
///
/// java 同位＝`JeeflowEngineImpl.java:264`（`prepareExecution` 内、`completeTask` 之前）
/// `args = FlowUtil.filterFieldByPerm(args, model, task.getTaskName())`；
/// moon 判例＝issues/150 病灶一修复（`filter_args_by_node_permission` 同形）。
/// 三处放行与 java 同形：节点缺失 / 非任务节点（java `instanceof TaskModel`；会签也是
/// `snaker:task` ⇒ `NodeType::Task` 覆盖）/ 无权限声明 ⇒ 原样返回。少了这一层，
/// 只读/隐藏字段的提交值会经 `complete_task` 留在实例变量里、被下一个没声明该字段权限的
/// 节点写进业务表（跨节点绕过；写侧 `is_editable` 只挡当次提交）。
pub fn filter_args_by_node_permission(
    args: &FlowData,
    model: &ProcessModel,
    task_name: &str,
) -> FlowData {
    let Some(node) = model.get_node(task_name) else {
        return args.clone();
    };
    if node.node_type != NodeType::Task {
        return args.clone();
    }
    let perms = node.field_permissions();
    if perms.is_empty() {
        return args.clone();
    }
    filter_fields_by_permission(args, &perms)
}

/// Register all built-in assignment handlers into a ServiceContext.
/// 组织维度 handler 在注册时捕获 user/org provider（对齐 Go RegisterBuiltinAssignments：
/// provider 必须先注册——wf_factory 的 with_user_provider/with_org_user_provider 在本函数之前）。
pub fn register_builtin_assignment_handlers(ctx: &mut ServiceContext) {
    let base = OrgAssignBase {
        user: ctx.user_provider.clone(),
        org: ctx.org_user_provider.clone(),
    };
    let dept_base = FormDeptAssignBase {
        org: ctx.org_user_provider.clone(),
    };
    let post_provider = ctx.post_user_provider.clone();
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
        Arc::new(DeptMainLeaderAssignmentHandler { base: base.clone() }));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.FormFieldAssigneeHandler",
        Arc::new(FormFieldAssigneeHandler));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskRoleAssigneeHandler",
        Arc::new(TaskRoleAssigneeHandler { org: ctx.org_user_provider.clone() }));
    // issues/259：节点编码即岗位编码；注册名保留 Java 嵌套类形状。
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.OrgUserAssignmentHandlers$TaskPostAssigneeHandler",
        Arc::new(TaskPostAssigneeHandler { post: post_provider.clone() }));
    // 两个「表单指定用户」类：注册名用顶级类 FQN，与其余七语言逐字一致
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.FormUserDeptLeaderAssignmentHandler",
        Arc::new(FormUserDeptLeaderAssignmentHandler { base: base.clone() }));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.FormUserDeptMainLeaderAssignmentHandler",
        Arc::new(FormUserDeptMainLeaderAssignmentHandler { base }));
    // issues/258：表单指定部门的（分管）领导，注册名同为 Java 顶级类 FQCN。
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.FormDeptLeaderAssignmentHandler",
        Arc::new(FormDeptLeaderAssignmentHandler { base: dept_base.clone() }));
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.FormDeptMainLeaderAssignmentHandler",
        Arc::new(FormDeptMainLeaderAssignmentHandler { base: dept_base }));
    // issues/259：表单指定岗位；注册名为 Java 顶级类 FQCN。
    ctx.register_assignment_handler(
        "com.mldong.jeeflow.interceptor.impl.FormPostAssignmentHandler",
        Arc::new(FormPostAssignmentHandler { post: post_provider }));
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
        ..Default::default()
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
            reapply_mode: crate::reapply::ReapplyMode::Normal,
            allow_add_sign: false,
            allow_recall: false,
            dynamic_countersign: false,
            add_sign_max_rounds: 0,
        };
        let mut args = FlowData::new();
        args.insert_str("node1", "user2,user3");
        let model = ProcessModel {
            name: "test".into(), display_name: "Test".into(),
            model_type: "approval".into(), expire_time: None,
            persist_mode: None, rel_table_name: None,
            nodes: vec![], edges: vec![],
        ..Default::default()
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
        assert_eq!(ctx.assignment_handlers.len(), 13);
        assert!(ctx.find_assignment_handler(
            "com.mldong.jeeflow.interceptor.impl.OperatorAssignmentHandler"
        ).is_some());
    }

    /// issues/183（audit8 R17）：入口过滤包装函数——三处放行（与 java `FlowUtil.filterFieldByPerm`
    /// 的"节点缺失 / 非 TaskModel / 无声明"同形）＋带声明档过滤生效＋非 f_ 键透传。
    #[test]
    fn test_i183_filter_args_by_node_permission() {
        use crate::json::JsonValue;
        let mut model = ProcessModel {
            name: "i183".into(), display_name: "I183".into(), model_type: "approval".into(),
            expire_time: None, persist_mode: None, rel_table_name: None,
            nodes: vec![], edges: vec![],
        ..Default::default()
        };
        let mut args = FlowData::new();
        args.insert_str("f_amount", "999");
        args.insert_str("f_note", "改注释");
        args.insert_str("submitType", "1");

        // ① 节点缺失 ⇒ 原样放行
        let r = filter_args_by_node_permission(&args, &model, "no_such_node");
        assert_eq!(r.get_str("f_amount"), Some("999"), "节点缺失必须原样放行");

        // ② 非任务节点（decision）⇒ 原样放行（java instanceof TaskModel）
        model.nodes.push(NodeModel {
            id: "dec1".into(), node_type: NodeType::Decision,
            display_name: "D1".into(), properties: HashMap::new(),
            reapply_mode: crate::reapply::ReapplyMode::Normal,
            allow_add_sign: false,
            allow_recall: false,
            dynamic_countersign: false,
            add_sign_max_rounds: 0,
        });
        let r = filter_args_by_node_permission(&args, &model, "dec1");
        assert_eq!(r.get_str("f_amount"), Some("999"), "非任务节点必须原样放行");

        // ③ 任务节点但无权限声明 ⇒ 原样放行
        model.nodes.push(NodeModel {
            id: "t_free".into(), node_type: NodeType::Task,
            display_name: "T-free".into(), properties: HashMap::new(),
            reapply_mode: crate::reapply::ReapplyMode::Normal,
            allow_add_sign: false,
            allow_recall: false,
            dynamic_countersign: false,
            add_sign_max_rounds: 0,
        });
        let r = filter_args_by_node_permission(&args, &model, "t_free");
        assert_eq!(r.get_str("f_amount"), Some("999"), "无声明节点必须原样放行（缺省可编辑）");

        // ④ 任务节点带声明（properties.field 子对象）⇒ 过滤生效
        let mut props = HashMap::new();
        props.insert("field".to_string(), JsonValue::Object(vec![
            ("PERMISSION_f_amount".to_string(), JsonValue::Number(1.0)),
        ]));
        model.nodes.push(NodeModel {
            id: "t_ro".into(), node_type: NodeType::Task,
            display_name: "T-ro".into(), properties: props,
            reapply_mode: crate::reapply::ReapplyMode::Normal,
            allow_add_sign: false,
            allow_recall: false,
            dynamic_countersign: false,
            add_sign_max_rounds: 0,
        });
        let r = filter_args_by_node_permission(&args, &model, "t_ro");
        assert_eq!(r.get_str("f_amount"), None, "只读字段必须被剔除");
        assert_eq!(r.get_str("f_note"), Some("改注释"), "无声明字段缺省可编辑（放行）");
        assert_eq!(r.get_str("submitType"), Some("1"), "非 f_ 键透传（不会被过滤误伤）");
    }
}

// ═══════════════════════════════════════════════════════
// 「表单指定用户」两个 handler 的契约测试（执行卡 §3，基准＝jeeflow-go v1.8.67）
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod form_user_tests {
    use super::*;
    use crate::model::{ProcessDefine, ProcessInstance, UserInfo};
    use crate::parser::NodeModel;

    const FQ_LEADER: &str = "com.mldong.jeeflow.interceptor.impl.FormUserDeptLeaderAssignmentHandler";
    const FQ_MAIN: &str = "com.mldong.jeeflow.interceptor.impl.FormUserDeptMainLeaderAssignmentHandler";

    /// emp/0→D01（两位经理＋一位分管领导）、other→D02（无领导）、nodept→无部门、其余查无此人
    struct FuUser;
    impl UserProvider for FuUser {
        fn get_user(&self, user_id: &str) -> JeeflowResult<Option<UserInfo>> {
            let dept = match user_id {
                "emp" | "0" => "D01",
                "other" => "D02",
                "nodept" => "",
                _ => return Ok(None),
            };
            Ok(Some(UserInfo {
                user_id: user_id.into(), real_name: user_id.into(), dept_id: dept.into(),
                dept_name: String::new(), post_id: String::new(), post_name: String::new(),
            }))
        }
    }

    struct FuOrg;
    impl OrgUserProvider for FuOrg {
        fn find_dept_leaders(&self, d: &str) -> JeeflowResult<Vec<String>> {
            Ok(if d == "D01" { vec!["leader1".into(), "leader2".into()] } else { vec![] })
        }
        fn find_dept_main_leaders(&self, d: &str) -> JeeflowResult<Vec<String>> {
            Ok(if d == "D01" { vec!["boss1".into()] } else { vec![] })
        }
        fn find_by_role(&self, _: &str) -> JeeflowResult<Vec<String>> { Ok(vec![]) }
    }

    fn base() -> OrgAssignBase {
        OrgAssignBase { user: Some(Arc::new(FuUser)), org: Some(Arc::new(FuOrg)) }
    }

    fn no_spi() -> OrgAssignBase {
        OrgAssignBase { user: None, org: None }
    }

    fn exec(node_id: &str, args: FlowData) -> Execution {
        let define = ProcessDefine {
            id: 1, name: "fu".into(), display_name: "fu".into(), define_type: "approval".into(),
            state: 1, content: b"{}".to_vec(), version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        let instance = ProcessInstance::create(&define, "hr", &FlowData::new());
        let model = ProcessModel {
            name: "fu".into(), display_name: "fu".into(), model_type: "approval".into(),
            expire_time: None, persist_mode: None, rel_table_name: None,
            nodes: vec![], edges: vec![], ..Default::default()
        };
        let mut e = Execution::new(instance, model, define, "hr", args);
        e.current_node = Some(NodeModel {
            id: node_id.into(), node_type: NodeType::Task, display_name: node_id.into(),
            properties: HashMap::new(),
            reapply_mode: crate::reapply::ReapplyMode::Normal,
            allow_add_sign: false, allow_recall: false, dynamic_countersign: false,
            add_sign_max_rounds: 0,
        });
        e
    }

    fn args(pairs: &[(&str, &str)]) -> FlowData {
        let mut d = FlowData::new();
        for (k, v) in pairs {
            d.insert_str(*k, *v);
        }
        d
    }

    #[test]
    fn positive_four_tiers() {
        let cases = [
            ("f_ 主通道", "manager", args(&[("f_manager", "emp")])),
            ("裸名回落", "manager", args(&[("manager", "emp")])),
            ("# 右半（字段名本身带 f_）", "manager#f_userId", args(&[("f_userId", "emp")])),
            ("# 右半的 f_ 变体", "manager#userId", args(&[("f_userId", "emp")])),
            ("_NN 去后缀的 f_ 形", "manager_01", args(&[("f_manager", "emp")])),
            ("_NN 去后缀的裸名形", "manager_01", args(&[("manager", "emp")])),
        ];
        for (label, node_id, a) in cases {
            let got = FormUserDeptLeaderAssignmentHandler { base: base() }
                .assign(&exec(node_id, a)).unwrap();
            assert_eq!(got, "leader1,leader2", "{}", label);
        }
    }

    #[test]
    fn positive_main_leader_branch() {
        let got = FormUserDeptMainLeaderAssignmentHandler { base: base() }
            .assign(&exec("manager#f_userId", args(&[("f_userId", "emp")]))).unwrap();
        assert_eq!(got, "boss1");
    }

    #[test]
    fn precedence_rules() {
        // 第 1 档 f_ 压裸名
        let a = args(&[("f_x", "emp"), ("x", "other")]);
        assert_eq!(find_form_user_field_value(&a, "x").and_then(|v| v.as_str()), Some("emp"));
        // # 档先裸名再 f_
        let b = args(&[("userId", "emp"), ("f_userId", "other")]);
        assert_eq!(find_form_user_field_value(&b, "manager#userId").and_then(|v| v.as_str()), Some("emp"));
        // _NN 档先 f_ 再裸名（jeeflow 变量存 f_，参照实现没有这一支）
        let c = args(&[("f_manager", "emp"), ("manager", "other")]);
        assert_eq!(find_form_user_field_value(&c, "manager_01").and_then(|v| v.as_str()), Some("emp"));
    }

    #[test]
    fn number_suffix_base_matches_lazy_regex() {
        // ^(.+?)_(\d+)$：从左往右第一个「后面全是数字」的下划线处切
        assert_eq!(number_suffix_base("task_01").as_deref(), Some("task"));
        assert_eq!(number_suffix_base("a_b_01").as_deref(), Some("a_b"));
        assert_eq!(number_suffix_base("manager"), None);
        assert_eq!(number_suffix_base("manager_01x"), None);
        assert_eq!(number_suffix_base("_01"), None, "前缀为空不算命中");
    }

    #[test]
    fn takes_first_non_empty_id_only() {
        let a = args(&[("k", "a,b")]);
        assert_eq!(first_form_user_id(find_form_user_field_value(&a, "k")), "a");
        let b = args(&[("k", "  ,  ,d")]);
        assert_eq!(first_form_user_id(find_form_user_field_value(&b, "k")), "d");
        // 数组形只取第一个非空成员
        let mut c = FlowData::new();
        c.insert("k".into(), JsonValue::Array(vec![
            JsonValue::Str("".into()), JsonValue::Str("  ".into()), JsonValue::Str("b".into())]));
        assert_eq!(first_form_user_id(find_form_user_field_value(&c, "k")), "b");
        // 数字 0 是合法 id，不能被真值判据吃掉
        let mut d = FlowData::new();
        d.insert("k".into(), JsonValue::Number(0.0));
        assert_eq!(first_form_user_id(find_form_user_field_value(&d, "k")), "0");
        assert_eq!(first_form_user_id(None), "");
    }

    #[test]
    fn id_zero_still_resolves_leaders() {
        let got = FormUserDeptLeaderAssignmentHandler { base: base() }
            .assign(&exec("manager", args(&[("f_manager", "0")]))).unwrap();
        assert_eq!(got, "leader1,leader2", "\"0\" 这枚 id 必须照常取人");
    }

    #[test]
    fn negative_shapes_return_empty_without_error() {
        let cases = [
            ("表单没给字段", args(&[])),
            ("值为空串", args(&[("f_userId", "")])),
            ("值为纯空白", args(&[("f_userId", "   ")])),
            ("指向查无此人", args(&[("f_userId", "ghost")])),
            ("指向无部门用户", args(&[("f_userId", "nodept")])),
            ("指向别的部门（无领导）", args(&[("f_userId", "other")])),
        ];
        for (label, a) in cases {
            assert_eq!(FormUserDeptLeaderAssignmentHandler { base: base() }
                .assign(&exec("manager#f_userId", a.clone())).unwrap(), "", "{}", label);
            assert_eq!(FormUserDeptMainLeaderAssignmentHandler { base: base() }
                .assign(&exec("manager#f_userId", a)).unwrap(), "", "{}", label);
        }
    }

    #[test]
    fn missing_spi_returns_empty() {
        let got = FormUserDeptLeaderAssignmentHandler { base: no_spi() }
            .assign(&exec("manager", args(&[("f_manager", "emp")]))).unwrap();
        assert_eq!(got, "");
    }

    /// 回归：现存 handler 仍走自己的查找，本轮并集没有渗进去
    #[test]
    fn existing_handlers_keep_their_own_lookup() {
        // DeptLeader 读操作人（hr 查无此人 ⇒ 空），表单里的 emp 不该被它看见
        let got = DeptLeaderAssignmentHandler { base: base() }
            .assign(&exec("manager", args(&[("manager", "emp")]))).unwrap();
        assert_eq!(got, "");
        // FormFieldAssignee 只认 f_<nodeId> 与裸名，不认 # 右半
        let legacy = FormFieldAssigneeHandler
            .assign(&exec("manager#userId", args(&[("f_userId", "emp")]))).unwrap();
        assert_eq!(legacy, "", "老类没有 # 档 ⇒ 同一入参应读不到");
    }

    /// issues/255：老类的第三档＝`_NN` 去后缀后查**裸名 base**（补齐前 rust 只有两档）。
    #[test]
    fn legacy_handler_number_suffix_tier() {
        let legacy = |node: &str, a: FlowData| {
            FormFieldAssigneeHandler.assign(&exec(node, a)).unwrap()
        };
        // 正向：task_01 ＋ 变量 task ⇒ 命中 base
        assert_eq!(legacy("task_01", args(&[("task", "userA,userB")])), "userA,userB");
        // 惰性量词的最早可切点：a_b_01 的 base 是 a_b，不是 a
        assert_eq!(legacy("a_b_01", args(&[("a", "userX"), ("a_b", "userC")])), "userC");
        // 优先级：f_<nodeId> → 裸名 → base 裸名
        assert_eq!(legacy("task_01", args(&[("f_task_01", "u1"), ("task_01", "u2"), ("task", "u3")])), "u1");
        assert_eq!(legacy("task_01", args(&[("task_01", "u2"), ("task", "u3")])), "u2");
        // 反向：尾锚非数字／base 为空都不命中
        assert_eq!(legacy("task_01x", args(&[("task", "u3")])), "");
        assert_eq!(legacy("_01", args(&[("", "u3")])), "");
        // 第三档只查裸名：f_<base> 是 issues/254 新类的扩面，老类不许跟着扩
        assert_eq!(legacy("task_02", args(&[("f_task", "u3")])), "");
    }

    #[test]
    fn registration_names_are_top_level_fqns() {
        let mut ctx = ServiceContext::new();
        ctx.user_provider = Some(Arc::new(FuUser));
        ctx.org_user_provider = Some(Arc::new(FuOrg));
        register_builtin_assignment_handlers(&mut ctx);
        for name in [FQ_LEADER, FQ_MAIN] {
            assert!(ctx.find_assignment_handler(name).is_some(), "注册名必须逐字可解析：{}", name);
            assert!(!name.contains('$'), "两个新类是顶级类，不带 OrgUserAssignmentHandlers$ 前缀");
        }
    }

    #[test]
    fn metadata_exposes_two_new_display_names() {
        let reg = crate::metadata::HandlerRegistry::new();
        let handlers = reg.list_handlers("AssignmentHandler");
        assert_eq!(handlers.len(), 13);
        // issues/259 TaskPost 插在 order 65；旧两枚仍固定占据 70/80。
        assert_eq!(handlers[8].class_name, FQ_LEADER);
        assert_eq!(handlers[8].display_name, "表单指定用户所属部门经理");
        assert_eq!(handlers[8].order, 70);
        assert_eq!(handlers[9].class_name, FQ_MAIN);
        assert_eq!(handlers[9].display_name, "表单指定用户所属部门分管领导");
        assert_eq!(handlers[9].order, 80);
    }
}

// ═══════════════════════════════════════════════════════
// issues/258「表单指定部门（分管）领导」两个 handler 的专属契约测试
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod form_dept_tests {
    use super::*;
    use crate::error::JeeflowError;
    use crate::model::{ProcessDefine, ProcessInstance};
    use crate::parser::NodeModel;
    use std::sync::Mutex;

    const FQ_LEADER: &str = "com.mldong.jeeflow.interceptor.impl.FormDeptLeaderAssignmentHandler";
    const FQ_MAIN: &str = "com.mldong.jeeflow.interceptor.impl.FormDeptMainLeaderAssignmentHandler";

    /// deptA/deptB 共享 leader；deptC 验 SPI 顺序；deptD 验领导 trim/空值/重复；err 验失败隔离。
    #[derive(Default)]
    struct FdOrg {
        leader_calls: Mutex<Vec<String>>,
        main_calls: Mutex<Vec<String>>,
    }

    impl OrgUserProvider for FdOrg {
        fn find_dept_leaders(&self, d: &str) -> JeeflowResult<Vec<String>> {
            self.leader_calls.lock().unwrap().push(d.to_string());
            if d == "err" {
                return Err(JeeflowError::Internal("dept leader lookup failed".into()));
            }
            let ids = match d {
                "deptA" => vec!["leaderA", "shared"],
                "deptB" => vec!["leaderB", "shared"],
                "deptC" => vec!["leaderC1", "leaderC2"],
                "deptD" => vec!["  leaderD  ", "", "leaderD", "  "],
                "0" => vec!["zeroLeader"],
                _ => vec![],
            };
            Ok(ids.into_iter().map(str::to_string).collect())
        }

        fn find_dept_main_leaders(&self, d: &str) -> JeeflowResult<Vec<String>> {
            self.main_calls.lock().unwrap().push(d.to_string());
            if d == "err" {
                return Err(JeeflowError::Internal("dept main leader lookup failed".into()));
            }
            let ids = match d {
                "deptA" => vec!["mainA"],
                "deptB" => vec!["mainB"],
                "deptC" => vec!["mainC"],
                "0" => vec!["zeroMain"],
                _ => vec![],
            };
            Ok(ids.into_iter().map(str::to_string).collect())
        }

        fn find_by_role(&self, _: &str) -> JeeflowResult<Vec<String>> {
            Ok(vec![])
        }
    }

    fn base() -> FormDeptAssignBase {
        FormDeptAssignBase { org: Some(Arc::new(FdOrg::default())) }
    }

    fn no_spi() -> FormDeptAssignBase {
        FormDeptAssignBase { org: None }
    }

    fn exec(node_id: Option<&str>, args: FlowData) -> Execution {
        let define = ProcessDefine {
            id: 1, name: "fd".into(), display_name: "fd".into(), define_type: "approval".into(),
            state: 1, content: b"{}".to_vec(), version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        let instance = ProcessInstance::create(&define, "clerk", &FlowData::new());
        let model = ProcessModel {
            name: "fd".into(), display_name: "fd".into(), model_type: "approval".into(),
            expire_time: None, persist_mode: None, rel_table_name: None,
            nodes: vec![], edges: vec![], ..Default::default()
        };
        let mut execution = Execution::new(instance, model, define, "clerk", args);
        if let Some(node_id) = node_id {
            execution.current_node = Some(NodeModel {
                id: node_id.into(), node_type: NodeType::Task, display_name: node_id.into(),
                properties: HashMap::new(),
                reapply_mode: crate::reapply::ReapplyMode::Normal,
                allow_add_sign: false, allow_recall: false, dynamic_countersign: false,
                add_sign_max_rounds: 0,
            });
        }
        execution
    }

    fn args(pairs: &[(&str, &str)]) -> FlowData {
        let mut data = FlowData::new();
        for (key, value) in pairs {
            data.insert_str(*key, *value);
        }
        data
    }

    fn arg_value(key: &str, value: JsonValue) -> FlowData {
        let mut data = FlowData::new();
        data.insert(key.to_string(), value);
        data
    }

    fn assign(base: &FormDeptAssignBase, node_id: Option<&str>, args: FlowData, main: bool) -> String {
        if main {
            FormDeptMainLeaderAssignmentHandler { base: base.clone() }
                .assign(&exec(node_id, args)).unwrap()
        } else {
            FormDeptLeaderAssignmentHandler { base: base.clone() }
                .assign(&exec(node_id, args)).unwrap()
        }
    }

    #[test]
    fn t01_prefixed_wins_over_bare() {
        let data = args(&[("f_depts", "deptA"), ("depts", "deptB")]);
        assert_eq!(assign(&base(), Some("depts"), data, false), "leaderA,shared");
    }

    #[test]
    fn t02_bare_fallback_and_miss() {
        assert_eq!(assign(&base(), Some("depts"), args(&[("depts", "deptB")]), false),
            "leaderB,shared");
        assert_eq!(assign(&base(), Some("depts"), args(&[("other", "deptB")]), false), "");
    }

    #[test]
    fn t03_hash_right_half_bare_then_prefixed() {
        let bare_first = args(&[("deptIds", "deptA"), ("f_deptIds", "deptB")]);
        assert_eq!(assign(&base(), Some("manager#deptIds"), bare_first, false), "leaderA,shared");
        assert_eq!(assign(&base(), Some("manager#deptIds"),
            args(&[("f_deptIds", "deptB")]), false), "leaderB,shared");
        // 含多个 # 时只取最后一个 # 的右半。
        assert_eq!(assign(&base(), Some("a#b#deptIds"),
            args(&[("deptIds", "deptA")]), false), "leaderA,shared");
    }

    #[test]
    fn t04_number_suffix_prefixed_then_bare_and_lazy_base() {
        let prefixed = args(&[("f_depts", "deptA"), ("depts", "deptB")]);
        assert_eq!(assign(&base(), Some("depts_01"), prefixed, false), "leaderA,shared");
        assert_eq!(assign(&base(), Some("depts_01"), args(&[("depts", "deptA")]), false),
            "leaderA,shared");
        // ^(.+?)_(\d+)$ 取最早可切点：a_b_01 → a_b，不是 a。
        let lazy = args(&[("a_b", "deptA"), ("a", "deptB")]);
        assert_eq!(assign(&base(), Some("a_b_01"), lazy, false), "leaderA,shared");
        assert_eq!(assign(&base(), Some("_01"), args(&[("f_", "deptA")]), false), "");
        assert_eq!(assign(&base(), Some("depts_01x"), args(&[("depts", "deptA")]), false), "");
    }

    #[test]
    fn t05_key_existence_not_truthiness() {
        // 高档位存在但为 null／空串时不回退裸名。
        for value in [JsonValue::Null, JsonValue::Str(String::new())] {
            let mut data = arg_value("f_depts", value);
            data.insert_str("depts", "deptB");
            assert_eq!(assign(&base(), Some("depts"), data, false), "");
        }
    }

    #[test]
    fn t06_comma_string_resolves_all_departments_in_order() {
        let data = args(&[("depts", "  deptA, ,deptB,, ")]);
        assert_eq!(assign(&base(), Some("depts"), data, false), "leaderA,shared,leaderB");
    }

    #[test]
    fn t07_array_resolves_all_departments_in_order() {
        let data = arg_value("depts", JsonValue::Array(vec![
            JsonValue::Str(" deptA ".into()),
            JsonValue::Number(0.0),
            JsonValue::Str("deptB".into()),
        ]));
        assert_eq!(assign(&base(), Some("depts"), data, false),
            "leaderA,shared,zeroLeader,leaderB");
    }

    #[test]
    fn t08_array_element_is_not_resplit() {
        let data = arg_value("depts", JsonValue::Array(vec![
            JsonValue::Str("deptA,deptB".into()),
        ]));
        assert_eq!(assign(&base(), Some("depts"), data, false), "");
    }

    #[test]
    fn t09_duplicate_departments_and_leaders_are_deduped() {
        let org = Arc::new(FdOrg::default());
        let base = FormDeptAssignBase { org: Some(org.clone()) };
        let data = args(&[("depts", "deptA,deptB,deptA")]);
        assert_eq!(assign(&base, Some("depts"), data, false), "leaderA,shared,leaderB");
        // 部门去重发生在 SPI 调用前，重复部门不能重复查。
        assert_eq!(*org.leader_calls.lock().unwrap(), vec!["deptA", "deptB"]);
    }

    #[test]
    fn t10_zero_dept_id_is_legal() {
        for value in [JsonValue::Str("0".into()), JsonValue::Number(0.0)] {
            let data = arg_value("depts", value);
            assert_eq!(assign(&base(), Some("depts"), data.clone(), false), "zeroLeader");
            assert_eq!(assign(&base(), Some("depts"), data, true), "zeroMain");
        }
    }

    #[test]
    fn t11_main_leader_variant_uses_its_own_spi() {
        let org = Arc::new(FdOrg::default());
        let base = FormDeptAssignBase { org: Some(org.clone()) };
        let data = args(&[("depts", "deptA,deptB")]);
        assert_eq!(assign(&base, Some("depts"), data, true), "mainA,mainB");
        assert_eq!(*org.main_calls.lock().unwrap(), vec!["deptA", "deptB"]);
        assert!(org.leader_calls.lock().unwrap().is_empty());
    }

    #[test]
    fn t12_invalid_department_is_skipped() {
        let data = args(&[("depts", "ghost,deptB")]);
        assert_eq!(assign(&base(), Some("depts"), data.clone(), false), "leaderB,shared");
        assert_eq!(assign(&base(), Some("depts"), data, true), "mainB");
    }

    #[test]
    fn t13_empty_null_and_missing_fields_are_empty() {
        let cases = vec![
            args(&[]),
            args(&[("f_depts", "")]),
            args(&[("f_depts", "  , , ")]),
            arg_value("f_depts", JsonValue::Null),
            arg_value("f_depts", JsonValue::Array(vec![])),
            arg_value("f_depts", JsonValue::Array(vec![
                JsonValue::Null, JsonValue::Str("   ".into()),
            ])),
        ];
        for data in cases {
            assert_eq!(assign(&base(), Some("depts"), data.clone(), false), "");
            assert_eq!(assign(&base(), Some("depts"), data, true), "");
        }
    }

    #[test]
    fn t14_missing_org_spi_is_empty() {
        let data = args(&[("depts", "deptA")]);
        assert_eq!(assign(&no_spi(), Some("depts"), data.clone(), false), "");
        assert_eq!(assign(&no_spi(), Some("depts"), data, true), "");
    }

    #[test]
    fn t15_no_node_or_empty_node_key_is_empty() {
        let data = args(&[("f_", "deptA"), ("f_depts", "deptB")]);
        for node_id in [None, Some("")] {
            assert_eq!(assign(&base(), node_id, data.clone(), false), "");
            assert_eq!(assign(&base(), node_id, data.clone(), true), "");
        }
    }

    #[test]
    fn t16_non_scalar_values_are_empty() {
        let cases = vec![
            JsonValue::Object(vec![]),
            JsonValue::Array(vec![JsonValue::Array(vec![JsonValue::Str("deptA".into())])]),
            // 不能把 1.5 截成部门 1。
            JsonValue::Number(1.5),
        ];
        for value in cases {
            let data = arg_value("depts", value);
            assert_eq!(assign(&base(), Some("depts"), data.clone(), false), "");
            assert_eq!(assign(&base(), Some("depts"), data, true), "");
        }
    }

    #[test]
    fn t17_multiple_leaders_keep_spi_order_and_trim() {
        let data = args(&[("depts", "deptC,deptD")]);
        assert_eq!(assign(&base(), Some("depts"), data, false),
            "leaderC1,leaderC2,leaderD");
    }

    #[test]
    fn t18_spi_errors_are_isolated_without_throwing() {
        let org = Arc::new(FdOrg::default());
        let base = FormDeptAssignBase { org: Some(org.clone()) };
        // 一个部门失败不影响后续部门。
        assert_eq!(assign(&base, Some("depts"), args(&[("depts", "err,deptB")]), false),
            "leaderB,shared");
        assert_eq!(assign(&base, Some("depts"), args(&[("depts", "err,deptB")]), true), "mainB");
        // 全部失败仍为空，不把 Err 传给引擎。
        assert_eq!(assign(&base, Some("depts"), args(&[("depts", "err")]), false), "");
        assert_eq!(*org.leader_calls.lock().unwrap(), vec!["err", "deptB", "err"]);
    }

    #[test]
    fn t19_registration_names_are_top_level_fqns_and_resolve() {
        let mut ctx = ServiceContext::new();
        // 故意不注册 UserProvider：两枚新 handler 只依赖 OrgUserProvider。
        ctx.org_user_provider = Some(Arc::new(FdOrg::default()));
        register_builtin_assignment_handlers(&mut ctx);
        assert_eq!(ctx.assignment_handlers.len(), 13);

        for (name, expected) in [
            (FQ_LEADER, "leaderA,shared"),
            (FQ_MAIN, "mainA"),
        ] {
            assert!(!name.contains('$'), "注册名必须是顶级类 FQCN：{}", name);
            let handler = ctx.find_assignment_handler(name).unwrap();
            let data = args(&[("depts", "deptA")]);
            assert_eq!(handler.assign(&exec(Some("depts"), data)).unwrap(), expected);
        }
    }

    #[test]
    fn t20_metadata_count_names_and_order() {
        let reg = crate::metadata::HandlerRegistry::new();
        let handlers = reg.list_handlers("AssignmentHandler");
        assert_eq!(handlers.len(), 13);
        let orders: Vec<i32> = handlers.iter().map(|handler| handler.order).collect();
        assert_eq!(orders, vec![-9999, 10, 20, 30, 40, 50, 60, 65, 70, 80, 90, 100, 110]);

        // issues/259 两枚插入后，部门两枚仍固定占据 90/100；FormPost 收尾在 110。
        assert_eq!(handlers[10].class_name, FQ_LEADER);
        assert_eq!(handlers[10].display_name, "表单指定部门负责人");
        assert_eq!(handlers[10].order, 90);
        assert_eq!(handlers[10].handler_type, "AssignmentHandler");
        assert_eq!(handlers[10].group, "built-in");
        assert_eq!(handlers[11].class_name, FQ_MAIN);
        assert_eq!(handlers[11].display_name, "表单指定部门分管领导");
        assert_eq!(handlers[11].order, 100);
        assert_eq!(handlers[11].handler_type, "AssignmentHandler");
        assert_eq!(handlers[11].group, "built-in");
    }
}
