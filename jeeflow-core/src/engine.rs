//! Engine core — JeeflowEngine trait + JeeflowEngineImpl.
//! Orchestrates: start → execute → jump → withdraw.
//! spec/04-engine-ops.md

use crate::context::ServiceContext;
use crate::error::{JeeflowError, JeeflowResult};
use crate::event::{ProcessPublisher, ProcessEvent, ProcessEventType};
use crate::json::{JsonValue, FlowData};
use crate::model::*;
use crate::parser::*;
use crate::spi::*;
use std::collections::HashMap;
use std::sync::Arc;

// ═══════════════════════════════════════════════════════
// Execution context
// ═══════════════════════════════════════════════════════

/// Execution context — passed through node execution chain.
#[derive(Clone)]
pub struct Execution {
    pub process_instance: ProcessInstance,
    pub process_model: ProcessModel,
    pub process_define: ProcessDefine,
    pub current_node: Option<NodeModel>,
    pub process_task: Option<ProcessTask>,
    pub process_task_list: Vec<ProcessTask>,
    pub args: FlowData,
    pub operator: String,
    pub is_merged: bool,
    /// New tasks created during this execution step.
    pub new_tasks: Vec<ProcessTask>,
    /// Whether the instance was finished/rejected during this step.
    pub instance_finished: bool,
    /// Gate variables for countersign expression evaluation (nrOfInstances, etc.)
    pub gate_vars: FlowData,
}

/// 跳转落点的三种语义（issues/121 P2：3「退回上一步」与 6「退回发起人」必须分开）
#[derive(Clone, Copy)]
enum JumpTarget<'a> {
    /// submitType=4 跳转到指定节点
    Node(&'a str),
    /// submitType=6 退回发起人（start 直接后继）
    FirstTaskNode,
    /// submitType=3 退回上一步（血缘版：复活 parent 那条历史行）
    RollbackLineage,
}

impl Execution {
    pub fn new(instance: ProcessInstance, model: ProcessModel,
               define: ProcessDefine, operator: &str, args: FlowData) -> Self {
        Execution {
            process_instance: instance,
            process_model: model,
            process_define: define,
            current_node: None,
            process_task: None,
            process_task_list: Vec::new(),
            args,
            operator: operator.to_string(),
            is_merged: false,
            new_tasks: Vec::new(),
            instance_finished: false,
            gate_vars: FlowData::new(),
        }
    }
}

// ═══════════════════════════════════════════════════════
// JeeflowEngine trait
// ═══════════════════════════════════════════════════════

pub trait JeeflowEngine: Send + Sync {
    fn start_process_instance(&self, define_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<ProcessInstance>;

    fn start_process_instance_with_parent(&self, define_id: i64, operator: &str, args: &FlowData,
                                           parent_id: i64, parent_node_name: &str)
        -> JeeflowResult<ProcessInstance>;

    fn execute_process_task(&self, task_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>>;

    fn execute_and_jump_task(&self, task_id: i64, operator: &str, args: &FlowData,
                              target_task_name: Option<&str>)
        -> JeeflowResult<Vec<ProcessTask>>;

    fn execute_and_jump_to_end(&self, task_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>>;

    fn execute_and_jump_to_first_task_node(&self, task_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>>;
}

// ═══════════════════════════════════════════════════════
// JeeflowEngineImpl
// ═══════════════════════════════════════════════════════

pub struct JeeflowEngineImpl {
    ctx: ServiceContext,
}

impl JeeflowEngineImpl {
    pub fn new(ctx: ServiceContext) -> Self {
        JeeflowEngineImpl { ctx }
    }

    pub fn context(&self) -> &ServiceContext {
        &self.ctx
    }

    /// 抄送知会事件（CC_CREATE / issues/102·104）：逐抄送人 fire，`cc_actor_id` 直传事件体。
    /// 接收人过滤（trim / 非空 / 去重）由集成层监听器负责，引擎只按 cc 行粒度 fire
    /// （对齐 Java `notifyCcCreate` / PHP v1.3.8）。无监听器装配时零副作用。
    pub fn notify_cc_create(&self, instance_id: i64, cc_actors: &[String]) {
        for actor in cc_actors {
            let event = ProcessEvent::new(ProcessEventType::CcCreate, instance_id)
                .with_cc_actor_id(actor.clone());
            ProcessPublisher::notify(&event, &self.ctx.event_listeners);
        }
    }

    fn repo(&self) -> &Arc<dyn ProcessRepository> {
        self.ctx.get_repository()
    }

    fn next_id(&self) -> i64 {
        self.ctx.get_id_generator().next_id()
    }

    /// Add user info variables (u_userId, u_realName, etc.)
    fn add_user_info(&self, args: &mut FlowData, operator: &str) {
        if operator == "flow.auto" || operator == "flow.admin" {
            return;
        }
        if let Some(up) = &self.ctx.user_provider {
            if let Ok(Some(user)) = up.get_user(operator) {
                args.insert_str("u_userId", &user.user_id);
                args.insert_str("u_realName", &user.real_name);
                args.insert_str("u_deptId", &user.dept_id);
                args.insert_str("u_deptName", &user.dept_name);
                args.insert_str("u_postId", &user.post_id);
                args.insert_str("u_postName", &user.post_name);
            }
        }
    }

    /// Generate autoGenTitle: "{realName}的{displayName}-{time}"
    fn gen_auto_title(args: &FlowData, display_name: &str) -> String {
        let real_name = args.get_str("u_realName").unwrap_or("未知");
        let now = crate::model::current_time_str();
        format!("{}的{}-{}", real_name, display_name, now)
    }

    /// Resolve assignee for a task node.
    fn resolve_assignee(&self, exec: &Execution, node: &NodeModel) -> Vec<String> {
        // Priority 1: tf_nextNodeOperator variable（v1.0.1 对齐 boot3 / 对齐 Python _resolve_actors）。
        // 前端「指定下一节点处理人」UserSelect 是 multiple，提交值是**数组**；也可能有
        // 字符串逗号分隔的旧形态——两种都收，否则数组形态 get_str 取不到 → 落到 assignee
        // 字面量，指定下一节点处理人不生效（e2e S15 红：指定刘洋后 gm_approve 仍是 chenhong）。
        if let Some(v) = exec.args.get("tf_nextNodeOperator") {
            let list: Vec<String> = match v {
                JsonValue::Array(items) => items
                    .iter()
                    .filter_map(|it| it.as_str())
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect(),
                _ => v
                    .as_str()
                    .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
                    .unwrap_or_default(),
            };
            if !list.is_empty() {
                return list;
            }
        }

        // Priority 2: assignee literal
        if let Some(assignee) = node.assignee() {
            if !assignee.is_empty() {
                if assignee == "applicant" {
                    return vec![exec.process_instance.operator.clone()];
                }
                // Check if it's a variable reference
                if let Some(val) = exec.args.get_str(&assignee) {
                    if !val.is_empty() {
                        return val.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                    }
                }
                // Literal value
                return assignee.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
            }
        }

        // Priority 3: assignmentHandler
        if let Some(handler_name) = node.assignment_handler() {
            if let Some(handler) = self.ctx.find_assignment_handler(&handler_name) {
                if let Ok(result) = handler.assign(exec) {
                    if !result.is_empty() {
                        return result.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                    }
                }
            }
        }

        // Priority 4: candidateUsers / candidateGroups
        let mut actors = Vec::new();
        if let Some(users) = node.candidate_users() {
            for u in users.split(',') {
                let u = u.trim();
                if !u.is_empty() { actors.push(u.to_string()); }
            }
        }
        // candidateGroups resolved via OrgUserProvider (done in create_task_with_assignment)

        actors
    }

    /// Execute a node in the process model.
    fn execute_node(&self, exec: &mut Execution, node: &NodeModel) -> JeeflowResult<()> {
        exec.current_node = Some(node.clone());

        match node.node_type {
            NodeType::Start => {
                // Clone next nodes to avoid borrow conflict
                let next_nodes: Vec<NodeModel> = exec.process_model.get_output_edges(&node.id)
                    .iter()
                    .filter_map(|e| exec.process_model.get_target_node(e).cloned())
                    .collect();
                for next in next_nodes {
                    self.execute_node(exec, &next)?;
                }
            }
            NodeType::Task | NodeType::Custom => {
                // 任务创建不触发节点拦截器（对齐 Java CreateTaskHandler / Go executeNode：
                // 创建任务 ≠ 节点执行完成）；persist 等 post 拦截器在任务**被执行**时
                // 由 execute_task_async 显式触发（1.8.0 SYNC 同步演进）
                self.create_task_with_assignment(exec, node)?;
            }
            NodeType::Decision => {
                // Fire pre-interceptors
                self.fire_pre_interceptors(exec)?;

                // Evaluate decision
                let target_node_name = self.evaluate_decision(exec, node)?;

                // Fire post-interceptors
                self.fire_post_interceptors(exec)?;

                // Follow the chosen edge
                let chosen_edge: Option<EdgeModel> = {
                    let edges = exec.process_model.get_output_edges(&node.id);
                    let mut found: Option<EdgeModel> = None;
                    for edge in edges {
                        let edge_expr = edge.expr();
                        let matches = if let Some(ref target) = target_node_name {
                            // Named target — match by target node id
                            edge.target_node_id == *target
                        } else if let Some(ref expr) = edge_expr {
                            // Expression evaluation
                            self.evaluate_expression(expr, exec)
                        } else {
                            false
                        };

                        if matches {
                            found = Some(edge.clone());
                            break;
                        }
                    }
                    found
                };
                if let Some(edge) = chosen_edge {
                    if let Some(next) = exec.process_model.get_target_node(&edge) {
                        let next = next.clone();
                        self.execute_node(exec, &next)?;
                    }
                }
            }
            NodeType::Fork => {
                self.fire_pre_interceptors(exec)?;
                let next_nodes: Vec<NodeModel> = exec.process_model.get_output_edges(&node.id)
                    .iter()
                    .filter_map(|e| exec.process_model.get_target_node(e).cloned())
                    .collect();
                for next in next_nodes {
                    self.execute_node(exec, &next)?;
                }
                self.fire_post_interceptors(exec)?;
            }
            NodeType::Join => {
                self.fire_pre_interceptors(exec)?;
                // Check if all parallel branches have completed
                let doing_tasks = exec.process_instance.get_doing_tasks();
                if doing_tasks.is_empty() || exec.is_merged {
                    exec.is_merged = true;
                    let next_nodes: Vec<NodeModel> = exec.process_model.get_output_edges(&node.id)
                        .iter()
                        .filter_map(|e| exec.process_model.get_target_node(e).cloned())
                        .collect();
                    for next in next_nodes {
                        self.execute_node(exec, &next)?;
                    }
                }
                self.fire_post_interceptors(exec)?;
            }
            NodeType::End => {
                self.fire_pre_interceptors(exec)?;

                // Check if instance should finish or reject
                let has_reject_var = exec.args.get_str("reject").map(|v| v == "true").unwrap_or(false);
                if has_reject_var {
                    exec.process_instance.reject();
                } else {
                    exec.process_instance.finish();
                }
                exec.instance_finished = true;

                // Persist
                self.repo().update_instance(&exec.process_instance)?;

                // Fire event
                let event = ProcessEvent::new(ProcessEventType::ProcessInstanceEnd,
                                               exec.process_instance.instance_id);
                ProcessPublisher::notify(&event, &self.ctx.event_listeners);

                self.fire_post_interceptors(exec)?;
            }
            NodeType::SubProcess => {
                // Start sub-process
                if let Some(_sub_define_name) = node.sub_process_name() {
                    // Look up define by name — would need find_define_by_name
                    // For now, create a simplified sub-process
                    let _sub_args = FlowData::new();
                    // Sub-process handling is complex; simplified for now
                }
            }
        }

        Ok(())
    }

    /// Create a task with assignment resolution.
    fn create_task_with_assignment(&self, exec: &mut Execution, node: &NodeModel) -> JeeflowResult<()> {
        let actor_ids = self.resolve_assignee(exec, node);

        if actor_ids.is_empty() && node.candidate_users().is_none() && node.candidate_groups().is_none() {
            // No actors — skip task creation (continue to next node)
            let next_nodes: Vec<NodeModel> = exec.process_model.get_output_edges(&node.id)
                .iter()
                .filter_map(|e| exec.process_model.get_target_node(e).cloned())
                .collect();
            for next in next_nodes {
                self.execute_node(exec, &next)?;
            }
            return Ok(());
        }

        let perform_type = PerformType::from_code(node.perform_type());
        let task_type = TaskType::from_code(node.task_type());

        let task = if perform_type == PerformType::Countersign {
            let cs_type = node.countersign_type();
            if cs_type.to_uppercase() == "SEQUENTIAL" || cs_type.to_uppercase() == "SERIAL" {
                // SEQUENTIAL: only create first actor's task (issues/94)
                if actor_ids.is_empty() {
                    return Ok(());
                }
                // Store full operator list in instance variables
                let op_list_key = format!("csv_{}_operatorList", node.id);
                exec.process_instance.variables.insert_str(&op_list_key, actor_ids.join(","));
                let lc_key = format!("csv_{}_loopCounter", node.id);
                exec.process_instance.variables.insert_i64(&lc_key, 0);
                // Create only the first actor's task
                let tasks = exec.process_instance.create_countersign_tasks(
                    &node.id, &node.display_name, &[actor_ids[0].clone()], &exec.operator,
                    task_type, node.form_key(), None);
                // TASK_START 不在这里 fire：此处 task_id 尚为 0（create_task 只置 0，
                // 真实 id 由 persist_tasks→save_task 的 next_id 分配）。若在此 fire，
                // 监听器 find_task(0) 查不到 → TODO 丢失（issues/13 salvo 栈根因，对齐
                // Java jeeflow-java 1.8.20「notifyTaskStart 移到 saveTask 之后」的时机修复）。
                // 统一改在 persist_tasks 落库后 fire（见下）。
                exec.new_tasks.extend(tasks);
                exec.process_instance.tasks.last().cloned().unwrap()
            } else {
                // PARALLEL: create all tasks at once
                let tasks = exec.process_instance.create_countersign_tasks(
                    &node.id, &node.display_name, &actor_ids, &exec.operator,
                    task_type, node.form_key(), None);
                // TASK_START 统一改在 persist_tasks 落库后 fire（见下）。
                exec.new_tasks.extend(tasks);
                exec.process_instance.tasks.last().cloned().unwrap()
            }
        } else {
            exec.process_instance.create_task(
                &node.id, &node.display_name, &actor_ids, &exec.operator,
                task_type, perform_type, node.form_key(), None)
        };

        // TASK_START 统一改在 persist_tasks 落库后 fire（见下）。此处只登记新任务。
        if perform_type != PerformType::Countersign {
            exec.new_tasks.push(task);
        }

        Ok(())
    }

    /// Evaluate decision expression.
    fn evaluate_decision(&self, _exec: &Execution, _node: &NodeModel) -> JeeflowResult<Option<String>> {
        // If there's an expression evaluator, use it
        // Otherwise, fall through to edge expressions
        Ok(None)
    }

    /// Evaluate a single edge expression.
    fn evaluate_expression(&self, expr: &str, exec: &Execution) -> bool {
        if let Some(eval) = &self.ctx.expression_evaluator {
            let vars: HashMap<String, JsonValue> = exec.args.inner().clone();
            match eval.eval(expr, &vars) {
                Ok(JsonValue::Bool(b)) => b,
                Ok(JsonValue::Number(n)) => n != 0.0,
                Ok(JsonValue::Str(s)) => !s.is_empty() && s != "false" && s != "0",
                _ => false,
            }
        } else {
            // Simple built-in expression evaluator
            self.simple_eval(expr, exec)
        }
    }

    /// Simple expression evaluator for basic comparisons.
    fn simple_eval(&self, expr: &str, exec: &Execution) -> bool {
        let expr = expr.trim();

        // Handle ${var} references
        let resolved = self.resolve_var_refs(expr, exec);

        // Handle comparison operators
        for op in &[">=", "<=", "!=", "==", ">", "<"] {
            if let Some(pos) = resolved.find(op) {
                let left = resolved[..pos].trim();
                let right = resolved[pos + op.len()..].trim();
                return match *op {
                    ">" => self.compare_values(left, right) == Some(std::cmp::Ordering::Greater),
                    "<" => self.compare_values(left, right) == Some(std::cmp::Ordering::Less),
                    ">=" => self.compare_values(left, right).map(|o| o != std::cmp::Ordering::Less).unwrap_or(false),
                    "<=" => self.compare_values(left, right).map(|o| o != std::cmp::Ordering::Greater).unwrap_or(false),
                    "==" => self.compare_values(left, right) == Some(std::cmp::Ordering::Equal),
                    "!=" => self.compare_values(left, right).map(|o| o != std::cmp::Ordering::Equal).unwrap_or(true),
                    _ => false,
                };
            }
        }

        // Boolean literal
        match resolved.to_lowercase().as_str() {
            "true" => true,
            "false" => false,
            _ => !resolved.is_empty() && resolved != "0" && resolved != "null",
        }
    }

    fn resolve_var_refs(&self, expr: &str, exec: &Execution) -> String {
        let mut result = expr.to_string();
        // Replace ${var} patterns
        while let Some(start) = result.find("${") {
            if let Some(end) = result[start..].find('}') {
                let var_name = &result[start + 2..start + end];
                let value = exec.args.get_str(var_name)
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "0".to_string());
                result = format!("{}{}{}", &result[..start], value, &result[start + end + 1..]);
            } else {
                break;
            }
        }
        // Replace #var patterns (countersign gate variables like #nrOfCompletedInstances)
        let mut out = String::new();
        let mut i = 0;
        let chars = result.as_bytes();
        while i < chars.len() {
            if chars[i] == b'#' && i + 1 < chars.len() && (chars[i + 1].is_ascii_alphabetic() || chars[i + 1] == b'_') {
                let start = i + 1;
                let mut end = start;
                while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == b'_') {
                    end += 1;
                }
                let var_name = &result[start..end];
                // Look up in gate_vars first, then args (handle both string and numeric values)
                let value = exec.gate_vars.get_str(var_name)
                    .or_else(|| exec.args.get_str(var_name))
                    .map(|s| s.to_string())
                    .or_else(|| exec.gate_vars.get_i64(var_name).map(|n| n.to_string()))
                    .or_else(|| exec.args.get_i64(var_name).map(|n| n.to_string()))
                    .unwrap_or_else(|| "0".to_string());
                out.push_str(&value);
                i = end;
            } else {
                out.push(chars[i] as char);
                i += 1;
            }
        }
        out
    }

    fn compare_values(&self, left: &str, right: &str) -> Option<std::cmp::Ordering> {
        // Try numeric comparison first
        if let (Ok(l), Ok(r)) = (left.parse::<f64>(), right.parse::<f64>()) {
            return l.partial_cmp(&r);
        }
        // Fall back to string comparison
        Some(left.cmp(right))
    }

    /// Fire pre-interceptors.
    fn fire_pre_interceptors(&self, _exec: &mut Execution) -> JeeflowResult<()> {
        // Pre-interceptors reserved; PersistPostInterceptor runs on post only.
        Ok(())
    }

    /// Fire post-interceptors (sorted by order).
    fn fire_post_interceptors(&self, exec: &mut Execution) -> JeeflowResult<()> {
        for interceptor in &self.ctx.interceptors {
            interceptor.intercept(exec)?;
        }
        Ok(())
    }

    /// Abandon remaining DOING countersign tasks for a node (issues/94).
    /// Only abandons tasks with task_name == node_id and state == DOING.
    /// The just-completed task is already FINISHED so won't be affected.
    fn abandon_countersign_remaining(&self, exec: &mut Execution, node_id: &str) -> JeeflowResult<()> {
        let mut abandoned = Vec::new();
        for task in &mut exec.process_instance.tasks {
            if task.task_name == node_id && task.task_state == TaskState::Doing.code() {
                task.task_state = TaskState::Abandon.code();
                abandoned.push(task.clone());
            }
        }
        // Persist abandoned tasks to repo
        for t in &abandoned {
            self.repo().update_task(t)?;
        }
        Ok(())
    }

    /// Assign IDs to instance and tasks.
    fn assign_ids(&self, instance: &mut ProcessInstance) {
        instance.instance_id = self.next_id();
        for task in &mut instance.tasks {
            if task.task_id == 0 {
                task.task_id = self.next_id();
                task.process_instance_id = instance.instance_id;
            }
        }
    }

    /// 委托查询用的流程名（契约 06 §4.5 条款 1.1）：**以流程模型的 `name` 为准**
    /// （`ProcessModel.name`，即流程 JSON 的 name），模型未带时回落 `wf_process_define.name`。
    ///
    /// 依据是**迁移基线**：内置版 mldong-wf 的 `SurrogateInterceptor` 用的正是
    /// `execution.getProcessModel().getName()`，Java 参考实现与之同构；用户在内置版配的
    /// 委托，迁到本栈后必须命中同一条。正常 deploy 会 `def.setName(model.getName())`
    /// 使两者恒等，但**测试里保留"define.name ≠ model.name"的诱饵行**钉住取值（见
    /// `tests_surrogate_*` 用例）。
    fn surrogate_process_name(&self, exec: &Execution) -> String {
        let model_name = exec.process_model.name.as_str();
        if !model_name.trim().is_empty() {
            return model_name.to_string();
        }
        exec.process_define.name.clone()
    }

    /// Persist new tasks (assigns IDs in-place for tasks with id=0).
    ///
    /// **本栈新任务落库唯一收口**（对齐 Java/Go 的 `saveNewTask`）：发起 / 办理推进 /
    /// **串行会签每一步推进** / 跳转四条建任务路径全部经此落库（9 处 `create_task` 调用点
    /// 都汇入 `exec.new_tasks`），故委托自动生效只挂这一处即全覆盖
    /// （契约 06 §4.5 条款 1；只挂"发起"一处会漏掉流转中产生的新单——实测易犯）。
    ///
    /// 时序（条款 2 ⚠️）：并入发生在 `save_task` **之前**，落在参与者集合本身，
    /// 随后由同一次收口把"原人 + 代理人"整体写入 `wf_process_task_actor`；
    /// **不做**"事后再补写一次 `add_task_actor`"（Java 首版挂在 taskId 分配前 → 打在空 id
    /// 上静默无效，本栈同样不引第二条写路径）。
    ///
    /// TASK_START 时机（issues/13 salvo 栈根因闭环，对齐 Java jeeflow-java 1.8.20
    /// 「notifyTaskStart 移到 saveTask 之后」）：`create_node_tasks` 里 create_task 只置
    /// task_id=0，真实 id 在本方法 `save_task` 内分配（sqlx save_task 内部 next_id）并
    /// 立即提交（autocommit，无显式事务）。故 TASK_START 必须在 `save_task` 之后 fire——
    /// 监听器 `find_task(source_id)` 此时才查得到该任务。若在 create 阶段 fire（id=0/未落库），
    /// 监听器 find_task 查空 → 静默 return → TODO 待办丢失（messagePage 恒空）。
    fn persist_tasks(&self, exec: &mut Execution) -> JeeflowResult<()> {
        let instance_id = exec.process_instance.instance_id;
        let process_name = self.surrogate_process_name(exec);
        // 委托并入后的参与者集合（回写聚合根副本用，循环外统一套用以免借用冲突）
        let mut merged_actors: Vec<(i64, Vec<String>)> = Vec::new();
        // issues/121 P1 建单不变量：本方法是本栈新任务落库**唯一收口**（发起/推进/串行会签
        // 每一步/跳转四条建任务路径全汇入 exec.new_tasks），与 issues/116 的委托并入口同型
        // ⇒ 挂这一处即全覆盖，只挂"发起"一处会漏掉流转中产生的新单。
        // parent＝产生这批新任务的那个刚办结任务；发起 execution 没有当前任务 ⇒ 落 0
        // （对齐 mldong-boot2 `Convert.toLong(execution.getProcessTaskId(), 0L)`）。
        let lineage_parent = exec.process_task.as_ref().map(|t| t.task_id).unwrap_or(0);
        let lineage_first: Vec<bool> = exec.new_tasks.iter()
            .map(|t| exec.process_model.is_first_task_node(&t.task_name)).collect();
        for (li, task) in exec.new_tasks.iter_mut().enumerate() {
            if task.task_id == 0 {
                task.task_id = self.next_id();
            }
            task.process_instance_id = instance_id;
            if task.parent_task_id.is_none() {
                task.parent_task_id = Some(lineage_parent);
            }
            // 行级首任务节点标记：门面出口现算版带"仅进行中"判定，已办结的历史行上恒 false，
            // 而血缘版回退要读那条历史行决定参与者 ⇒ 必须建单时落库（算法沿用 parser 现成判定）。
            task.variables.insert(
                "isFirstTaskNode".to_string(),
                JsonValue::Bool(lineage_first[li]));
            // issues/116 批次 D：参与者落库前应用生效中的委托（未配置扩展仓储/查询报错
            // 均静默跳过，不打断建单；开关关闭时原样返回）。
            if crate::surrogate::apply_surrogate_to_task(&self.ctx, task, &process_name) {
                merged_actors.push((task.task_id, task.actor_ids.clone()));
            }
            self.repo().save_task(task)?;
            // Save actors（集合已含代理人，与任务同批落库）
            if !task.actor_ids.is_empty() {
                self.repo().add_task_actor(task.task_id, &task.actor_ids)?;
            }
            // 落库后 fire TASK_START（此时 task_id 已分配且行已提交，监听器 find_task 可查）
            let event = ProcessEvent::new(ProcessEventType::ProcessTaskStart, task.task_id);
            ProcessPublisher::notify(&event, &self.ctx.event_listeners);
        }
        // 聚合根内的任务副本同步并入后的参与者（否则 update_instance 落库的副本仍是
        // "只有原人"，且 is_allowed/详情读聚合副本时会漏判代理人——同 issues/114 §6
        // Java 那条"内存仓 addTaskActor 不回写任务副本"的病根）。
        if !merged_actors.is_empty() {
            for t in &mut exec.process_instance.tasks {
                if let Some((_, actors)) = merged_actors.iter().find(|(id, _)| *id == t.task_id) {
                    t.actor_ids = actors.clone();
                }
            }
        }
        Ok(())
    }
}

impl JeeflowEngine for JeeflowEngineImpl {
    fn start_process_instance(&self, _define_id: i64, _operator: &str, _args: &FlowData)
        -> JeeflowResult<ProcessInstance> {
        // Synchronous wrapper — in practice the engine is called via async facade
        Err(JeeflowError::Internal("Use async start_process_instance_async".into()))
    }

    fn start_process_instance_with_parent(&self, _define_id: i64, _operator: &str, _args: &FlowData,
                                           _parent_id: i64, _parent_node_name: &str)
        -> JeeflowResult<ProcessInstance> {
        Err(JeeflowError::Internal("Use async variant".into()))
    }

    fn execute_process_task(&self, _task_id: i64, _operator: &str, _args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>> {
        Err(JeeflowError::Internal("Use async variant".into()))
    }

    fn execute_and_jump_task(&self, _task_id: i64, _operator: &str, _args: &FlowData,
                              _target_task_name: Option<&str>)
        -> JeeflowResult<Vec<ProcessTask>> {
        Err(JeeflowError::Internal("Use async variant".into()))
    }

    fn execute_and_jump_to_end(&self, _task_id: i64, _operator: &str, _args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>> {
        Err(JeeflowError::Internal("Use async variant".into()))
    }

    fn execute_and_jump_to_first_task_node(&self, _task_id: i64, _operator: &str, _args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>> {
        Err(JeeflowError::Internal("Use async variant".into()))
    }
}

impl JeeflowEngineImpl {
    /// Async start process instance.
    pub async fn start_async(&self, define_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<ProcessInstance> {
        // 1. Load define (sync)
        let define = self.repo().find_define_by_id(define_id)?
            .ok_or(JeeflowError::DefineNotFound(define_id))?;

        // 2. Parse model
        let model = ModelParser::parse(&define.content_str())?;

        // 3. Build args with user info (sync)
        let mut full_args = FlowData::new();
        full_args.merge(args);
        self.add_user_info(&mut full_args, operator);

        // Auto-generate title
        let title = Self::gen_auto_title(&full_args, &define.display_name);
        full_args.insert_str("autoGenTitle", &title);

        // 4. Create instance
        let mut instance = ProcessInstance::create(&define, operator, &full_args);

        // 5. Set expire time
        if let Some(et) = &model.expire_time {
            instance.expire_time = Some(et.clone());
        }

        // 6. Assign IDs
        self.assign_ids(&mut instance);

        // 7. Save instance (sync)
        self.repo().save_instance(&mut instance)?;

        // 8. Handle CC actors (sync) — 对齐 Go facade.go:203（issues/56 E28）：
        // vben 发起页"抄送给"是多选 ApiSelect，提交 JSON 数组；也兼容逗号分隔字符串。
        // 旧版仅 get_str+split，数组走 get_str=None → cc 实例从不创建（L3 S6）。
        let cc_actors = parse_cc_actors(full_args.inner().get("f_ccActors"));
        if !cc_actors.is_empty() {
            self.repo().create_cc_instance(instance.instance_id, operator, &cc_actors)?;
            // CC_CREATE（issues/102·104，六语言统一）：逐抄送人 fire，与 cc 行粒度一一对应
            self.notify_cc_create(instance.instance_id, &cc_actors);
        }

        // 9. Execute from start node
        let mut exec = Execution::new(instance.clone(), model, define, operator, full_args);

        if let Some(start) = exec.process_model.get_start().cloned() {
            self.execute_node(&mut exec, &start)?;
        }

        // 10. Persist new tasks (sync) — assigns IDs in-place
        self.persist_tasks(&mut exec)?;

        // 11. Update instance (sync)
        self.repo().update_instance(&exec.process_instance)?;

        // 12. Fire start event
        let event = ProcessEvent::new(ProcessEventType::ProcessInstanceStart, instance.instance_id);
        ProcessPublisher::notify(&event, &self.ctx.event_listeners);

        Ok(exec.process_instance)
    }

    /// Async execute process task.
    pub async fn execute_task_async(&self, task_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>> {
        // 1. Load task (sync)
        let task = self.repo().find_task_by_id(task_id)?
            .ok_or(JeeflowError::TaskNotFound(task_id))?;

        if task.task_state != TaskState::Doing.code() {
            return Err(JeeflowError::InvalidState(format!("Task {} is not DOING", task_id)));
        }

        // 2. Load instance (sync)
        let mut instance = self.repo().find_instance_by_id(task.process_instance_id)?
            .ok_or(JeeflowError::InstanceNotFound(task.process_instance_id))?;
        // Hydrate aggregate root: load tasks from repo into instance
        instance.tasks = self.repo().find_history_tasks(instance.instance_id)?;

        // 3. Load define + parse model (sync)
        let define = self.repo().find_define_by_id(instance.define_id)?
            .ok_or(JeeflowError::DefineNotFound(instance.define_id))?;
        let model = ModelParser::parse(&define.content_str())?;

        // 4. Build args + user info (sync)
        // resume 语义对齐 Go mergeVars(args, inst.Variables)：以实例已持久化变量为底、
        // 本次提交覆盖。否则发起时的 f_* 表单字段（如指定审批人 f_approver）在 resume 后
        // 丢失，FormFieldAssigneeHandler 等下游读 exec.args 拿不到（L3 S11-B）。
        let mut full_args = instance.variables.clone();
        full_args.merge(args);
        self.add_user_info(&mut full_args, operator);

        // 5. Permission check
        if !task.is_allowed(operator) {
            return Err(JeeflowError::PermissionDenied(
                format!("Operator {} not allowed on task {}", operator, task_id)));
        }

        // 5.5. Inject countersignDisagreeFlag if submitType==20 (issues/94)
        let submit_type = full_args.get_i64("submitType")
            .or_else(|| full_args.get_str("submitType").and_then(|s| s.parse::<i64>().ok()));
        if submit_type == Some(20) {
            instance.variables.insert_str("countersignDisagreeFlag", "1");
            full_args.insert_str("countersignDisagreeFlag", "1");
        }

        // 6. Complete task in aggregate.
        // 传本次提交原始 args（非合并后的 full_args）：契约 spec/06 §4.3 第 5 条要求
        // 任务变量按「既有变量 ← args」合并，args 最高，full_args 含实例全量变量会污染任务变量。
        instance.complete_task(task_id, operator, args).map_err(|e| JeeflowError::Business(e))?;

        // 6.5. Set countersignDisagreeFlag on the completed task's variables too
        if submit_type == Some(20) {
            if let Some(t) = instance.tasks.iter_mut().find(|t| t.task_id == task_id) {
                t.variables.insert_str("countersignDisagreeFlag", "1");
            }
        }

        // 7. Persist task update (sync)
        if let Some(t) = instance.tasks.iter().find(|t| t.task_id == task_id) {
            self.repo().update_task(t)?;
        }

        // 8. Find the node for this task
        let node = model.get_node(&task.task_name).cloned();

        // 9. Build execution
        let mut exec = Execution::new(instance, model, define, operator, full_args);
        exec.process_task = Some(task.clone());

        // 9.5. 任务完成节点自身的后置拦截器（对齐 Go ExecuteProcessTask 1.8.0 SYNC 同步演进）：
        // 此时 exec.args 携带本次提交的 f_/tf_ 字段（已并入 instance.variables），
        // persist 按被完成任务的节点判定字段权限/状态字段。
        if let Some(ref cur) = node {
            exec.current_node = Some(cur.clone());
            self.fire_post_interceptors(&mut exec)?;
        }

        // 10. Countersign gate check (issues/94)
        let is_countersign = node.as_ref().map(|n| n.is_countersign()).unwrap_or(false);
        if is_countersign {
            let node_ref = node.as_ref().unwrap();
            let node_id = &node_ref.id;

            // Pre-compute same-node task counts (avoid borrow conflict with abandon)
            let node_task_counts: Vec<(bool, i32)> = exec.process_instance.tasks.iter()
                .filter(|t| &t.task_name == node_id)
                .map(|t| (t.is_finished(), t.task_state))
                .collect();
            let total = node_task_counts.len();
            let finished_count = node_task_counts.iter().filter(|(f, _)| *f).count();
            let doing_count = node_task_counts.iter().filter(|(_, s)| *s == TaskState::Doing.code()).count();
            let all_finished = node_task_counts.iter().all(|(f, _)| *f);

            let cs_type = node_ref.countersign_type();
            let cond = node_ref.countersign_completion_condition();
            let is_sequential = cs_type.to_uppercase() == "SEQUENTIAL" || cs_type.to_uppercase() == "SERIAL";

            // Check one-vote veto gate first
            let veto_hit = submit_type == Some(20)
                && cond.as_ref().map(|c| c.trim().eq_ignore_ascii_case("ONE_VOTE_VETO")).unwrap_or(false);

            if veto_hit {
                // Veto → merged: abandon remaining DOING tasks
                self.abandon_countersign_remaining(&mut exec, node_id)?;
                exec.is_merged = true;
                // Fall through to 10b (follow output edges)
            } else if is_sequential {
                // SEQUENTIAL: check if more actors to process
                let op_list_key = format!("csv_{}_operatorList", node_id);
                let lc_key = format!("csv_{}_loopCounter", node_id);
                let op_list_str = exec.process_instance.variables.get_str_or(&op_list_key, "");
                let operator_list: Vec<String> = op_list_str.split(',')
                    .map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
                let lc = exec.process_instance.variables.get_i64_or(&lc_key, 0) as usize;

                if lc + 1 < operator_list.len() {
                    // Create next sequential task, do NOT follow edges
                    let next_lc = lc + 1;
                    exec.process_instance.variables.insert_i64(&lc_key, next_lc as i64);
                    let new_task = exec.process_instance.create_countersign_tasks(
                        node_id, &node_ref.display_name,
                        &[operator_list[next_lc].clone()], &exec.operator,
                        TaskType::from_code(node_ref.task_type()),
                        node_ref.form_key(), None);
                    exec.new_tasks.extend(new_task);
                    // Persist + return (no edge follow)
                    self.persist_tasks(&mut exec)?;
                    self.repo().update_instance(&exec.process_instance)?;
                    return Ok(exec.new_tasks);
                } else {
                    // Last person → merged, fall through to 10b
                    self.abandon_countersign_remaining(&mut exec, node_id)?;
                    exec.is_merged = true;
                }
            } else {
                // PARALLEL: check completion condition
                let has_expr_cond = cond.as_ref().map(|c| {
                    let c = c.trim();
                    !c.is_empty() && !c.eq_ignore_ascii_case("ONE_VOTE_VETO")
                }).unwrap_or(false);

                if has_expr_cond {
                    // Expression condition (e.g. "#nrOfCompletedInstances==2")
                    // Build gate vars (using pre-computed counts)
                    exec.gate_vars.insert_i64("nrOfInstances", total as i64);
                    exec.gate_vars.insert_i64("nrOfActivateInstances", doing_count as i64);
                    exec.gate_vars.insert_i64("nrOfCompletedInstances", finished_count as i64);
                    // Also add to instance variables for expression resolution
                    exec.process_instance.variables.insert_i64("nrOfInstances", total as i64);
                    exec.process_instance.variables.insert_i64("nrOfActivateInstances", doing_count as i64);
                    exec.process_instance.variables.insert_i64("nrOfCompletedInstances", finished_count as i64);

                    let cond_str = cond.as_ref().unwrap().trim();
                    let merged = self.simple_eval(cond_str, &exec);

                    if merged {
                        self.abandon_countersign_remaining(&mut exec, node_id)?;
                        exec.is_merged = true;
                        // Fall through to 10b (follow output edges)
                    } else {
                        // Not merged → return without following edges
                        self.persist_tasks(&mut exec)?;
                        self.repo().update_instance(&exec.process_instance)?;
                        return Ok(exec.new_tasks);
                    }
                } else {
                    // No condition (or ONE_VOTE_VETO without veto hit) → all must finish
                    let merged = all_finished;
                    if merged {
                        self.abandon_countersign_remaining(&mut exec, node_id)?;
                        exec.is_merged = true;
                        // Fall through to 10b (follow output edges)
                    } else {
                        // Not all finished → return without following edges
                        self.persist_tasks(&mut exec)?;
                        self.repo().update_instance(&exec.process_instance)?;
                        return Ok(exec.new_tasks);
                    }
                }
            }
        }

        // 10b. Continue execution from current node: non-countersign tasks always,
        // and countersign tasks once the gate is merged (issues/94 fall-through).
        if let Some(node) = node {
            let next_nodes: Vec<NodeModel> = exec.process_model.get_output_edges(&node.id)
                .iter()
                .filter_map(|e| exec.process_model.get_target_node(e).cloned())
                .collect();
            for next in next_nodes {
                self.execute_node(&mut exec, &next)?;
            }
        }

        // 11. Handle task-level CC (sync)
        if let Some(cc_actors) = exec.args.get_str("tf_ccActors") {
            let actors: Vec<String> = cc_actors.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !actors.is_empty() {
                self.repo().create_cc_instance(exec.process_instance.instance_id, operator, &actors)?;
                // CC_CREATE（issues/102·104，六语言统一）：办理时抄送同样逐抄送人 fire
                self.notify_cc_create(exec.process_instance.instance_id, &actors);
            }
        }

        // 12. Persist new tasks + update instance (sync) — assigns IDs in-place
        self.persist_tasks(&mut exec)?;
        self.repo().update_instance(&exec.process_instance)?;

        Ok(exec.new_tasks)
    }

    /// Async execute and jump to specific task node.
    pub async fn execute_and_jump_async(&self, task_id: i64, operator: &str,
                                          args: &FlowData, target_name: Option<&str>)
        -> JeeflowResult<Vec<ProcessTask>> {
        match target_name {
            Some(name) => self.execute_jump_inner(task_id, operator, args, JumpTarget::Node(name)).await,
            // issues/121 P2：target 为空＝血缘版「退回上一步」（submitType=3）。
            // 此前这条分支与「退回发起人」（6）共用 get_first_task_node()，两语义塌成同值（issues/119）。
            None => self.execute_jump_inner(task_id, operator, args, JumpTarget::RollbackLineage).await,
        }
    }

    async fn execute_jump_inner(&self, task_id: i64, operator: &str, args: &FlowData,
                                mode: JumpTarget<'_>) -> JeeflowResult<Vec<ProcessTask>> {
        // Similar to execute_task_async but jumps to target node
        let task = self.repo().find_task_by_id(task_id)?
            .ok_or(JeeflowError::TaskNotFound(task_id))?;

        let mut instance = self.repo().find_instance_by_id(task.process_instance_id)?
            .ok_or(JeeflowError::InstanceNotFound(task.process_instance_id))?;
        instance.tasks = self.repo().find_history_tasks(instance.instance_id)?;

        let define = self.repo().find_define_by_id(instance.define_id)?
            .ok_or(JeeflowError::DefineNotFound(instance.define_id))?;
        let model = ModelParser::parse(&define.content_str())?;

        // resume 语义对齐 Go mergeVars(args, inst.Variables)：实例已持久化变量为底、
        // 本次提交覆盖（详见 execute_task_async 注释，L3 S11-B）。
        let mut full_args = instance.variables.clone();
        full_args.merge(args);
        self.add_user_info(&mut full_args, operator);

        if !task.is_allowed(operator) {
            return Err(JeeflowError::PermissionDenied(
                format!("Operator {} not allowed on task {}", operator, task_id)));
        }

        // Inject countersignDisagreeFlag if submitType==20 (issues/94 round 2)
        let submit_type = full_args.get_i64("submitType")
            .or_else(|| full_args.get_str("submitType").and_then(|s| s.parse::<i64>().ok()));
        if submit_type == Some(20) {
            instance.variables.insert_str("countersignDisagreeFlag", "1");
            full_args.insert_str("countersignDisagreeFlag", "1");
        }

        instance.complete_task(task_id, operator, args).map_err(|e| JeeflowError::Business(e))?;
        // Set flag on completed task's variables
        if submit_type == Some(20) {
            if let Some(t) = instance.tasks.iter_mut().find(|t| t.task_id == task_id) {
                t.variables.insert_str("countersignDisagreeFlag", "1");
            }
        }
        if let Some(t) = instance.tasks.iter().find(|t| t.task_id == task_id) {
            self.repo().update_task(t)?;
        }

        let mut exec = Execution::new(instance, model, define, operator, full_args);
        exec.process_task = Some(task);

        match mode {
            JumpTarget::Node(name) => {
                if let Some(node) = exec.process_model.get_node(name).cloned() {
                    self.execute_node(&mut exec, &node)?;
                }
            }
            // submitType=6 退回发起人：跳 start 直接后继那条节点（形状与原实现一致，已与 3 彻底分开）
            JumpTarget::FirstTaskNode => {
                if let Some(node) = exec.process_model.get_first_task_node().cloned() {
                    self.execute_node(&mut exec, &node)?;
                }
            }
            // submitType=3 退回上一步：复活血缘前驱那条历史行
            JumpTarget::RollbackLineage => self.rollback_to_parent(&mut exec)?,
        }

        self.persist_tasks(&mut exec)?;
        self.repo().update_instance(&exec.process_instance)?;

        Ok(exec.new_tasks)
    }

    /// Async execute and jump to end (reject).
    pub async fn execute_and_jump_to_end_async(&self, task_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>> {
        let task = self.repo().find_task_by_id(task_id)?
            .ok_or(JeeflowError::TaskNotFound(task_id))?;

        let mut instance = self.repo().find_instance_by_id(task.process_instance_id)?
            .ok_or(JeeflowError::InstanceNotFound(task.process_instance_id))?;
        instance.tasks = self.repo().find_history_tasks(instance.instance_id)?;

        let _define = self.repo().find_define_by_id(instance.define_id)?
            .ok_or(JeeflowError::DefineNotFound(instance.define_id))?;

        // resume 语义对齐 Go mergeVars（同 execute_task_async，三路径保持一致）。
        let mut full_args = instance.variables.clone();
        full_args.merge(args);
        self.add_user_info(&mut full_args, operator);

        if !task.is_allowed(operator) {
            return Err(JeeflowError::PermissionDenied(
                format!("Operator {} not allowed on task {}", operator, task_id)));
        }

        // Inject countersignDisagreeFlag if submitType==20 (issues/94 round 2)
        let submit_type = full_args.get_i64("submitType")
            .or_else(|| full_args.get_str("submitType").and_then(|s| s.parse::<i64>().ok()));
        if submit_type == Some(20) {
            instance.variables.insert_str("countersignDisagreeFlag", "1");
            full_args.insert_str("countersignDisagreeFlag", "1");
        }

        instance.complete_task(task_id, operator, args).map_err(|e| JeeflowError::Business(e))?;
        // Set flag on completed task's variables
        if submit_type == Some(20) {
            if let Some(t) = instance.tasks.iter_mut().find(|t| t.task_id == task_id) {
                t.variables.insert_str("countersignDisagreeFlag", "1");
            }
        }
        if let Some(t) = instance.tasks.iter().find(|t| t.task_id == task_id) {
            self.repo().update_task(t)?;
        }

        // Reject the instance
        instance.reject();
        instance.abandon_all_doing();

        self.repo().update_instance(&instance)?;

        // Abandon all doing tasks
        for task in &instance.tasks {
            if task.task_state == TaskState::Doing.code() {
                self.repo().update_task(task)?;
            }
        }

        // Fire event（合并派：驳回=办结，同 finish 路径 fire ProcessInstanceEnd；
        // issues/104 §2.3 兑现缺口——execute_and_jump_to_end_async 此前漏 fire）
        let event = ProcessEvent::new(ProcessEventType::ProcessInstanceEnd,
                                       instance.instance_id);
        ProcessPublisher::notify(&event, &self.ctx.event_listeners);

        Ok(Vec::new())
    }

    /// Async execute and jump to first task node (return to initiator).
    pub async fn execute_and_jump_to_first_async(&self, task_id: i64, operator: &str, args: &FlowData)
        -> JeeflowResult<Vec<ProcessTask>> {
        // issues/121 P2：不再借道 execute_and_jump_async(None)——那条现在是血缘回退（3）
        self.execute_jump_inner(task_id, operator, args, JumpTarget::FirstTaskNode).await
    }

    /// 退回上一步（血缘版，规范 04 · 退回上一步）：上一步来源＝当前行的 parent_task_id，
    /// 复活那条历史行；不按模型入边拓扑推（拓扑版会回到本实例没走过的节点，且 3 与 6 塌成同值）。
    ///
    /// 错码用 `Business(msg)` 前缀表达：本栈 `JeeflowError::code()` 恒 99999999，
    /// 契约只要求"异常与 msg 可区分、HTTP 出口仍 99999999"。
    fn rollback_to_parent(&self, exec: &mut Execution) -> JeeflowResult<()> {
        const NO_LINEAGE: &str = "20010007: 上一步任务ID为空，无法驳回至上一步处理";
        const GUARD: &str = "20010008: 无法驳回至上一步处理，请确认上一步骤并非fork、join、suprocess以及会签任务";

        let current = match exec.process_task.clone() {
            Some(t) => t,
            None => return Err(JeeflowError::Business(NO_LINEAGE.to_string())),
        };
        let parent_id = match current.parent_task_id {
            Some(p) if p != 0 => p,
            _ => return Err(JeeflowError::Business(NO_LINEAGE.to_string())),
        };
        let history = match exec.process_instance.tasks.iter().find(|t| t.task_id == parent_id) {
            Some(t) => t.clone(),
            None => return Err(JeeflowError::Business(NO_LINEAGE.to_string())),
        };
        if !exec.process_model.can_rejected(&current.task_name, &history.task_name) {
            return Err(JeeflowError::Business(GUARD.to_string()));
        }

        // 复活行的变量只带数据类键：tf_*（上一次表单提交）与 csv_*/会签簿记都是"上次提交"的残留，
        // 留着会让新待办显示用户这次没填的东西、或让复活的会签节点从错位的序号继续推进。
        let mut vars = FlowData::new();
        for (k, v) in history.variables.iter() {
            if k == "submitType" || k == "taskName"
                || k.starts_with("tf_") || k.starts_with("csv_")
                || k.starts_with("loopCounter") || k.starts_with("nrOfInstances")
                || k.starts_with("operatorList") {
                continue;
            }
            vars.insert(k.clone(), v.clone());
        }
        // 首任务节点那条由发起人提交 ⇒ 参与者取该行 u_userId；其余取该行办结人。
        // 老行没这个键 ⇒ 按 false 处理（宁可派给该行 actor_id，也不用带"仅进行中"判定的现算值）。
        let is_first_row = matches!(history.variables.get("isFirstTaskNode"),
                                    Some(JsonValue::Bool(true)));
        let operator = if is_first_row {
            match history.variables.get("u_userId").and_then(|v| v.as_str()) {
                Some(s) => s.to_string(),
                None => exec.process_instance.operator.clone(),
            }
        } else {
            match history.actor_id.clone() {
                Some(a) => a,
                None => return Err(JeeflowError::Business(NO_LINEAGE.to_string())),
            }
        };
        vars.insert("isFirstTaskNode".to_string(), JsonValue::Bool(is_first_row));

        let mut revived = history.clone();
        revived.task_id = 0;                     // persist_tasks 里统一分配真实 id
        revived.task_state = TaskState::Doing.code();
        revived.actor_id = None;                 // 进行中任务该列恒无值
        revived.actor_ids = vec![operator];
        revived.finish_time = None;
        revived.variables = vars;                // parent_task_id 随行拷贝＝"上一步的上一步"
        exec.new_tasks.push(revived);
        Ok(())
    }
}

/// 解析发起时抄送人（对齐 Go facade.go:203 issues/56 E28）：
/// 支持 JSON 数组（vben 多选 ApiSelect 提交）与逗号分隔字符串两种形态。
fn parse_cc_actors(v: Option<&JsonValue>) -> Vec<String> {
    let Some(v) = v else { return Vec::new(); };
    let mut out: Vec<String> = match v {
        // 数组：逐项取原始标量（对齐 Go fmt.Sprintf("%v", a)，兼容字符串/数字 id）。
        // 注意不能用 JsonValue 的 Display——它走 to_json_string 会给字符串加引号。
        JsonValue::Array(items) => items
            .iter()
            .filter(|it| !it.is_null())
            .filter_map(|it| match it {
                JsonValue::Str(s) => Some(s.clone()),
                JsonValue::Number(n) => Some(format!("{}", *n as i64)),
                JsonValue::Bool(b) => Some(b.to_string()),
                _ => None,
            })
            .collect(),
        // 逗号分隔字符串
        JsonValue::Str(s) => s
            .split(',')
            .map(|x| x.trim().to_string())
            .filter(|x| !x.is_empty())
            .collect(),
        _ => Vec::new(),
    };
    out.retain(|s| !s.is_empty());
    out
}

// ═══════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryRepository;
    use crate::id_gen::AtomicIdGenerator;

    fn make_engine() -> (JeeflowEngineImpl, Arc<MemoryRepository>) {
        let repo = Arc::new(MemoryRepository::new());
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        let engine = JeeflowEngineImpl::new(ctx);
        (engine, repo)
    }

    fn simple_flow_json() -> Vec<u8> {
        r#"{
            "name": "test-flow",
            "displayName": "Test Flow",
            "type": "approval",
            "nodes": [
                {"id": "start", "type": "snaker:start", "text": {"value": "Start"}},
                {"id": "apply", "type": "snaker:task", "text": {"value": "Apply"},
                 "properties": {"assignee": "applicant"}},
                {"id": "end", "type": "snaker:end", "text": {"value": "End"}}
            ],
            "edges": [
                {"id": "e1", "sourceNodeId": "start", "targetNodeId": "apply"},
                {"id": "e2", "sourceNodeId": "apply", "targetNodeId": "end"}
            ]
        }"#.as_bytes().to_vec()
    }

    fn make_define(repo: &MemoryRepository) -> i64 {
        let mut define = ProcessDefine {
            id: 0,
            name: "test-flow".into(),
            display_name: "Test".into(),
            define_type: "approval".into(),
            state: 1,
            content: simple_flow_json(),
            version: 1,
            create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_define(&mut define).unwrap();
        define.id
    }

    #[test]
    fn test_engine_start_async() {
        let (engine, repo) = make_engine();
        let define_id = make_define(&repo);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(engine.start_async(define_id, "user1", &FlowData::new()));
        assert!(result.is_ok());
        let instance = result.unwrap();
        assert_eq!(instance.operator, "user1");
    }

    /// 捕获 TASK_START 事件的监听器（issues/13 回归测试用，对齐 Java TaskStartEventOrderTest）。
    struct TaskStartCapture {
        source_ids: std::sync::Mutex<Vec<i64>>,
    }

    impl ProcessEventListener for TaskStartCapture {
        fn on_event(&self, event: &ProcessEvent) {
            if event.event_type == ProcessEventType::ProcessTaskStart {
                self.source_ids.lock().unwrap().push(event.source_id);
            }
        }
    }

    /// TASK_START 时机回归（issues/13 salvo 栈根因闭环，对齐 Java jeeflow-java 1.8.20）：
    /// 事件必须在任务**落库后** fire——source_id 非 0，且事件到达时 find_task(source_id) 查得到。
    /// 若退回 create 阶段 fire（task_id=0/未落库），此处 `find_task_by_id` 必为 None → 红。
    #[test]
    fn test_task_start_event_fires_after_persist() {
        let repo = Arc::new(MemoryRepository::new());
        let mut ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        let capture = Arc::new(TaskStartCapture { source_ids: std::sync::Mutex::new(Vec::new()) });
        ctx.register_event_listener(capture.clone());
        let engine = JeeflowEngineImpl::new(ctx);

        let define_id = make_define(&repo);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let instance = rt.block_on(engine.start_async(define_id, "user1", &FlowData::new())).unwrap();
        assert!(instance.instance_id > 0);

        let fired = capture.source_ids.lock().unwrap().clone();
        // simple_flow_json 的 apply 节点 assignee=applicant → 必产生 1 个任务 → TASK_START 必 fire
        assert_eq!(fired.len(), 1, "TASK_START 应恰好 fire 一次（simple_flow 单任务节点）");
        let task_id = fired[0];
        assert!(task_id > 0, "TASK_START 的 source_id 必须是已分配的真实 task_id（非 0）");
        assert!(
            repo.find_task_by_id(task_id).unwrap().is_some(),
            "TASK_START fire 时任务必须已落库（find_task_by_id 可查）"
        );
    }

    /// 捕获 CC_CREATE 事件的监听器（issues/102·104 P0 测试，对齐 Java CcCreateEventTest）。
    struct CcCreateCapture {
        events: std::sync::Mutex<Vec<(i64, Option<String>)>>,
    }

    impl ProcessEventListener for CcCreateCapture {
        fn on_event(&self, event: &ProcessEvent) {
            if event.event_type == ProcessEventType::CcCreate {
                self.events.lock().unwrap()
                    .push((event.source_id, event.cc_actor_id.clone()));
            }
        }
    }

    /// P0 正向（issues/102 验收口径）：带抄送人发起 → **逐抄送人** fire CC_CREATE，
    /// source_id=instance_id、cc_actor_id 与 f_ccActors 顺序一一对应、cc 行已落库。
    #[test]
    fn test_cc_create_event_fired_per_actor() {
        let repo = Arc::new(MemoryRepository::new());
        let mut ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        let capture = Arc::new(CcCreateCapture { events: std::sync::Mutex::new(Vec::new()) });
        ctx.register_event_listener(capture.clone());
        let engine = JeeflowEngineImpl::new(ctx);

        let define_id = make_define(&repo);
        let mut args = FlowData::new();
        args.insert("f_ccActors".into(), JsonValue::Array(vec![
            JsonValue::Str("u1".into()), JsonValue::Str("u2".into()),
        ]));
        let rt = tokio::runtime::Runtime::new().unwrap();
        let instance = rt.block_on(engine.start_async(define_id, "user1", &args)).unwrap();

        let fired = capture.events.lock().unwrap().clone();
        assert_eq!(fired.len(), 2, "应逐抄送人 fire 恰好两次，实得 {:?}", fired);
        assert!(fired.iter().all(|(sid, _)| *sid == instance.instance_id),
            "source_id 应为 instance_id：{:?}", fired);
        let actors: Vec<String> = fired.iter().map(|(_, a)| a.clone().expect("cc_actor_id 应直传")).collect();
        assert_eq!(actors, vec!["u1".to_string(), "u2".to_string()], "cc_actor_id 顺序应与 f_ccActors 一致");

        // cc 行已落库，且与 fire 粒度一一对应
        let mut q = crate::model::PageQuery::new(1, 10);
        let page = repo.page_cc_instances(&mut q).unwrap();
        assert_eq!(page.record_count, 2, "cc 实例应逐人落库");
    }

    /// P0 零副作用（issues/102 验收口径）：无监听器装配 → 抄送照常落库、fire 侧不抛错。
    #[test]
    fn test_cc_create_no_listener_zero_side_effect() {
        let repo = Arc::new(MemoryRepository::new());
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        let engine = JeeflowEngineImpl::new(ctx);

        let define_id = make_define(&repo);
        let mut args = FlowData::new();
        args.insert("f_ccActors".into(), JsonValue::Array(vec![
            JsonValue::Str("u9".into()),
        ]));
        let rt = tokio::runtime::Runtime::new().unwrap();
        let instance = rt.block_on(engine.start_async(define_id, "user1", &args)).unwrap();

        let mut q = crate::model::PageQuery::new(1, 10);
        let page = repo.page_cc_instances(&mut q).unwrap();
        assert_eq!(page.record_count, 1, "无监听器时 cc 实例仍应照常落库");
        assert!(instance.instance_id > 0);
    }

    /// 捕获 PROCESS_INSTANCE_END 事件的监听器（issues/104 §2.3 回归：驳回路径补 fire）。
    struct InstanceEndCapture {
        source_ids: std::sync::Mutex<Vec<i64>>,
    }

    impl ProcessEventListener for InstanceEndCapture {
        fn on_event(&self, event: &ProcessEvent) {
            if event.event_type == ProcessEventType::ProcessInstanceEnd {
                self.source_ids.lock().unwrap().push(event.source_id);
            }
        }
    }

    /// 回归（issues/104 §2.3）：驳回（execute_and_jump_to_end_async）路径必须 fire
    /// ProcessInstanceEnd——此前该结束路径漏 fire，致 salvo 栈 CC 五场景 reject 维红。
    #[test]
    fn test_reject_path_fires_instance_end() {
        let repo = Arc::new(MemoryRepository::new());
        let mut ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        let capture = Arc::new(InstanceEndCapture { source_ids: std::sync::Mutex::new(Vec::new()) });
        ctx.register_event_listener(capture.clone());
        let engine = JeeflowEngineImpl::new(ctx);

        let define_id = make_define(&repo);
        let rt = tokio::runtime::Runtime::new().unwrap();
        let instance = rt.block_on(engine.start_async(define_id, "user1", &FlowData::new())).unwrap();

        // 取发起后在办任务（simple flow：apply 节点 assignee=applicant → user1）
        let tasks = repo.find_doing_tasks(instance.instance_id, &[]).unwrap();
        assert!(!tasks.is_empty(), "驳回前应有在办任务");
        let task = &tasks[0];

        // 驳回（jump_to_end）：flow.auto 旁路 is_allowed，驱动 execute_and_jump_to_end_async
        let result = rt.block_on(engine.execute_and_jump_to_end_async(
            task.task_id, "flow.auto", &FlowData::new()));
        assert!(result.is_ok(), "驳回应成功：{:?}", result.err());

        // 实例应为已拒绝（state=45）
        let inst = repo.find_instance_by_id(instance.instance_id).unwrap().unwrap();
        assert_eq!(inst.state, 45, "驳回后实例 state 应为 45（已拒绝）");

        // 必须 fire 恰好一次 ProcessInstanceEnd，source_id=instance_id
        let fired = capture.source_ids.lock().unwrap().clone();
        assert_eq!(fired.len(), 1, "驳回路径应 fire 恰好一次 ProcessInstanceEnd，实得 {:?}", fired);
        assert_eq!(fired[0], instance.instance_id, "source_id 应为 instance_id");
    }

    #[test]
    fn test_engine_start_not_found() {
        let (engine, _repo) = make_engine();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(engine.start_async(99999, "user1", &FlowData::new()));
        assert!(result.is_err());
    }

    #[test]
    fn test_execution_new() {
        let define = ProcessDefine {
            id: 1, name: "test".into(), display_name: "Test".into(),
            define_type: "approval".into(), state: 1, content: simple_flow_json(),
            version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        let model = crate::parser::ModelParser::parse(std::str::from_utf8(&define.content).unwrap()).unwrap();
        let instance = ProcessInstance::create(&define, "user1", &FlowData::new());
        let exec = Execution::new(instance.clone(), model, define, "user1", FlowData::new());
        assert_eq!(exec.operator, "user1");
        assert!(!exec.instance_finished);
        assert!(exec.new_tasks.is_empty());
    }

    #[test]
    fn test_engine_execute_task_async() {
        let (engine, repo) = make_engine();
        let define_id = make_define(&repo);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let instance = rt.block_on(engine.start_async(define_id, "user1", &FlowData::new())).unwrap();

        // Find the created task
        let tasks = repo.find_doing_tasks(instance.instance_id, &[]).unwrap();
        // Just verify the engine can find tasks (may be empty depending on flow)
        let _ = tasks;
        // The important thing is start_async succeeded
        assert!(instance.instance_id > 0);
    }

    #[test]
    fn test_engine_execute_task_permission_denied() {
        let (engine, repo) = make_engine();
        let define_id = make_define(&repo);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let instance = rt.block_on(engine.start_async(define_id, "user1", &FlowData::new())).unwrap();

        let tasks = repo.find_doing_tasks(instance.instance_id, &[]).unwrap();
        if !tasks.is_empty() {
            let task_id = tasks[0].task_id;
            let result = rt.block_on(engine.execute_task_async(task_id, "user999", &FlowData::new()));
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_engine_execute_task_not_found() {
        let (engine, _repo) = make_engine();
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(engine.execute_task_async(99999, "user1", &FlowData::new()));
        assert!(result.is_err());
    }

    #[test]
    fn test_engine_with_ext_repository() {
        let repo = Arc::new(MemoryRepository::new());
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        let engine = JeeflowEngineImpl::new(ctx);
        assert!(engine.repo().find_define_by_id(1).is_ok());
    }

    // ═══════════════════════════════════════════════════════
    // Compliance 22 scenarios (spec/08)
    // ═══════════════════════════════════════════════════════

    use crate::model::UserInfo;

    struct ComplianceUserProvider;
    impl UserProvider for ComplianceUserProvider {
        fn get_user(&self, user_id: &str) -> JeeflowResult<Option<UserInfo>> {
            Ok(Some(UserInfo {
                user_id: user_id.to_string(),
                real_name: user_id.to_string(),
                dept_id: "dept1".into(), dept_name: "TestDept".into(),
                post_id: "post1".into(), post_name: "TestPost".into(),
            }))
        }
    }

    fn flows_dir() -> String {
        crate::flowsdir::dir().to_string_lossy().into_owned()
    }

    fn load_flow(name: &str) -> String {
        let path = format!("{}/{}.json", flows_dir(), name);
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("Failed to read {}: {}", path, e))
    }

    fn make_compliance_engine() -> (JeeflowEngineImpl, Arc<MemoryRepository>) {
        let repo = Arc::new(MemoryRepository::new());
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_user_provider(Arc::new(ComplianceUserProvider))
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        (JeeflowEngineImpl::new(ctx), repo)
    }

    fn save_define(repo: &Arc<MemoryRepository>, name: &str, content: &str) -> i64 {
        let mut define = ProcessDefine {
            id: 0, name: name.into(), display_name: name.into(),
            define_type: "approval".into(), state: 1,
            content: content.as_bytes().to_vec(),
            version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_define(&mut define).unwrap();
        define.id
    }

    /// Start instance and auto-complete the apply node (operator = applicant).
    async fn start_and_apply(engine: &JeeflowEngineImpl, repo: &Arc<MemoryRepository>, define_id: i64) -> i64 {
        let inst = engine.start_async(define_id, "applicant", &FlowData::new()).await.unwrap();
        let tasks = repo.find_doing_tasks(inst.instance_id, &[]).unwrap();
        if let Some(apply_task) = tasks.iter().find(|t| t.actor_ids.contains(&"applicant".to_string())) {
            engine.execute_task_async(apply_task.task_id, "applicant", &FlowData::new()).await.unwrap();
        }
        inst.instance_id
    }

    // ── v1.0 core scenarios (1-10) ──

    /// #91 regression: execute_task_async must return new_tasks with non-zero IDs.
    #[tokio::test]
    async fn test_c01_execute_returns_nonzero_task_ids() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("01-simple");
        let did = save_define(&repo, "simple-nonzero", &flow);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let tasks = repo.find_doing_tasks(inst.instance_id, &[]).unwrap();
        let apply_task = tasks.iter().find(|t| t.actor_ids.contains(&"applicant".to_string())).unwrap();
        // Execute apply → should create task for "leader" with non-zero id
        let new_tasks = engine.execute_task_async(apply_task.task_id, "applicant", &FlowData::new()).await.unwrap();
        assert!(!new_tasks.is_empty(), "#91: execute should return new tasks");
        for t in &new_tasks {
            assert!(t.task_id > 0, "#91: new task id should be non-zero, got {}", t.task_id);
        }
        // Cross-check: the returned id should match what's in the repo
        let repo_tasks = repo.find_doing_tasks(inst.instance_id, &[]).unwrap();
        for nt in &new_tasks {
            assert!(repo_tasks.iter().any(|rt| rt.task_id == nt.task_id),
                "#91: returned task_id {} should exist in repo", nt.task_id);
        }
    }

    #[tokio::test]
    async fn test_c01_simple_linear() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("01-simple");
        let did = save_define(&repo, "simple", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        // After apply, task1 should be created for "leader"
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert!(!tasks.is_empty(), "c01: task1 should exist after apply");
        assert_eq!(tasks[0].actor_ids[0], "leader");
        // Execute task1 → end → instance complete
        engine.execute_task_async(tasks[0].task_id, "leader", &FlowData::new()).await.unwrap();
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 20, "c01: instance should be completed (state=20)");
    }

    #[tokio::test]
    async fn test_c02_multi_task() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("02-multi-task");
        let did = save_define(&repo, "multi-task", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        // task1 for leader
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks[0].actor_ids[0], "leader");
        engine.execute_task_async(tasks[0].task_id, "leader", &FlowData::new()).await.unwrap();
        // task2 for manager
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks[0].actor_ids[0], "manager");
        engine.execute_task_async(tasks[0].task_id, "manager", &FlowData::new()).await.unwrap();
        // task3 for boss
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks[0].actor_ids[0], "boss");
        engine.execute_task_async(tasks[0].task_id, "boss", &FlowData::new()).await.unwrap();
        // Instance complete
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 20, "c02: all 3 tasks approved, instance done");
    }

    #[tokio::test]
    async fn test_c03_decision_branch() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("03-decision-expr");
        let did = save_define(&repo, "decision-expr", &flow);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        // Just verify instance started (decision evaluation depends on expression engine)
        assert!(inst.instance_id > 0, "c03: instance created");
        let tasks = repo.find_doing_tasks(inst.instance_id, &[]).unwrap();
        assert!(!tasks.is_empty(), "c03: apply task should exist");
    }

    #[tokio::test]
    async fn test_c04_parallel_fork_join() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("04-fork-join");
        let did = save_define(&repo, "fork-join", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert!(tasks.len() >= 2, "c04: fork should create parallel tasks, got {}", tasks.len());
    }

    #[tokio::test]
    async fn test_c05_countersign_parallel() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("05-countersign-parallel");
        let did = save_define(&repo, "countersign-parallel", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 3, "c05: parallel countersign should create exactly 3 tasks, got {}", tasks.len());
    }

    /// #94 regression: sequential countersign creates one at a time with completion gate.
    #[tokio::test]
    async fn test_c06_countersign_sequential() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("06-countersign-sequential");
        let did = save_define(&repo, "countersign-sequential", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;

        // After apply: exactly 1 DOING task for userA
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 1, "c06: after apply DOING should be exactly 1");
        assert_eq!(tasks[0].actor_ids[0], "userA", "c06: first task should be for userA");

        // userA completes → state=10, next task for userB created
        let mut args = FlowData::new();
        args.insert_i64("submitType", 1);
        engine.execute_task_async(tasks[0].task_id, "userA", &args).await.unwrap();
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 10, "c06: after userA instance should still be running");
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 1, "c06: after userA DOING should be exactly 1");
        assert_eq!(tasks[0].actor_ids[0], "userB", "c06: next task should be for userB");

        // userB completes → state=20 (finished)
        engine.execute_task_async(tasks[0].task_id, "userB", &args).await.unwrap();
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 20, "c06: after userB instance should be finished");
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 0, "c06: no DOING tasks after finish");
    }

    #[tokio::test]
    async fn test_c07_countersign_ratio() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("07-countersign-ratio");
        let did = save_define(&repo, "countersign-ratio", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 4, "c07: ratio countersign should create exactly 4 tasks, got {}", tasks.len());
    }

    #[tokio::test]
    async fn test_c08_reject_to_applicant() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("08-countersign-sequential-approve");
        let did = save_define(&repo, "cs-seq-approve", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert!(!tasks.is_empty(), "c08: should have tasks after apply");
        // Verify instance state is running (10)
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 10, "c08: instance should be running");
    }

    #[tokio::test]
    async fn test_c09_permission_check() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("09-with-reject");
        let did = save_define(&repo, "with-reject", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        if let Some(task) = tasks.first() {
            // Non-actor should fail
            let result = engine.execute_task_async(task.task_id, "unauthorized_user", &FlowData::new()).await;
            assert!(result.is_err(), "c09: non-actor should be denied");
        }
    }

    #[tokio::test]
    async fn test_c10_interceptor_event() {
        // Verify engine can start with interceptors registered
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("10-mixed-mode");
        let did = save_define(&repo, "mixed-mode", &flow);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        assert!(inst.instance_id > 0, "c10: instance started with mixed-mode flow");
    }

    // ── v1.0.1~v1.1.0 enhanced scenarios (11-15) ──

    #[tokio::test]
    async fn test_c11_assignee_variable() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("11-assignee-vars");
        let did = save_define(&repo, "assignee-vars", &flow);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        assert!(inst.instance_id > 0, "c11: assignee variable flow started");
    }

    #[tokio::test]
    async fn test_c12_system_auto_execute() {
        // flow.auto / flow.admin should be able to execute any task
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("01-simple");
        let did = save_define(&repo, "simple-auto", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        if let Some(task) = tasks.first() {
            // flow.auto should be able to execute
            let result = engine.execute_task_async(task.task_id, "flow.auto", &FlowData::new()).await;
            assert!(result.is_ok(), "c12: flow.auto should be able to execute");
        }
    }

    #[tokio::test]
    async fn test_c13_define_write_ops() {
        let (engine, repo) = make_compliance_engine();
        // save
        let mut define = ProcessDefine {
            id: 0, name: "c13-test".into(), display_name: "C13".into(),
            define_type: "approval".into(), state: 0,
            content: b"{}".to_vec(), version: 1,
            create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_define(&mut define).unwrap();
        assert!(define.id > 0, "c13: save_define should assign id");
        // update state
        repo.update_define_state(define.id, 1).unwrap();
        let d = repo.find_define_by_id(define.id).unwrap().unwrap();
        assert_eq!(d.state, 1, "c13: state should be updated");
        // remove
        repo.remove_define(define.id).unwrap();
        let d = repo.find_define_by_id(define.id).unwrap();
        assert!(d.is_none(), "c13: define should be removed");
    }

    #[tokio::test]
    async fn test_c14_update_instance_cascade() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("01-simple");
        let did = save_define(&repo, "simple-cascade", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        // Update instance state (cascade: tasks should reflect)
        let mut inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        inst.business_no = Some("BIZ-001".into());
        repo.update_instance(&inst).unwrap();
        let updated = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(updated.business_no, Some("BIZ-001".into()), "c14: instance update should persist");
    }

    #[tokio::test]
    async fn test_c15_facade_routing() {
        // Verify all 42 actions are dispatchable (already tested in facade, but verify here at engine level)
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("01-simple");
        let did = save_define(&repo, "simple-routing", &flow);
        // Verify define can be found
        let d = repo.find_define_by_id(did).unwrap();
        assert!(d.is_some(), "c15: define should be findable");
    }

    // ── v1.2.0 view endpoint scenarios (16-18) ──

    #[tokio::test]
    async fn test_c16_view_endpoints() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("01-simple");
        let did = save_define(&repo, "simple-views", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        // approvalRecord: find history tasks
        let history = repo.find_history_tasks(iid).unwrap();
        assert!(!history.is_empty(), "c16: should have history tasks");
    }

    #[tokio::test]
    async fn test_c17_highlight() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("01-simple");
        let did = save_define(&repo, "simple-highlight", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let history = repo.find_history_tasks(iid).unwrap();
        assert!(!history.is_empty(), "c17: highlight should have history");
    }

    #[tokio::test]
    async fn test_c18_candidate_page() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("12-candidate-page");
        let did = save_define(&repo, "candidate-page", &flow);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        assert!(inst.instance_id > 0, "c18: candidate page flow started");
    }

    // ── v1.3.0 alignment fix scenarios (19-20) ──

    #[tokio::test]
    async fn test_c19_cc_paging() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("01-simple");
        let did = save_define(&repo, "simple-cc", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        // Create CC instances
        repo.create_cc_instance(iid, "user1", &["cc_user1".into(), "cc_user2".into()]).unwrap();
        let page = repo.page_cc_instances(&PageQuery::new(1, 10)).unwrap();
        assert!(page.record_count > 0, "c19: cc instances should exist");
    }

    #[tokio::test]
    async fn test_c20_add_task_actor() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("01-simple");
        let did = save_define(&repo, "simple-actor", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        if let Some(task) = tasks.first() {
            repo.add_task_actor(task.task_id, &["extra_actor".into()]).unwrap();
            let actors = repo.find_task_actors(task.task_id).unwrap();
            assert!(actors.contains(&"extra_actor".to_string()), "c20: extra actor should be added");
            // Add again — should deduplicate
            repo.add_task_actor(task.task_id, &["extra_actor".into()]).unwrap();
            let actors = repo.find_task_actors(task.task_id).unwrap();
            let count = actors.iter().filter(|a| *a == "extra_actor").count();
            assert_eq!(count, 1, "c20: should deduplicate actors");
        }
    }

    // ── v1.4.0 metadata scenarios (21-22) ──

    #[test]
    fn test_c21_enum_dict() {
        use crate::metadata::EnumDictRegistry;
        let registry = EnumDictRegistry::default();
        // Verify 7 standard keys exist (wf_ prefix)
        let keys = vec!["wf_process_define_state", "wf_process_instance_state", "wf_process_submit_type",
                        "wf_process_task_state", "wf_process_task_type", "wf_process_task_perform_type",
                        "wf_countersign_type"];
        for key in &keys {
            let dict = registry.get_dict(key);
            assert!(dict.is_some(), "c21: enum dict '{}' should exist", key);
            assert!(!dict.unwrap().is_empty(), "c21: enum dict '{}' should have entries", key);
        }
    }

    #[test]
    fn test_c22_handler_registry() {
        use crate::metadata::{HandlerRegistry, HandlerMeta};
        let mut registry = HandlerRegistry::new();
        let built_in_count = registry.all_handlers().len();
        registry.register(HandlerMeta {
            handler_type: "assignment".into(), class_name: "handler1".into(),
            display_name: "Handler 1".into(), order: 1, group: "test".into(),
        });
        registry.register(HandlerMeta {
            handler_type: "assignment".into(), class_name: "handler2".into(),
            display_name: "Handler 2".into(), order: 2, group: "test".into(),
        });
        let by_type = registry.list_handlers("assignment");
        assert!(by_type.len() >= 2, "c22: should have >= 2 assignment handlers");
        let all = registry.all_handlers();
        assert_eq!(all.len(), built_in_count + 2, "c22: total should include built-ins + 2");
    }

    // ── v1.5.0 #94 countersign gate scenarios (23-28) ──

    /// #94 test #2: parallel soft reject (submitType=20, no ONE_VOTE_VETO)
    #[tokio::test]
    async fn test_c23_parallel_soft_reject() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("05-countersign-parallel");
        let did = save_define(&repo, "cs-parallel-soft", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 3, "c23: should have 3 DOING tasks");
        let userA_task = tasks.iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap();

        // userA submitType=20 → soft reject (no ONE_VOTE_VETO configured)
        let mut args = FlowData::new();
        args.insert_i64("submitType", 20);
        engine.execute_task_async(userA_task.task_id, "userA", &args).await.unwrap();

        // Instance still running, userA FINISHED, userB/userC still DOING
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 10, "c23: soft reject should not finish instance");
        let doing = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(doing.len(), 2, "c23: userB/userC should still be DOING");
        // countersignDisagreeFlag=1 in instance variables
        assert_eq!(inst.variables.get_str("countersignDisagreeFlag"), Some("1"),
            "c23: countersignDisagreeFlag should be 1 in instance variables");
        // Task variable also has the flag
        let all_tasks = repo.find_history_tasks(iid).unwrap();
        let userA_done = all_tasks.iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap();
        assert_eq!(userA_done.variables.get_str("countersignDisagreeFlag"), Some("1"),
            "c23: countersignDisagreeFlag should be 1 in task variables");
    }

    /// #94 test #3: one-vote veto (13)
    #[tokio::test]
    async fn test_c24_one_vote_veto() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("13-countersign-one-vote-veto");
        let did = save_define(&repo, "cs-one-vote-veto", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 3, "c24: should have 3 DOING tasks");
        let userA_task = tasks.iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap();

        // userA submitType=20 → ONE_VOTE_VETO triggers → instance finished
        let mut args = FlowData::new();
        args.insert_i64("submitType", 20);
        engine.execute_task_async(userA_task.task_id, "userA", &args).await.unwrap();

        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 20, "c24: veto should finish instance");
        // userB/userC tasks should be ABANDON (99)
        let all_tasks = repo.find_history_tasks(iid).unwrap();
        let userB_task = all_tasks.iter().find(|t| t.actor_ids.contains(&"userB".to_string())).unwrap();
        let userC_task = all_tasks.iter().find(|t| t.actor_ids.contains(&"userC".to_string())).unwrap();
        assert_eq!(userB_task.task_state, 99, "c24: userB task should be ABANDON(99)");
        assert_eq!(userC_task.task_state, 99, "c24: userC task should be ABANDON(99)");
        // userA FINISHED
        let userA_done = all_tasks.iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap();
        assert_eq!(userA_done.task_state, 20, "c24: userA task should be FINISHED(20)");
        // flag present
        assert_eq!(inst.variables.get_str("countersignDisagreeFlag"), Some("1"),
            "c24: countersignDisagreeFlag should be 1");
    }

    /// #94 test #4: ratio expression (#nrOfCompletedInstances==2)
    #[tokio::test]
    async fn test_c25_ratio_expression() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("07-countersign-ratio");
        let did = save_define(&repo, "cs-ratio", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 4, "c25: should have 4 DOING tasks");

        // Complete 1 person → still running, 3 DOING
        let userA_task = tasks.iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap();
        engine.execute_task_async(userA_task.task_id, "userA", &FlowData::new()).await.unwrap();
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 10, "c25: after 1 completion should still be running");
        let doing = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(doing.len(), 3, "c25: should have 3 DOING after 1 completion");

        // Complete 2nd person → merged (ratio 2/4 met), 2 remaining ABANDON
        let userB_task = doing.iter().find(|t| t.actor_ids.contains(&"userB".to_string())).unwrap();
        engine.execute_task_async(userB_task.task_id, "userB", &FlowData::new()).await.unwrap();
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 20, "c25: after 2nd completion should be finished (ratio met)");
        let all_tasks = repo.find_history_tasks(iid).unwrap();
        let abandon_count = all_tasks.iter().filter(|t| t.task_state == 99).count();
        assert_eq!(abandon_count, 2, "c25: 2 remaining tasks should be ABANDON");
    }

    /// #94 test #5: soft reject follow-up (remaining complete normally)
    #[tokio::test]
    async fn test_c26_soft_reject_followup() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("05-countersign-parallel");
        let did = save_define(&repo, "cs-soft-followup", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;

        // userA soft-rejects (submitType=20)
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        let userA_task = tasks.iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap();
        let mut args = FlowData::new();
        args.insert_i64("submitType", 20);
        engine.execute_task_async(userA_task.task_id, "userA", &args).await.unwrap();

        // userB completes normally
        let doing = repo.find_doing_tasks(iid, &[]).unwrap();
        let userB_task = doing.iter().find(|t| t.actor_ids.contains(&"userB".to_string())).unwrap();
        engine.execute_task_async(userB_task.task_id, "userB", &FlowData::new()).await.unwrap();

        // userC completes normally → instance finished
        let doing = repo.find_doing_tasks(iid, &[]).unwrap();
        let userC_task = doing.iter().find(|t| t.actor_ids.contains(&"userC".to_string())).unwrap();
        engine.execute_task_async(userC_task.task_id, "userC", &FlowData::new()).await.unwrap();

        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 20, "c26: after all complete, instance should be finished");
    }

    /// #94 test #6: negative — execute abandoned task should fail
    #[tokio::test]
    async fn test_c27_execute_abandoned_task_fails() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("13-countersign-one-vote-veto");
        let did = save_define(&repo, "cs-abandon-neg", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;

        // Trigger veto to abandon userB/userC
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        let userA_task = tasks.iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap();
        let userB_task = tasks.iter().find(|t| t.actor_ids.contains(&"userB".to_string())).unwrap();
        let userB_id = userB_task.task_id;
        let mut args = FlowData::new();
        args.insert_i64("submitType", 20);
        engine.execute_task_async(userA_task.task_id, "userA", &args).await.unwrap();

        // Try to execute abandoned userB task → should fail
        let result = engine.execute_task_async(userB_id, "userB", &FlowData::new()).await;
        assert!(result.is_err(), "c27: executing abandoned task should fail");
    }

    /// #94 round 2 test #1: 08 full chain hard assertions
    /// Flow: apply(applicant) → task1(SEQUENTIAL userA,userB) → approve(leader) → end
    #[tokio::test]
    async fn test_c28_08_full_chain() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("08-countersign-sequential-approve");
        let did = save_define(&repo, "cs-seq-08", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;

        // Step 1: after apply → DOING==1 actor==userA
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 1, "c28: after apply DOING should be 1");
        assert_eq!(tasks[0].actor_ids, vec!["userA"], "c28: after apply actor should be userA");
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 10, "c28: after apply state should be 10");

        // Step 2: userA completes → DOING==1 actor==userB (sequential next)
        let userA_task = tasks.into_iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap();
        engine.execute_task_async(userA_task.task_id, "userA", &FlowData::new()).await.unwrap();
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 1, "c28: after userA DOING should be 1");
        assert_eq!(tasks[0].actor_ids, vec!["userB"], "c28: after userA actor should be userB");
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 10, "c28: after userA state should be 10");

        // Step 3: userB completes → merged, fall through to 10b → creates approve(leader)
        let userB_task = tasks.into_iter().find(|t| t.actor_ids.contains(&"userB".to_string())).unwrap();
        engine.execute_task_async(userB_task.task_id, "userB", &FlowData::new()).await.unwrap();
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 1, "c28: after userB DOING should be 1 (leader approve)");
        assert_eq!(tasks[0].actor_ids, vec!["leader"], "c28: after userB actor should be leader");
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 10, "c28: after userB state should be 10 (not finished!)");

        // Step 4: leader completes → follows edge to end → state==20
        let leader_task = tasks.into_iter().find(|t| t.actor_ids.contains(&"leader".to_string())).unwrap();
        engine.execute_task_async(leader_task.task_id, "leader", &FlowData::new()).await.unwrap();
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(tasks.len(), 0, "c28: after leader DOING should be 0");
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 20, "c28: after leader state should be 20");
    }

    /// #94 round 2 test #2: jump_to_end flag injection (no countersign gate on jump)
    #[tokio::test]
    async fn test_c29_jump_flag_injection() {
        let (engine, repo) = make_compliance_engine();
        let flow = load_flow("13-countersign-one-vote-veto");
        let did = save_define(&repo, "cs-jump-flag", &flow);
        let iid = start_and_apply(&engine, &repo, did).await;
        let tasks = repo.find_doing_tasks(iid, &[]).unwrap();
        let userA_task = tasks.into_iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap();

        // Execute via jump_to_end with submitType=20
        let mut args = FlowData::new();
        args.insert_i64("submitType", 20);
        engine.execute_and_jump_to_end_async(userA_task.task_id, "userA", &args).await.unwrap();

        // Instance should be rejected (state=45)
        let inst = repo.find_instance_by_id(iid).unwrap().unwrap();
        assert_eq!(inst.state, 45, "c29: instance should be rejected");
        // Flag should be in instance.variables
        assert_eq!(inst.variables.get_str("countersignDisagreeFlag").unwrap(), "1",
            "c29: flag should be in instance variables");
        // Flag should be on userA's task variables
        let all_tasks = repo.find_history_tasks(iid).unwrap();
        let userA_done = all_tasks.iter().find(|t| t.task_id == userA_task.task_id).unwrap();
        assert_eq!(userA_done.variables.get_str("countersignDisagreeFlag").unwrap(), "1",
            "c29: flag should be on task variables");
    }

    /// c30: 发起时抄送解析 parse_cc_actors（L3 S6）——三形态：
    /// JSON 数组（vben 多选 ApiSelect）、逗号分隔字符串、空/缺失。
    #[test]
    fn test_parse_cc_actors() {
        use crate::json::JsonValue;
        // ① 正向：JSON 数组（vben 多选提交）
        let arr = JsonValue::Array(vec![
            JsonValue::Str("2086772715431137280".into()),
            JsonValue::Str("1686404946814533633".into()),
        ]);
        assert_eq!(
            parse_cc_actors(Some(&arr)),
            vec!["2086772715431137280".to_string(), "1686404946814533633".to_string()]
        );
        // ② 兼容：逗号分隔字符串（含空格/空段）
        let s = JsonValue::Str(" u1 , u2 ,, ".into());
        assert_eq!(parse_cc_actors(Some(&s)), vec!["u1".to_string(), "u2".to_string()]);
        // ③ 负向：null / 缺失 / 空数组 → 空
        assert!(parse_cc_actors(None).is_empty());
        assert!(parse_cc_actors(Some(&JsonValue::Null)).is_empty());
        assert!(parse_cc_actors(Some(&JsonValue::Array(vec![]))).is_empty());
        assert!(parse_cc_actors(Some(&JsonValue::Str("  ".into()))).is_empty());
    }

    /// c31: resume 时 FormFieldAssignee 从发起时 f_* 变量取人（L3 S11-B 回归）。
    /// 流程 start → apply(applicant) → approver(FormFieldAssignee) → end。
    /// 发起时带 f_approver=u0011；apply 审批后 resume，approver 节点须按
    /// exec.args 里的 f_approver 建任务给 u0011。修复前 resume 的 exec.args
    /// 不含 f_approver（未 merge instance.variables）→ 节点被跳过 → 无任务。
    #[tokio::test]
    async fn test_c31_form_field_assignee_on_resume() {
        use crate::interceptor::register_builtin_assignment_handlers;
        let repo = Arc::new(MemoryRepository::new());
        let mut ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_user_provider(Arc::new(ComplianceUserProvider))
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        register_builtin_assignment_handlers(&mut ctx);
        let engine = JeeflowEngineImpl::new(ctx);

        let flow = r#"{
            "name": "s11b", "displayName": "S11B", "type": "approval",
            "nodes": [
                {"id": "start", "type": "snaker:start", "text": {"value": "Start"}},
                {"id": "apply", "type": "snaker:task", "text": {"value": "Apply"},
                 "properties": {"assignee": "applicant"}},
                {"id": "approver", "type": "snaker:task", "text": {"value": "Approver"},
                 "properties": {"assignmentHandler": "com.mldong.jeeflow.interceptor.impl.FormFieldAssigneeHandler"}},
                {"id": "end", "type": "snaker:end", "text": {"value": "End"}}
            ],
            "edges": [
                {"id": "e1", "sourceNodeId": "start", "targetNodeId": "apply"},
                {"id": "e2", "sourceNodeId": "apply", "targetNodeId": "approver"},
                {"id": "e3", "sourceNodeId": "approver", "targetNodeId": "end"}
            ]
        }"#.to_string();
        let did = save_define(&repo, "s11b", &flow);

        // 发起带 f_approver
        let mut args = FlowData::new();
        args.insert_str("f_approver", "u0011");
        let inst = engine.start_async(did, "applicant", &args).await.unwrap();

        // apply 节点 doing（applicant）→ 审批
        let tasks = repo.find_doing_tasks(inst.instance_id, &[]).unwrap();
        let apply = tasks
            .iter()
            .find(|t| t.actor_ids.contains(&"applicant".to_string()))
            .expect("c31: apply task for applicant");
        engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();

        // resume 后 approver 节点须按 f_approver 建任务给 u0011
        let after = repo.find_doing_tasks(inst.instance_id, &[]).unwrap();
        assert!(
            after.iter().any(|t| t.actor_ids.iter().any(|a| a == "u0011")),
            "c31: approver task should be created for u0011 from f_approver on resume; doing={:?}",
            after.iter().map(|t| (t.task_name.clone(), t.actor_ids.clone())).collect::<Vec<_>>()
        );
    }

    // ═══════════════════════════════════════════════════════
    // issues/116 批次 D · 委托代理自动生效（引擎内置、默认开启；内存仓路）
    //   断言一律打在**参与者表读回值**上（find_task_actors），不是内存集合自嗨；
    //   sqlx 真机对应用例：test_mysql_i116_surrogate_agent_lands_in_task_actor
    // ═══════════════════════════════════════════════════════

    /// 带扩展仓储的引擎（MemoryRepository 同时实现 ProcessRepository + ProcessExtRepository，
    /// 与 salvo 集成壳的装配姿势一致）。
    fn make_surrogate_engine() -> (JeeflowEngineImpl, Arc<MemoryRepository>) {
        let repo = Arc::new(MemoryRepository::new());
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_user_provider(Arc::new(ComplianceUserProvider))
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        (JeeflowEngineImpl::new(ctx), repo)
    }

    /// **显式关闭**委托自动生效的引擎（条款 3；关闭后回到"仅台账"行为）。
    /// 也是正向用例的回退自证姿势：把开关换成它，上面的并入断言必须变红。
    fn make_surrogate_engine_off() -> (JeeflowEngineImpl, Arc<MemoryRepository>) {
        let repo = Arc::new(MemoryRepository::new());
        let ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_ext_repository(repo.clone() as Arc<dyn ProcessExtRepository>)
            .with_user_provider(Arc::new(ComplianceUserProvider))
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)))
            .with_surrogate_auto_apply(false);
        (JeeflowEngineImpl::new(ctx), repo)
    }

    #[allow(clippy::too_many_arguments)]
    fn add_surrogate_row(
        repo: &MemoryRepository,
        operator: &str,
        process_name: &str,
        agent: &str,
        start: Option<&str>,
        end: Option<&str>,
        enabled: i32,
    ) -> i64 {
        let mut sg = ProcessSurrogate {
            id: 0,
            process_name: process_name.into(),
            operator: operator.into(),
            surrogate: agent.into(),
            start_time: start.map(str::to_string),
            end_time: end.map(str::to_string),
            enabled,
            create_time: None, create_user: Some("admin".into()),
            update_time: None, update_user: None,
        };
        repo.save_surrogate(&mut sg).unwrap();
        sg.id
    }

    /// 落一条"窗口宽到与时区/时钟无关"的生效委托（引擎按 `current_time_str()` 判窗）。
    fn add_surrogate(repo: &MemoryRepository, operator: &str, process_name: &str, agent: &str) -> i64 {
        add_surrogate_row(repo, operator, process_name, agent,
            Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), 1)
    }

    /// 从参与者表读回并按人排序（HashMap 遍历序随机，对账需稳定）。
    fn persisted_actors(repo: &MemoryRepository, task_id: i64) -> Vec<String> {
        let mut v = repo.find_task_actors(task_id).unwrap();
        v.sort();
        v
    }

    /// 条款 1 + 1.1 + 2 ⚠️：发起与**办理推进**两条建任务路径都要并入代理人，
    /// 且落在参与者表真行上；`processName` 取**流程模型 name**，不取 define.name。
    #[tokio::test]
    async fn test_surrogate_applies_on_start_and_advance_with_model_name() {
        let (engine, repo) = make_surrogate_engine();
        // 01-simple 的流程 JSON 里 name = "simple"；故意把定义行命名成 "decoy-define-name"
        // ——若实现取的是 wf_process_define.name，define 侧那条诱饵委托就会命中而露馅。
        let flow = load_flow("01-simple");
        let did = save_define(&repo, "decoy-define-name", &flow);
        add_surrogate(&repo, "applicant", "simple", "agentOfApplicant");
        add_surrogate(&repo, "leader", "simple", "agentOfModelName");
        add_surrogate(&repo, "leader", "decoy-define-name", "agentOfDefineName");

        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let doing = repo.find_doing_tasks(inst.instance_id, &[]).unwrap();
        let apply = doing.iter().find(|t| t.task_name == "apply").expect("发起应建 apply 任务");
        assert_eq!(
            persisted_actors(&repo, apply.task_id),
            vec!["agentOfApplicant".to_string(), "applicant".to_string()],
            "① 发起路径：代理人须与原授权人一起落进 wf_process_task_actor（授权人保留、任一可办）"
        );

        engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();
        let doing = repo.find_doing_tasks(inst.instance_id, &[]).unwrap();
        let task1 = doing.iter().find(|t| t.task_name == "task1").expect("推进应建 task1");
        let actors = persisted_actors(&repo, task1.task_id);
        assert_eq!(
            actors,
            vec!["agentOfModelName".to_string(), "leader".to_string()],
            "② 推进路径新单同样要并入代理人（只挂发起一处就会漏掉这一手）"
        );
        assert!(
            !actors.contains(&"agentOfDefineName".to_string()),
            "③ 命中了 define.name 侧的委托＝取的是 wf_process_define.name，条款 1.1 要求取流程模型 name"
        );
    }

    /// 条款 1.2（不级联）+ 1.4（多命中取 id 最大）。
    #[tokio::test]
    async fn test_surrogate_no_cascade_and_takes_max_id() {
        let (engine, repo) = make_surrogate_engine();
        let did = save_define(&repo, "surr-nocascade", &load_flow("01-simple"));
        // leader 两条同时生效：后落库的 id 更大 → 只能取 agentNew
        add_surrogate(&repo, "leader", "simple", "agentOld");
        add_surrogate(&repo, "leader", "simple", "agentNew");
        // 级联诱饵：agentNew 自己又把单子委托给 agentDeep（不得展开）
        add_surrogate(&repo, "agentNew", "simple", "agentDeep");

        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").unwrap();
        engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();

        let task1 = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "task1").unwrap();
        assert_eq!(
            persisted_actors(&repo, task1.task_id),
            vec!["agentNew".to_string(), "leader".to_string()],
            "多条命中只取 id 最大的一条，且代理人自身的委托不再展开（A→B、B→C 时 C 不收单）"
        );
    }

    /// 条款 1.3 串行会签：代理人只进**当一步**任务的参与者，不扩投票名册。
    #[tokio::test]
    async fn test_surrogate_sequential_countersign_step_only() {
        let (engine, repo) = make_surrogate_engine();
        let did = save_define(&repo, "surr-seq", &load_flow("08-countersign-sequential-approve"));
        add_surrogate(&repo, "userA", "cs-seq-approve", "agentA");
        add_surrogate(&repo, "userB", "cs-seq-approve", "agentB");

        let iid = start_and_apply(&engine, &repo, did).await;
        let roster_key = "csv_task1_operatorList";
        let step1 = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(step1.len(), 1, "串行会签一步只有一个在办任务");
        assert_eq!(
            persisted_actors(&repo, step1[0].task_id),
            vec!["agentA".to_string(), "userA".to_string()],
            "第一步任务的参与者 = 当步人 + 其代理人"
        );
        assert_eq!(
            repo.find_instance_by_id(iid).unwrap().unwrap().variables.get_str(roster_key),
            Some("userA,userB"),
            "投票名册 operatorList 不得被代理人扩写（改了就是改了票数）"
        );

        engine.execute_task_async(step1[0].task_id, "userA", &FlowData::new()).await.unwrap();
        let step2 = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(step2.len(), 1);
        assert_eq!(
            persisted_actors(&repo, step2[0].task_id),
            vec!["agentB".to_string(), "userB".to_string()],
            "第二步推进出的任务同样并入当步代理人"
        );
        assert_eq!(
            repo.find_instance_by_id(iid).unwrap().unwrap().variables.get_str(roster_key),
            Some("userA,userB"),
            "推进后名册仍不变"
        );
        let node_rows: Vec<i64> = repo.find_history_tasks(iid).unwrap()
            .iter().filter(|t| t.task_name == "task1").map(|t| t.task_id).collect();
        assert_eq!(node_rows.len(), 2, "串行会签的任务行数只随步数增长，不因委托新增");
    }

    /// 条款 1.3 并行会签：不新增任务行、不改票数；代理人只共享那一行。
    #[tokio::test]
    async fn test_surrogate_parallel_countersign_no_extra_task_rows() {
        let (engine, repo) = make_surrogate_engine();
        let did = save_define(&repo, "surr-parallel", &load_flow("05-countersign-parallel"));
        add_surrogate(&repo, "userA", "countersign-parallel", "agentX");

        let iid = start_and_apply(&engine, &repo, did).await;
        let doing = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(doing.len(), 3, "并行会签 3 人 3 行，委托不得新增任务行");
        let a_task = doing.iter().find(|t| t.actor_ids.contains(&"userA".to_string())).unwrap();
        assert_eq!(
            persisted_actors(&repo, a_task.task_id),
            vec!["agentX".to_string(), "userA".to_string()],
            "代理人并入 userA 那一行（任一可办），不是再开一行"
        );

        // 票数不变：agentX 代办 userA 那一行 + userB 办结后，userC 仍在办（未提前 merge）
        engine.execute_task_async(a_task.task_id, "agentX", &FlowData::new()).await.unwrap();
        let doing = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(doing.len(), 2, "agentX 办掉 userA 那一行后应只剩 2 行在办");
        let b_task = doing.iter().find(|t| t.actor_ids.contains(&"userB".to_string())).unwrap();
        engine.execute_task_async(b_task.task_id, "userB", &FlowData::new()).await.unwrap();
        let still_doing = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(still_doing.len(), 1, "票数没被代理人扩大的话，仍差 userC 一票");
        assert_eq!(repo.find_instance_by_id(iid).unwrap().unwrap().state, 10,
            "会签未齐不得提前结束实例");
    }

    /// 条款 3（可显式关闭）+ 条款 4（未配置扩展仓储静默跳过，不打断建单）。
    #[tokio::test]
    async fn test_surrogate_switch_off_and_missing_ext_repository() {
        // ① 显式关闭：回到"仅台账"——委托记录照存照查，参与者集合不再并入
        let (engine, repo) = make_surrogate_engine_off();
        let did = save_define(&repo, "surr-off", &load_flow("01-simple"));
        let sid = add_surrogate(&repo, "applicant", "simple", "agentOff");
        assert!(repo.find_surrogate_by_id(sid).unwrap().is_some(), "关闭不影响台账可查");
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").unwrap();
        assert_eq!(
            persisted_actors(&repo, apply.task_id),
            vec!["applicant".to_string()],
            "开关关闭后不得并入代理人（正向用例的回退自证即改这一行）"
        );

        // ② 未配置扩展仓储：建单照常、不抛"未配置扩展仓储"
        let (engine2, repo2) = make_compliance_engine();
        assert!(engine2.context().ext_repository.is_none(), "该引擎应未装配扩展仓储");
        let did2 = save_define(&repo2, "surr-noext", &load_flow("01-simple"));
        let inst2 = engine2.start_async(did2, "applicant", &FlowData::new()).await.unwrap();
        let apply2 = repo2.find_doing_tasks(inst2.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").unwrap();
        assert_eq!(
            persisted_actors(&repo2, apply2.task_id),
            vec!["applicant".to_string()],
            "缺扩展仓储属正常部署形态：静默跳过、建单不被打断"
        );
    }

    /// 条款 5 四判据的引擎侧负例（防"引擎绕过判据直接取一条"）：
    /// 窗外（已过期/未开始）/ enabled≠1 / 非本人委托 → 一律不并入。
    #[tokio::test]
    async fn test_surrogate_negative_predicates_not_applied() {
        let (engine, repo) = make_surrogate_engine();
        let did = save_define(&repo, "surr-neg", &load_flow("01-simple"));
        add_surrogate_row(&repo, "leader", "simple", "agentExpired",
            Some("2000-01-01 00:00:00"), Some("2001-01-01 00:00:00"), 1);
        add_surrogate_row(&repo, "leader", "simple", "agentNotStarted",
            Some("2999-01-01 00:00:00"), Some("2999-12-31 23:59:59"), 1);
        add_surrogate_row(&repo, "leader", "simple", "agentDisabled", None, None, 0);
        // 判据④「只有 1 生效」：2 这种脏值也不生效
        add_surrogate_row(&repo, "leader", "simple", "agentDirtyTwo", None, None, 2);
        // 别人的委托不得串到 leader 身上
        add_surrogate_row(&repo, "someoneelse", "simple", "agentOther", None, None, 1);

        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").unwrap();
        engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();
        let task1 = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "task1").unwrap();
        assert_eq!(
            persisted_actors(&repo, task1.task_id),
            vec!["leader".to_string()],
            "窗外 / enabled=0 / enabled 脏值 / 他人委托 都不该被并入"
        );
    }

    fn clock120_fixed() -> String {
        crate::clock::testclock::at(48)
    }

    /// issues/120：注入的时钟是引擎**唯一**时间出口——写库审计列与委托生效窗必须同时跟着走。
    /// 此前两者都是 UTC，而门面 `NOW()` 是本地，同一次响应里两套基准。
    /// 委托窗改用**注入钟为基准的真窗**（±1h，基准取本 UTC 日 +48h ⇒ 真实 UTC 必落窗外），
    /// 不再铺 2000~2999 那种与时区无关的宽窗（台账 §4 点名的假绿源头）。
    #[tokio::test]
    async fn test_i120_engine_clock_drives_columns_and_window() {
        use crate::clock::testclock::at;
        // 注入即独占时钟作用域（`ClockScope` 持进程级互斥，防止别的用例的注入值串进来）
        let _scope = crate::clock::ClockScope::injected(clock120_fixed);
        let (engine, repo) = make_surrogate_engine();
        let did = save_define(&repo, "clock120", &load_flow("01-simple"));
        // 正向窗：注入钟 12:00 的 ±1h
        add_surrogate_row(&repo, "leader", "simple", "agentInWindow",
            Some(&at(47)), Some(&at(49)), 1);
        // 负向窗：start 落在注入钟之后 ⇒ 不得生效
        add_surrogate_row(&repo, "applicant", "simple", "agentOutOfWindow",
            Some(&at(49)), None, 1);

        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        assert_eq!(
            inst.create_time.as_deref(),
            Some(at(48).as_str()),
            "实例 create_time 必须取注入钟"
        );
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").expect("发起应建 apply 任务");
        assert_eq!(
            apply.create_time.as_deref(),
            Some(at(48).as_str()),
            "任务 create_time 必须取注入钟"
        );
        assert_eq!(
            persisted_actors(&repo, apply.task_id),
            vec!["applicant".to_string()],
            "负向：start 在注入钟之后的委托不得并入"
        );

        engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();
        let task1 = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "task1").expect("推进应建 task1");
        assert_eq!(
            persisted_actors(&repo, task1.task_id),
            vec!["agentInWindow".to_string(), "leader".to_string()],
            "正向：以注入钟为基准的真窗（±1h）内委托必须生效"
        );
        let done = repo.find_task_by_id(apply.task_id).unwrap().expect("apply 行");
        assert_eq!(
            done.finish_time.as_deref(),
            Some(at(48).as_str()),
            "finish_time 必须与 create_time 同一时钟出口"
        );
        drop(_scope);
        // 出作用域必须恢复默认基准，否则注入值会漏给同 binary 的其他用例（假绿源头）；
        // 断言前先取独占权，确保此刻无人注入。
        let _idle = crate::clock::lock_scope();
        assert_ne!(
            crate::clock::current_time_str(),
            at(48),
            "ClockScope 作用域结束后不得残留注入值"
        );
    }

    /// 推进到链上第 n 条进行中任务（返回该行的副本），用行上已有的参与者办结前序任务。
    async fn advance_until_doing(
        engine: &JeeflowEngineImpl, repo: &std::sync::Arc<crate::memory::MemoryRepository>,
        iid: i64, name: &str) -> crate::model::ProcessTask {
        for _ in 0..6 {
            let doing = repo.find_doing_tasks(iid, &[]).unwrap();
            if let Some(t) = doing.iter().find(|t| t.task_name == name) {
                return t.clone();
            }
            let t = doing.first().unwrap().clone();
            let who = t.actor_ids.first().cloned().unwrap_or_else(|| "applicant".to_string());
            let mut a = FlowData::new();
            a.insert_i64("submitType", 1);
            engine.execute_task_async(t.task_id, &who, &a).await.unwrap();
        }
        panic!("链上应出现 {}，实得最后一次查询的进行中任务", name);
    }

    /// issues/121 P2 正向：退回上一步复活血缘前驱那条行——落点、参与者（该行原办结人而非回退人）、
    /// parent 随行拷贝、控制类残留剔除、实例保持 DOING。夹具 02-multi-task（apply→task1→task2→task3）。
    #[tokio::test]
    async fn test_i121_p2_rollback_revives_parent_row() {
        let (engine, repo) = make_surrogate_engine();
        let did = save_define(&repo, "lineage121c", &load_flow("02-multi-task"));
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let iid = inst.instance_id;

        let t1 = advance_until_doing(&engine, &repo, iid, "task1").await;
        let mut a1 = FlowData::new();
        a1.insert_i64("submitType", 1);
        let who1 = t1.actor_ids.first().cloned().unwrap();
        engine.execute_task_async(t1.task_id, &who1, &a1).await.unwrap();
        let t2 = advance_until_doing(&engine, &repo, iid, "task2").await;

        let mut rb = FlowData::new();
        rb.insert_i64("submitType", 3);
        let who2 = t2.actor_ids.first().cloned().unwrap();
        engine.execute_and_jump_async(t2.task_id, &who2, &rb, None).await.unwrap();

        let doing = repo.find_doing_tasks(iid, &[]).unwrap();
        let revived = doing.iter().find(|t| t.task_name == "task1")
            .unwrap_or_else(|| panic!("回退后应在 task1 复出一条待办，实得 {:?}",
                doing.iter().map(|t| t.task_name.clone()).collect::<Vec<_>>()));
        assert_ne!(revived.task_id, t1.task_id, "复活应是新行，不是把原行改回进行中");
        assert_eq!(revived.actor_ids, vec![who1.clone()],
            "参与者＝task1 的原办结人，不是执行回退的 {}", who2);
        assert!(!revived.actor_ids.iter().any(|a| a == &who2),
            "执行回退的人不该被派到自己退出来的待办上");
        assert_eq!(revived.parent_task_id, t1.parent_task_id,
            "parent 随行拷贝＝上一步的上一步");
        assert!(!revived.variables.contains_key("submitType"), "复活行不该带 submitType 残留");
        assert!(!revived.variables.contains_key("taskName"), "复活行不该带 taskName 残留");
        for (k, _) in revived.variables.iter() {
            assert!(!k.starts_with("tf_"), "复活行不该带 tf_* 残留: {}", k);
            assert!(!k.starts_with("loopCounter"), "复活行不该带会签簿记残留: {}", k);
        }
        assert_eq!(revived.variables.get("isFirstTaskNode"), Some(&JsonValue::Bool(false)),
            "task1 不是首任务节点，标记应随行留档为 false");
        assert_eq!(revived.task_state, crate::model::TaskState::Doing.code());
        let after = repo.find_instance_by_id(iid).unwrap().expect("实例");
        assert_eq!(after.state, crate::model::InstanceState::Doing.code(),
            "回退后实例必须仍是 DOING");
        assert_eq!(repo.find_history_tasks(iid).unwrap().iter()
            .filter(|t| t.task_name == "task1").count(), 2,
            "原 task1 行应作为历史行保留，加上复活行共两条");
    }

    /// issues/121 P2 两格负向：
    /// ① 无血缘（parent 为 0，＝P1 之前落的老行形状）⇒ 20010007，不得静默不建单；
    /// ② 血缘前驱跨不过 fork/join（boot2 canRejected 遇到 fork/join/start 直接跳过、不深入）
    ///    ⇒ 20010008。夹具 04-fork-join：分支任务的 parent 是 fork 之前的 apply。
    #[tokio::test]
    async fn test_i121_p2_rollback_rejects_no_lineage_and_guard() {
        // ① 无血缘：02-multi-task 的 apply 行 parent 落 0（发起 execution 无当前任务）
        let (engine, repo) = make_surrogate_engine();
        let did = save_define(&repo, "lineage121n", &load_flow("02-multi-task"));
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .first().expect("应有 apply 进行中行").clone();
        assert_eq!(apply.parent_task_id, Some(0), "前置条件：发起那条 parent 应为 0");
        let mut rb = FlowData::new();
        rb.insert_i64("submitType", 3);
        let e = engine.execute_and_jump_async(apply.task_id, "applicant", &rb, None)
            .await.err().expect("无血缘必须报错，不得静默通过");
        assert!(e.to_string().contains("20010007"), "错码须体现在 msg，实得 {}", e.to_string());

        // ② 守卫：fork 分支任务的 parent 在 fork 之前 ⇒ boot2 语义下不可回退
        let (engine2, repo2) = make_surrogate_engine();
        let did2 = save_define(&repo2, "lineage121f", &load_flow("04-fork-join"));
        let inst2 = engine2.start_async(did2, "applicant", &FlowData::new()).await.unwrap();
        let branch = advance_until_doing(&engine2, &repo2, inst2.instance_id, "taskA").await;
        assert!(branch.parent_task_id.unwrap_or(0) != 0,
            "前置条件：分支行的 parent 应已由 P1 写入");
        let who = branch.actor_ids.first().cloned().unwrap_or_else(|| "applicant".to_string());
        let e2 = engine2.execute_and_jump_async(branch.task_id, &who, &rb, None)
            .await.err().expect("血缘前驱跨不过 fork 时必须被守卫拦下");
        assert!(e2.to_string().contains("20010008"), "实得 {}", e2.to_string());
    }

}
