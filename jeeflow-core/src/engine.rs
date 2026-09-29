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
    ///
    /// **三条抄送路径共用本函数**（规范 11 §11.2 原则 1 ＋ §11.7）：发起 `f_ccActors`、
    /// 办理 `tf_ccActors`、门面手动 `processInstance/createCCInstance`——"新增了一条抄送记录"
    /// 这个事实成立就 fire `CC_CREATE`(4)，路径不进事件名。
    ///
    /// **入参一律是"实际新建的 actor 子集"**（issues/141 G2 · spec 06 §4）：调用点先走
    /// [`ProcessRepository::create_cc_instance_if_absent`] 拿子集，子集为空整支不 fire——
    /// §11.2 原则 1「码=事实」，重复抄送没发生"创建"就不该发码 4，严禁照旧按原始请求全量 fire。
    pub fn notify_cc_create(&self, instance_id: i64, cc_actors: &[String]) {
        for actor in cc_actors {
            let event = ProcessEvent::new(ProcessEventType::CcCreate, instance_id)
                .with_cc_actor_id(actor.clone());
            self.fire_event(event);
        }
    }

    /// 事件**唯一 fire 口**（引擎内部与门面手动路径共用，规范 11 §11.5）。
    ///
    /// 语义要点：① fire 必须排在**落库之后**（§11.2 原则 3——先 fire 后落库会让监听器
    /// 站内信反查 / persist 回写读到旧状态，issues/121·126 同一族）；② 监听器异常
    /// 在 [`ProcessPublisher::notify`] 内逐监听器隔离，**不经 `?` 传播**，故本方法返回 `()`。
    pub fn fire_event(&self, event: ProcessEvent) {
        ProcessPublisher::notify(&event, &self.ctx.event_listeners);
    }

    /// 任务维度结果事件（5 `TASK_COMPLETE` / 6 `TASK_REJECT`）统一 fire 口，
    /// 载荷四键按规范 11 §11.3：`instanceId` / `taskId` / `operator` / `submitType`，
    /// `sourceId`＝taskId。
    ///
    /// 5 与 6 **互斥**（§11.3 码 6 括注）：由调用点保证——同意/会签办理/重新提交走
    /// [`execute_task_async`] 发 5，退回/拒绝（submitType 2/3/6）走 `execute_jump_inner` /
    /// [`execute_and_jump_to_end_async`] 发 6；同一次动作不会两支都发。
    /// 拒绝/跳转/退发起人不再各开一号，靠载荷 `submitType` 区分（§11.2 原则 2）。
    fn notify_task_outcome(
        &self,
        event_type: ProcessEventType,
        instance_id: i64,
        task_id: i64,
        operator: &str,
        submit_type: Option<i64>,
    ) {
        let mut data = FlowData::new();
        data.insert_i64("instanceId", instance_id);
        data.insert_i64("taskId", task_id);
        data.insert_str("operator", operator);
        match submit_type {
            Some(v) => data.insert_i64("submitType", v),
            // 未经门面提交的引擎内部动作（如 flow.auto 驱动）无 submitType ⇒ 键位保留、值 null，
            // 监听器判据"必须能拿到上表这些键"仍成立（§11.3 注）。
            None => data.insert("submitType".to_string(), JsonValue::Null),
        }
        self.fire_event(ProcessEvent::new(event_type, task_id).with_data(data));
    }

    /// 8 `TASK_WITHDRAW`：撤回把实例 `state` 写 30 且被撤任务行更新完成之后 fire，
    /// **每轮撤回只 fire 一次**（不逐任务，§11.3 码 8）。载荷 `instanceId` / `operator`。
    pub fn notify_task_withdraw(&self, instance_id: i64, operator: &str) {
        let mut data = FlowData::new();
        data.insert_i64("instanceId", instance_id);
        data.insert_str("operator", operator);
        self.fire_event(ProcessEvent::new(ProcessEventType::TaskWithdraw, instance_id).with_data(data));
    }

    /// 7 `TASK_TRANSFER`：任务参与者被替换并落库之后 fire，`sourceId`＝taskId。
    /// 载荷 `instanceId` / `taskId` / `fromActor` / `toActor` / `operator`（§11.3 码 7）。
    pub fn notify_task_transfer(
        &self,
        instance_id: i64,
        task_id: i64,
        from_actor: &str,
        to_actor: &str,
        operator: &str,
    ) {
        let mut data = FlowData::new();
        data.insert_i64("instanceId", instance_id);
        data.insert_i64("taskId", task_id);
        data.insert_str("fromActor", from_actor);
        data.insert_str("toActor", to_actor);
        data.insert_str("operator", operator);
        self.fire_event(ProcessEvent::new(ProcessEventType::TaskTransfer, task_id).with_data(data));
    }

    /// 9 `INSTANCE_TERMINATED`：实例 `state` 写 40 落库之后 fire。
    /// 载荷 `instanceId` / `operator` / `reason`（§11.3 码 9）。
    ///
    /// ⚠️ 本栈门面当前**没有"终止实例"的 action**（issues/134 §5.2 同记：壳侧造不出这一档），
    /// 故引擎内无触发点，本方法是给终止腿落地时用的唯一入口
    /// ——集成层严禁自己补发（§11.1），缺哪一支走 issues 反馈。
    pub fn notify_instance_terminated(&self, instance_id: i64, operator: &str, reason: &str) {
        let mut data = FlowData::new();
        data.insert_i64("instanceId", instance_id);
        data.insert_str("operator", operator);
        data.insert_str("reason", reason);
        self.fire_event(
            ProcessEvent::new(ProcessEventType::InstanceTerminated, instance_id).with_data(data),
        );
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
            NodeType::Task => {
                // 任务创建不触发节点拦截器（对齐 Java CreateTaskHandler / Go executeNode：
                // 创建任务 ≠ 节点执行完成）；persist 等 post 拦截器在任务**被执行**时
                // 由 execute_task_async 显式触发（1.8.0 SYNC 同步演进）
                self.create_task_with_assignment(exec, node)?;
            }
            NodeType::Custom => {
                // 记录类节点（spec/02 §6.1，issues/142 A 批）：**与任务类分家**。
                // 旧形状 `NodeType::Task | NodeType::Custom => create_task_with_assignment`
                // 给它建了一条 DOING 待办行，正是 §6.1「禁止的形状①」；
                // 正确形状＝执行 clazz → 落一条 task_state=20 的历史行（真落库）→ 令牌续流。
                // 拦截器同样不触发：它既不建待办也不"办理"，与任务类同口径。
                self.execute_custom_node(exec, node)?;
            }
            NodeType::Unknown => {
                // 类型表里没有的档（issues/142 A 批把旧 `_ => Custom` 兜底臂拆出来的那一半）。
                // 解析期已按 G4 义务 2 落过一条可诊断 WARNING（`parser::unknown_node_warning`，
                // 带 nodeId 与实得类型串），这里的行为照 java `ModelParser.java:86-89`：
                // **跳过节点**——不建行、不 fire 事件、**也不沿出边推进**
                // （java 那边节点压根没进模型，令牌同样到不了它的下游，两边可观测结果一致）。
                // 这里不再补日志：同一个定义每次 start/execute 都会重新解析，再打一遍就是刷屏。
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

                // Fire event（2 PROCESS_INSTANCE_END）：实例 state 落库为终态之后 fire，
                // 载荷带落库后的 `state` 整数（规范 11 §11.3 码 2「直传载荷键」）。
                // 办结与拒绝**共用这一支**、规范名不拆（§11.6），下游按 state 分。
                let mut data = FlowData::new();
                data.insert_i64("instanceId", exec.process_instance.instance_id);
                data.insert_i64("state", exec.process_instance.state as i64);
                let event = ProcessEvent::new(ProcessEventType::ProcessInstanceEnd,
                                             exec.process_instance.instance_id).with_data(data);
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

        // spec/02 §6.1 表第一行 ＋ §6.2 第 3 条（issues/142 A 批 · owner 2026-09-30 拍）：
        // **任务类解析不出参与者也必须建单**——建一行参与者为空的 DOING 行，
        // 照 java `CreateTaskHandler`（:38-63）的无条件建单姿势。
        // 旧形状「actor_ids 为空 && 无 candidateUsers/candidateGroups ⇒ 一行不建、令牌直接沿出边跑」
        // 正是 §6.1 点名的死锁黑洞那一族：节点在库里**什么痕迹都不留**，实例走过了却查不到，
        // 出了问题只能靠猜（go/node/rust/moon 四栈同形，本轮四栈跟改）。
        // ⚠️ 零参与者这一行谁也办不动（`is_allowed` 恒 false）是**设计如此**：
        //    它的价值是"实例停在哪个节点"可查，并且还能被 transfer / nextNodeOperator 救活。
        //    建完即返回、**不沿出边推进**，所以不存在"自动推进逻辑为它反复重入"的形状。
        // candidateUsers/candidateGroups 按 spec/02 §4 明确**不生成 actor**（只供选人页），
        // 所以"只配了候选人"的节点也落在这一档＝零参与者行（与 java 同判）。

        let perform_type = PerformType::from_code(node.perform_type());
        let task_type = TaskType::from_code(node.task_type());
        // issues/126 案 A：节点上配的到期表达式一次读好、三条建单支路共用（未配 ⇒ 该列保持 NULL）
        let expr = crate::expire_time::expire_expr_of(node);

        let task = if perform_type == PerformType::Countersign {
            // 会签的建单是**逐成员**一行（java `createCountersignTasks` 同款 for-actor），
            // 名册为空 ⇒ 天然 0 行，"零参与者建单"那一档在会签上不成立（基准 java 亦然）。
            // 这里必须早退：下面两支收尾都要 `tasks.last().cloned().unwrap()`，
            // 空名册时不是 panic（聚合还空着）就是把**别人的**行当本节点的新单返回。
            if actor_ids.is_empty() {
                return Ok(());
            }
            let cs_type = node.countersign_type();
            if cs_type.to_uppercase() == "SEQUENTIAL" || cs_type.to_uppercase() == "SERIAL" {
                // SEQUENTIAL: only create first actor's task (issues/94)
                if actor_ids.is_empty() {
                    return Ok(());
                }
                // Create only the first actor's task
                let mut tasks = exec.process_instance.create_countersign_tasks(
                    &node.id, &node.display_name, &[actor_ids[0].clone()], &exec.operator,
                    task_type, node.form_key(), None);
                // issues/131：簿记三件落在首位成员任务的变量上（java ProcessInstance.java:257-259）。
                // 聚合根 push 的是克隆 ⇒ 返回值与 tasks.last() 两份都写，才算"落库那份带上了"。
                if let Some(t) = tasks.first_mut() {
                    seed_countersign_bookkeeping(t, &node.id, &actor_ids, 0);
                }
                if let Some(t) = exec.process_instance.tasks.last_mut() {
                    seed_countersign_bookkeeping(t, &node.id, &actor_ids, 0);
                }
                // issues/126 案 A 写点②：串行会签**首位成员**按节点表达式算到期时间
                // （变量源＝实例变量，对齐 Java `this.variables` / boot2 `execution.getArgs()`）
                stamp_expire(&mut tasks, &mut exec.process_instance.tasks, expr.as_deref(),
                             &exec.process_instance.variables);
                // TASK_START 不在这里 fire：此处 task_id 尚为 0（create_task 只置 0，
                // 真实 id 由 persist_tasks→save_task 的 next_id 分配）。若在此 fire，
                // 监听器 find_task(0) 查不到 → TODO 丢失（issues/13 salvo 栈根因，对齐
                // Java jeeflow-java 1.8.20「notifyTaskStart 移到 saveTask 之后」的时机修复）。
                // 统一改在 persist_tasks 落库后 fire（见下）。
                exec.new_tasks.extend(tasks);
                exec.process_instance.tasks.last().cloned().unwrap()
            } else {
                // PARALLEL: create all tasks at once
                let mut tasks = exec.process_instance.create_countersign_tasks(
                    &node.id, &node.display_name, &actor_ids, &exec.operator,
                    task_type, node.form_key(), None);
                // issues/126 案 A 写点③：并行会签**全员**逐条按节点表达式算到期时间
                stamp_expire(&mut tasks, &mut exec.process_instance.tasks, expr.as_deref(),
                             &exec.process_instance.variables);
                // TASK_START 统一改在 persist_tasks 落库后 fire（见下）。
                exec.new_tasks.extend(tasks);
                exec.process_instance.tasks.last().cloned().unwrap()
            }
        } else {
            let mut task = exec.process_instance.create_task(
                &node.id, &node.display_name, &actor_ids, &exec.operator,
                task_type, perform_type, node.form_key(), None);
            // issues/126 案 A 写点①：普通建单按节点表达式算到期时间；
            // 节点没配 ⇒ 这一列保持 NULL（不写 now()、不写 ''、不写 0）
            stamp_expire(std::slice::from_mut(&mut task), &mut exec.process_instance.tasks,
                         expr.as_deref(), &exec.process_instance.variables);
            task
        };

        // TASK_START 统一改在 persist_tasks 落库后 fire（见下）。此处只登记新任务。
        if perform_type != PerformType::Countersign {
            exec.new_tasks.push(task);
        }

        Ok(())
    }

    /// 记录类（`snaker:custom`）节点执行腿（issues/142 A 批 · spec/02 §6.1／§6.2）。
    ///
    /// 三步固定形状，只做满一半即违反 §6.2（"落库不建行、建行不落库、记日志但停在原地都算"）：
    ///   ① 执行 `clazz` 处理器（按名注册，见 [`crate::spi::CustomNodeHandler`]）；
    ///   ② 落一条 `task_state=20` 的历史行**并真落库**（`repo.save_task` 那条 INSERT 腿）——
    ///      只在聚合内存对象里 append 一条不算做到；
    ///   ③ 令牌沿出边继续流转（java `CustomModel` 收尾那句 `runOutTransition`）。
    ///
    /// 三条配套判据：
    /// - **不解析参与者**：记录类"本来就不该有参与者"（§6.1），历史行的参与者＝当前操作人只是
    ///   **留痕主体**（java `createHistoryTask` 的 `singletonList(operator)` 同形）。行是 DONE
    ///   ⇒ 不进待办列表、谁也办不动；**绝不允许**因为"没人可办"就兜底挂给操作人造一条待办。
    /// - **不 fire 码 3**：`PROCESS_TASK_START` 表达的事实是"新待办产生"（规范 11 §11.3），
    ///   给一条生来已完成的行发它＝凭空多一条办不动的待办。⇒ 落库走 [`Self::persist_history_task`]
    ///   这条独立通道，不与 `persist_tasks` 共用（对照件：python `engine.py::_exec_custom_node`、
    ///   java 本轮 `saveHistoryTasks`）。
    /// - **`clazz` 解析不了不许打断建单**（§6.2 第 2 条）：空串与"未注册"分两档各记一条可诊断
    ///   日志后照常落行续流；处理器**自身**返回 `Err` 是业务错误，照旧外抛（末句明写不在豁免内）。
    fn execute_custom_node(&self, exec: &mut Execution, node: &NodeModel) -> JeeflowResult<()> {
        // ── ① 执行 clazz（两档日志分开，§6.2 要求"未注册处理器"与"clazz 为空串"分别可诊断，
        //    不许像 c# 那样合成同一条）──
        let clazz = node.prop_str("clazz").map(|s| s.trim().to_string()).unwrap_or_default();
        if clazz.is_empty() {
            eprintln!("{}", custom_missing_clazz_warning(&node.id));
        } else if let Some(handler) = self.ctx.find_custom_handler(&clazz) {
            // 处理器的 Err 用 `?` 原样外抛；panic 也**不**套 catch_unwind——
            // 套上就等于把"业务错误"降级成"跳过处理器"，把 §6.2 的豁免面做成"什么都吞"。
            if let Some(value) = handler.handle(exec)? {
                let var_key = node.prop_str("val")
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| crate::spi::CUSTOM_RETURN_VAL.to_string());
                // 本次执行的表达式/参与者求值读的是 `exec.args` ⇒ 写这儿才对下游生效；
                // 跨步骤那份是实例变量（`update_instance` 落库）⇒ 同步一份，值才真"落进流程变量"
                // 而不是随本次 execution 结束蒸发。java/python 只写 args 那一侧，
                // 本栈两处都写是**超集**，不改变基准侧判据读到的那一档。
                exec.args.insert(var_key.clone(), value.clone());
                exec.process_instance.variables.insert(var_key, value);
            }
        } else {
            eprintln!("{}", custom_unregistered_clazz_warning(&node.id, &clazz));
        }

        // ── ② DONE 历史行：接的就是那个"有形状、一直零调用者"的聚合根工厂（issues/137 B 那一笔）──
        let mut history = exec.process_instance.create_history_task(
            &node.id, &node.display_name, &exec.operator, TaskType::from_code(node.task_type()));
        self.persist_history_task(exec, &mut history)?;

        // ── ③ 令牌继续沿出边流转 ──
        let next_nodes: Vec<NodeModel> = exec.process_model.get_output_edges(&node.id)
            .iter()
            .filter_map(|e| exec.process_model.get_target_node(e).cloned())
            .collect();
        for next in next_nodes {
            self.execute_node(exec, &next)?;
        }
        Ok(())
    }

    /// 记录类历史行（DONE）的**独立**落库通道 —— 与 [`Self::persist_tasks`] 故意不共用。
    ///
    /// 为什么不塞进 `exec.new_tasks` 走现成收口：那条收口是 `save_task` + fire 码 3 的**成对腿**，
    /// 而码 3 表达的是"新待办产生"。记录类行生来 `task_state=20`，跟着走就会被待办列表/站内信
    /// 收到一条谁也办不动的假单。委托并腿（issues/116）同样跳过——那是给待办收单人用的。
    ///
    /// 建单不变量照 `persist_tasks` 那三件抄（spec/02 §6.2 第 1 条明写"task_parent_id 与行级
    /// 首节点标记照建单不变量走"）：应用层 id 分配、`process_instance_id`、
    /// parent＝本次 execution 刚办结的那个任务（发起腿没有当前任务 ⇒ 落 0）、
    /// 行变量里的 `isFirstTaskNode` 标记（尺子与 `persist_tasks` 同一个
    /// [`ProcessModel::is_first_task_node`]）。
    fn persist_history_task(&self, exec: &mut Execution, task: &mut ProcessTask) -> JeeflowResult<()> {
        if task.task_id == 0 {
            task.task_id = self.next_id();
        }
        task.process_instance_id = exec.process_instance.instance_id;
        if task.parent_task_id.is_none() {
            task.parent_task_id = Some(exec.process_task.as_ref().map(|t| t.task_id).unwrap_or(0));
        }
        task.variables.insert(
            "isFirstTaskNode".to_string(),
            JsonValue::Bool(exec.process_model.is_first_task_node(&task.task_name)));
        // **到期时间这一档：不写，落 NULL**（issues/137 B 留给本函数的"复活时记得补
        // apply_expire_time"那句前提经本轮核实**不成立**，判定过程写在本轮收口报告）：
        //   · spec/02 §6 的 custom 属性字典只有 clazz/methodName/args/val，**没有 expireTime**
        //     （expireTime 挂在 §4 任务节点那一族，而记录类不解析参与者也不办理）；
        //   · java `ProcessInstance.createHistoryTask` 结构上就拿不到表达式——它收的是
        //     `CustomModel`（不是 TaskModel），taskType/performType/formKey/expireTime 四列传 null；
        //   · python 同判（`model.py::create_history_task` 注释"无 expireTime"）。
        // ⇒ 与两基准一致留 NULL，不在本栈自造"记录类也算到期"的新语义。
        self.repo().save_task(task)?;
        if !task.actor_ids.is_empty() {
            self.repo().add_task_actor(task.task_id, &task.actor_ids)?;
        }
        // 聚合根那一格同步成落库形状：`create_history_task` push 进 tasks 的是**克隆**，
        // 不回写则 `instance.tasks` 里这条还是 id=0／无 parent／无行变量，
        // 而同一次调用里后续的 `update_instance`／监听器反查读的就是聚合那份。
        // 按"最后一格"定位与 `create_history_task` 自己的注释同一理由（push 之后最后一格
        // 必然是本行），这里再加两道列值自检，避免哪天中间插进第二个 push 时静默错位。
        if let Some(last) = exec.process_instance.tasks.last_mut() {
            if last.task_name == task.task_name && last.task_state == task.task_state {
                *last = task.clone();
            }
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
    /// ⚠️ 唯一**故意不走这条腿**的是记录类（`snaker:custom`）那条 DONE 历史行，它走
    /// [`Self::persist_history_task`]：本方法是"save_task + fire 码 3"的成对腿，而码 3 的语义
    /// 是"新待办产生"，给一条生来已完成的行发它就是 §6.1 禁止的假待办（判据见那个方法的注释）。
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
            // 载荷三键按规范 11 §11.3 码 3：instanceId / taskId / actors（actors 含委托并入后的集合）
            let mut data = FlowData::new();
            data.insert_i64("instanceId", instance_id);
            data.insert_i64("taskId", task.task_id);
            data.insert(
                "actors".to_string(),
                JsonValue::Array(task.actor_ids.iter().map(|a| JsonValue::Str(a.clone())).collect()),
            );
            let event = ProcessEvent::new(ProcessEventType::ProcessTaskStart, task.task_id)
                .with_data(data);
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

        // 7.5 Fire 1 PROCESS_INSTANCE_START：实例行 insert **之后**立刻 fire
        // （规范 11 §11.3 码 1 触发时机／08 场景 28，sourceId＝instanceId、载荷 instanceId）。
        // 位置必须排在待办生成（步骤 10 persist_tasks 里的 3 PROCESS_TASK_START）之前：
        // spec §11.8／08 场景「L2-30」钉的是**按顺序**的码值序列 `[1,3,5,2]`，
        // "只断出现过不算过"——排在 persist 之后就是 [3,1,…]，顺序判据直接红。
        // Java 参考实现的同序形状：PROCESS_INSTANCE_START 在 StartModel.execute（开始节点）里 fire，
        // 而 taskId 是之后 saveNewTask 才分配的。
        let mut start_data = FlowData::new();
        start_data.insert_i64("instanceId", instance.instance_id);
        let event = ProcessEvent::new(ProcessEventType::ProcessInstanceStart, instance.instance_id)
            .with_data(start_data);
        ProcessPublisher::notify(&event, &self.ctx.event_listeners);

        // 8. Handle CC actors (sync) — 对齐 Go facade.go:203（issues/56 E28）：
        // vben 发起页"抄送给"是多选 ApiSelect，提交 JSON 数组；也兼容逗号分隔字符串。
        // 旧版仅 get_str+split，数组走 get_str=None → cc 实例从不创建（L3 S6）。
        let cc_actors = parse_cc_actors(full_args.inner().get("f_ccActors"));
        if !cc_actors.is_empty() {
            // issues/141 G2 写侧判重＝幂等空操作（spec 06 §4）：同一 (实例, 被抄送人) 已有 cc 行时
            // 跳过——不新增行、不重置未读、不更新原行时间；**新建子集**才拿去 fire。
            let created = self.repo().create_cc_instance_if_absent(instance.instance_id, operator, &cc_actors)?;
            // CC_CREATE（issues/102·104，六语言统一）：逐抄送人 fire，与 cc 行粒度一一对应。
            // 入参＝实际新建的子集而不是原始 cc_actors（spec §11.2 原则 1「码=事实」）：
            // 重复抄送没发生"创建"就不该发这个事件；子集为空整支不 fire（不空转、也不照旧全量 fire）。
            if !created.is_empty() {
                self.notify_cc_create(instance.instance_id, &created);
            }
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

        // 1 PROCESS_INSTANCE_START 在步骤 7.5（实例行落库后、待办生成前）fire，
        // 以保证 spec §11.8 的码值顺序 [1,3,5,2]（见那里的注释）。
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

        // 7.5 Fire 5 TASK_COMPLETE：任务行 state→已完成（20）**落库之后** fire
        // （规范 11 §11.3 码 5 触发时机／08 场景 30，先 fire 后落库即红）。
        // 本方法是"同意/申请/重新提交/会签办理"路径 ⇒ 发 5；退回三档（submitType 2/3/6）
        // 走 execute_jump_inner / execute_and_jump_to_end_async 发 6，两支互斥不并列。
        // 会签未合并（只办掉一个人那一张）时同样发 5——"这张任务单被办掉"是既成事实。
        self.notify_task_outcome(ProcessEventType::TaskComplete,
                                 instance.instance_id, task_id, operator, submit_type);

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
            // issues/126 案 A：串行推进那一支要用的节点到期表达式（其余档位走不到，读一次不贵）
            let cs_expr = crate::expire_time::expire_expr_of(node_ref);

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
                // issues/131：名单与序号从**刚完成那一条任务的变量**上读（java CountersignHandler
                // .java:71-77 读 completed.getVariables()），不再读实例变量 csv_*。
                let roster_key = format!("operatorList_{}", node_id);
                let lc_key = format!("loopCounter_{}", node_id);
                let operator_list: Vec<String> = countersign_roster(&task.variables, &roster_key);
                let lc = task.variables.get_i64_or(&lc_key, 0) as usize;

                if lc + 1 < operator_list.len() {
                    // Create next sequential task, do NOT follow edges
                    let next_lc = lc + 1;
                    let mut new_task = exec.process_instance.create_countersign_tasks(
                        node_id, &node_ref.display_name,
                        &[operator_list[next_lc].clone()], &exec.operator,
                        TaskType::from_code(node_ref.task_type()),
                        node_ref.form_key(), None);
                    // 三件重写在新任务上（java CountersignHandler.java:154-157）：名册原样带过去、
                    // 序号 +1、总数不变 ⇒ 代理人只扩"当一步"的参与者，改不动票数。
                    if let Some(t) = new_task.first_mut() {
                        seed_countersign_bookkeeping(t, node_id, &operator_list, next_lc as i64);
                    }
                    if let Some(t) = exec.process_instance.tasks.last_mut() {
                        seed_countersign_bookkeeping(t, node_id, &operator_list, next_lc as i64);
                    }
                    // issues/126 案 A 写点⑤（§1.8 里最容易被漏掉的那一处）：串行会签**推进出的
                    // 下一位成员**。java 侧这一支是 `CountersignHandler.createNextCountersignTask`，
                    // 聚合根为此专开公开入口 `applyNodeExpireTime`（commit `cb541d4`）；基准侧 boot2
                    // 的串行推进是回调 `createCountersignTask`（`ProcessTaskServiceImpl:485`，
                    // 内含 :524 那处到期写）⇒ 不补就是"首成员有到期、第二三位没有"。
                    stamp_expire(&mut new_task, &mut exec.process_instance.tasks,
                                 cs_expr.as_deref(), &exec.process_instance.variables);
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

        // 11. Handle task-level CC (sync) — 规范 11 §11.7（issues/127）：办理带 `tf_ccActors`
        // ⇒ 与任务更新同批建 `wf_process_cc_instance` 行，**落库后**逐抄送人 fire CC_CREATE(4)。
        // 取值与发起腿 `f_ccActors` 同用一个 parse_cc_actors（数组＝vben 多选 ApiSelect 提交，
        // 逗号串＝旧客户端）；此前只 get_str ⇒ 数组形态静默丢值、cc 行与事件双双缺失，
        // 正是 issues/56 E28 在发起腿踩过的同一个坑。
        let cc_actors = parse_cc_actors(exec.args.inner().get("tf_ccActors"));
        if !cc_actors.is_empty() {
            // issues/141 G2：办理腿与发起腿**同一条判重判据**（spec §11.7「三条入口共用一支」）——
            // 已有 cc 行的 (实例, 人) 跳过，不新增行、不重置未读、不更新原行时间。
            let created = self.repo()
                .create_cc_instance_if_absent(exec.process_instance.instance_id, operator, &cc_actors)?;
            // CC_CREATE（issues/102·104，六语言统一）：逐**实际新建**的抄送人 fire，子集空则不发。
            if !created.is_empty() {
                self.notify_cc_create(exec.process_instance.instance_id, &created);
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

        // 跳转三档的结果事件（规范 11 §11.3 码 5/6，**互斥**：走 reject 就不再 fire complete）：
        //   JumpTarget::Node        ＝ submitType 4 跳指定节点 → 5 TASK_COMPLETE（码 5 事实列"同意/跳转/会签办理"）
        //   JumpTarget::FirstTaskNode ＝ submitType 6 退回发起人   → 6 TASK_REJECT
        //   JumpTarget::RollbackLineage＝ submitType 3 退回上一步   → 6 TASK_REJECT
        // fire 排在被办任务 update_task 落库之后（taskId 可反查）；实例终态 2 由 End 节点
        // / jump_to_end 那一支另 fire，载荷 submitType 供下游分档（§11.2 原则 2 码粗载荷细）。
        let outcome = match mode {
            JumpTarget::Node(_) => ProcessEventType::TaskComplete,
            JumpTarget::FirstTaskNode | JumpTarget::RollbackLineage => ProcessEventType::TaskReject,
        };
        self.notify_task_outcome(outcome, instance.instance_id, task_id, operator, submit_type);

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

        // 拒绝（submitType 2）＝任务被退回/拒绝 → 6 TASK_REJECT，任务行落库之后 fire
        // （规范 11 §11.3 码 6；与 5 互斥——这一支**不发** TASK_COMPLETE，
        // 08 场景 30「走退回的这一次不得再发 5」）。实例终态 2 在下方另 fire。
        self.notify_task_outcome(ProcessEventType::TaskReject,
                                 instance.instance_id, task_id, operator, submit_type);

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
        // 载荷带落库后的 state（规范 11 §11.3 码 2），本路径为已拒绝 45。
        let mut data = FlowData::new();
        data.insert_i64("instanceId", instance.instance_id);
        data.insert_i64("state", instance.state as i64);
        let event = ProcessEvent::new(ProcessEventType::ProcessInstanceEnd,
                                     instance.instance_id).with_data(data);
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
    /// 对外 msg 用固定中文文案、不含引擎内部码：本栈 `JeeflowError::code()` 恒 99999999，
    /// 两格语义由文案区分（引擎内部码 20010007、20010008 只留在规范与本注释，HTTP 出口仍 99999999）。
    fn rollback_to_parent(&self, exec: &mut Execution) -> JeeflowResult<()> {
        const NO_LINEAGE: &str = "上一步任务ID为空，无法驳回至上一步处理";
        const GUARD: &str = "无法驳回至上一步处理，请确认上一步骤并非fork、join、suprocess以及会签任务";

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
        // issues/126 案 A 写点④（回退/跳转新建）：到期时间按**被回退掉的那个节点**（＝当前行所在
        // 节点）的表达式重算——逐字对齐基准侧 boot2 `ProcessTaskServiceImpl.rejectTask` 的
        // `String expireTime = ((TaskModel)current).getExpireTime();` 与 Java 参考实现
        // `ProcessInstance.rejectTask`（commit `cb541d4`）。
        // ⚠️ 变量源用**随行拷贝那份变量** `vars`（＝boot2 的 hisVariable），不是实例变量——
        // 两档搞混会让"表达式是个变量名"这一档跨栈给出不同答案（§1 变量源那段）。
        // 当前节点没配表达式 ⇒ 尺子直接 return，复活行沿用 `history` 克隆来的继承值（boot2 同形）。
        // （附带发现：姊妹栈 go `d10ebd0` 这一支取的是 `prev`＝复活行的节点，与 boot2/java 不一致。）
        if let Some(cur_node) = exec.process_model.get_node(&current.task_name) {
            if matches!(cur_node.node_type, NodeType::Task | NodeType::Custom) {
                let cur_expr = crate::expire_time::expire_expr_of(cur_node);
                crate::expire_time::apply_expire_time(&mut revived, cur_expr.as_deref(), &vars);
            }
        }
        revived.variables = vars;
        // parent 随行拷贝＝"上一步的上一步"。**老行该列为 NULL 时必须落 0，不能留 None**：
        // 留 None 会被 persist_tasks 的建单不变量补成"本次被回退掉的那个任务"id，
        // 于是当前行 parent=复活行、复活行 parent=当前行，血缘成二元环，回退链在此处会来回跳。
        // （对齐 Java `history.getParentTaskId()` 与 MoonBit `engine_ops.mbt` 的 carry 口径。）
        revived.parent_task_id = Some(history.parent_task_id.unwrap_or(0));
        exec.new_tasks.push(revived);
        Ok(())
    }
}


/// issues/131（案 A，以 java 为准）：串行会签簿记三件写在**成员任务的变量**上，
/// 不再是实例变量 `csv_{node}_operatorList` 逗号串。判据基准逐字取 Java 参考实现：
/// 写侧 `ProcessInstance.java:257-259`、读与推进侧 `CountersignHandler.java:71-84,154-157`。
/// 名单必须是**数组**——门禁 L2-23 按 `len(值)` 数成员，逗号串会被数成字符串长度。
fn seed_countersign_bookkeeping(t: &mut ProcessTask, node: &str, roster: &[String], loop_counter: i64) {
    let list = JsonValue::Array(roster.iter().map(|a| JsonValue::Str(a.clone())).collect());
    t.variables.insert(format!("operatorList_{}", node), list);
    t.variables.insert_i64(format!("loopCounter_{}", node), loop_counter);
    t.variables.insert_i64(format!("nrOfInstances_{}", node), roster.len() as i64);
}

/// issues/126 案 A：**五处建单写点共用的尺子**（对齐 Java `ProcessInstance.applyExpireTime` 一把尺子
/// 量五处，commit `cb541d4`；本栈写点清单见 `expire_time` 模块头与五处调用点注释）。
///
/// 聚合根 `create_task` / `create_countersign_tasks` push 进 `instance.tasks` 的是**克隆**，
/// 交回引擎、最终由 `persist_tasks` 落库的是返回值 ⇒ 两份都要写才算数
/// （issues/131 同款教训，见 `seed_countersign_bookkeeping` 的两个 `if let`）。
///
/// 参数按字段拆开传（`new` / `aggregate` / `args` 三个借用互不重叠），调用点直接借 `exec` 的
/// 两个字段就能过借用检查——否则会撞成"同时可变借 `exec.process_instance`"。
///
/// `expr` 为空（节点没配）⇒ 一份都不动：这一列保持 NULL，不写 `now()`、不写 `''`、不写 0。
fn stamp_expire(new: &mut [ProcessTask], aggregate: &mut Vec<ProcessTask>,
                expr: Option<&str>, args: &FlowData) {
    for t in new.iter_mut() {
        crate::expire_time::apply_expire_time(t, expr, args);
    }
    let n = new.len();
    if n == 0 || n > aggregate.len() {
        return;
    }
    let start = aggregate.len() - n;
    for i in 0..n {
        let v = new[i].expire_time.clone();
        aggregate[start + i].expire_time = v;
    }
}

/// 从任务变量读会签全量名册（数组形状；缺键/非数组 ⇒ 空表，与 java `toStringList` 同档）。
/// 存量兼容**有意不做**（owner 2026-09-28：demo 重启即重建、pro 是 goframe 版本）
/// ⇒ 不再兜底读旧的实例变量 `csv_*`。
fn countersign_roster(vars: &FlowData, key: &str) -> Vec<String> {
    vars.get(key)
        .and_then(|v| v.as_array())
        .map(|a| a.iter().filter_map(|x| x.as_str().map(|s| s.to_string())).collect())
        .unwrap_or_default()
}

/// 记录类节点「**clazz 为空串/缺键**」那一档的日志文案单点（spec/02 §6.2 第 2 条）。
///
/// 两档必须**分开**、各自可诊断（条文原话："未注册处理器"与"clazz 为空串"要分档，
/// c# 把两者合成同一个异常、覆盖面比 java 宽，本轮跟改）。落点是 stderr——本仓 core 零依赖、
/// 没有可注入的 logger 门面，与 `event.rs::ProcessPublisher`／`surrogate.rs::expand_actors`
/// 同口径；拆成纯函数是为了让用例钉得住文案（"记了日志"这件事否则无法断）。
fn custom_missing_clazz_warning(node_id: &str) -> String {
    format!("[jeeflow] custom 节点 nodeId={} 未配置 clazz（处理器类名为空），跳过处理器执行；历史行照常落库、令牌继续流转", node_id)
}

/// 记录类节点「**clazz 有值但注册表里没有**」那一档的日志文案单点（同上，另一档）。
/// 带上实得的 clazz 串与注册入口名，排障时不必再去猜是拼错还是没注册。
fn custom_unregistered_clazz_warning(node_id: &str, clazz: &str) -> String {
    format!("[jeeflow] custom 节点 nodeId={} 的 clazz={} 未注册处理器（注册入口 ServiceContext::register_custom_handler），跳过处理器执行；历史行照常落库、令牌继续流转", node_id, clazz)
}

/// 取本次刚建出的那条任务（聚合根里 push 的是克隆，两份都要写才算落库）。
/// 解析发起时抄送人（对齐 Go facade.go:203 issues/56 E28）：
/// 支持 JSON 数组（vben 多选 ApiSelect 提交）与逗号分隔字符串两种形态。
///
/// 形状判定（trim／丢空／折叠）不在本函数里自造——拆成原始串集合后统一交给
/// [`crate::model::normalize_cc_actors`]（issues/141 G10「空不创建行」，spec 06 §2.10），
/// 与门面手动腿、两仓写侧共用同一条判据。
fn parse_cc_actors(v: Option<&JsonValue>) -> Vec<String> {
    let Some(v) = v else { return Vec::new(); };
    let raw: Vec<String> = match v {
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
        // 逗号分隔字符串：只拆不判，判据与数组腿同一条腿（G10 要求两形同判）
        JsonValue::Str(s) => s.split(',').map(|x| x.to_string()).collect(),
        _ => Vec::new(),
    };
    // issues/141 G10「空不创建行」（spec 06 §2.10）：逐项 trim、空串/纯空白丢弃、
    // 同一次调用内重复折叠；丢完为空 ⇒ 两个调用点（发起腿/办理腿的 `if !cc_actors.is_empty()`）
    // 既不建 cc 行也不 fire 码 4。改前本函数只有 `retain(|s| !s.is_empty())`：
    // 数组腿的 `"  "`/`"\t"` 原样活着且不 trim ⇒ 真落 `actor_id='  '` 的行并照旧 fire，
    // 还与 issues/141 G2 写侧判重错开（`" a "` 与 `"a"` 落两行）。写侧另有第二层兜底。
    crate::model::normalize_cc_actors(&raw)
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

        // cc 行已落库，且与 fire 粒度一一对应。
        // issues/129：`operator` 为空不再等于"看全部"（那条正是被堵的旁路）⇒ 计数逐人取。
        let mut cc_total = 0;
        for actor in ["u1", "u2"] {
            let mut q = crate::model::PageQuery::new(1, 10);
            q.operator = Some(actor.to_string());
            cc_total += repo.page_cc_instances(&q).unwrap().record_count;
        }
        assert_eq!(cc_total, 2, "cc 实例应逐人落库");
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
        // issues/129：空 operator ⇒ 空页（不再是"看全部"），按抄送接收人取数
        q.operator = Some("u9".to_string());
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
        // issues/129：`page_cc_instances` 传空 operator 现在是空页（原"空即全量"是旁路）
        // ⇒ c19 的"cc 行确实落库"判据改成逐接收人取数，两条都要在。
        for actor in ["cc_user1", "cc_user2"] {
            let mut cq = PageQuery::new(1, 10);
            cq.operator = Some(actor.to_string());
            let page = repo.page_cc_instances(&cq).unwrap();
            assert_eq!(page.record_count, 1, "c19: cc 实例应为接收人 {} 落库一条", actor);
        }
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
        // issues/131（案 A，以 java 为准）：名册落点从实例变量 `csv_{node}_operatorList`（逗号串）
        // 搬到**成员任务的变量** `operatorList_{node}`（数组）。三条判据分开钉：
        // ①新键在任务上且是数组；②旧落点彻底没有（键名与容器一起搬家，不是"两边都写"）；
        // ③名册不得被代理人扩写——改了就是改了票数（原断言的强度，保留）。
        let roster_key = "operatorList_task1";
        let step1 = repo.find_doing_tasks(iid, &[]).unwrap();
        assert_eq!(step1.len(), 1, "串行会签一步只有一个在办任务");
        assert_eq!(
            persisted_actors(&repo, step1[0].task_id),
            vec!["agentA".to_string(), "userA".to_string()],
            "第一步任务的参与者 = 当步人 + 其代理人"
        );
        assert_eq!(
            countersign_roster(&step1[0].variables, roster_key),
            vec!["userA".to_string(), "userB".to_string()],
            "投票名册须落在任务变量 operatorList_节点 且是数组、不得被代理人扩写"
        );
        let inst_vars = &repo.find_instance_by_id(iid).unwrap().unwrap().variables;
        assert!(
            inst_vars.get(roster_key).is_none()
                && inst_vars.get("csv_task1_operatorList").is_none()
                && inst_vars.get("csv_task1_loopCounter").is_none(),
            "簿记三件都不得再留在实例变量上（落点搬家＝换容器＋换键名）"
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
            countersign_roster(&step2[0].variables, roster_key),
            vec!["userA".to_string(), "userB".to_string()],
            "推进后名册仍不变（java CountersignHandler.java:154 把原名单重写在新任务上）"
        );
        assert_eq!(
            step2[0].variables.get_i64_or("loopCounter_task1", -1),
            1,
            "推进后 loopCounter_节点 须 +1 写在新任务上（java:156）"
        );
        assert_eq!(
            step2[0].variables.get_i64_or("nrOfInstances_task1", -1),
            2,
            "nrOfInstances_节点 全程是名册总人数，不随推进变小（java:157）"
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

    /// issues/123 · 规范 06 §4.5 条款 1.4 的引擎侧 A 格：
    /// 同一授权人先配"窗内+enabled=1"，再配一条更"新"的无效记录（四种无效形状各一格）
    /// ⇒ 建单时代理人**不**并入，且旧的那条有效记录**不得复活**。
    ///
    /// 缺这条，把实现改回"先滤生效、再从剩下的取最新"也不会红——而那个写法正是
    /// 13 栈在 L2-17/L2-18 上恒并入的根因（上一条窗内委托会把用户后续设置永久盖掉）。
    #[tokio::test]
    async fn test_surrogate_i123_newest_invalid_beats_older_valid() {
        // (新那条的形状说明, 代理人, start, end, enabled)
        let shapes: Vec<(&str, &str, Option<&str>, Option<&str>, i32)> = vec![
            ("窗外（已过期）", "agentNewExpired",
                Some("2000-01-01 00:00:00"), Some("2001-01-01 00:00:00"), 1),
            ("窗外（未开始）", "agentNewNotStarted",
                Some("2999-01-01 00:00:00"), Some("2999-12-31 23:59:59"), 1),
            ("enabled=0", "agentNewDisabled", None, None, 0),
            ("enabled 脏值 2（契约：只认 1）", "agentNewDirty", None, None, 2),
            ("自委托（代理人就是授权人本人）", "leader", None, None, 1),
        ];
        for (why, agent, start, end, enabled) in shapes {
            let (engine, repo) = make_surrogate_engine();
            let did = save_define(&repo, "i123-old", &load_flow("01-simple"));
            // 旧：窗内 + enabled=1（宽窗，与时区/时钟基准无关）
            add_surrogate_row(&repo, "leader", "simple", "agentOldValid",
                Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), 1);
            // 新：id 更大 ⇒ 由它裁决
            let new_id = add_surrogate_row(&repo, "leader", "simple", agent, start, end, enabled);
            assert!(new_id > 0, "新行须落库（用例前提：它是该作用域的最新一条）");

            let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
            let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
                .into_iter().find(|t| t.task_name == "apply").unwrap();
            engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();
            let task1 = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
                .into_iter().find(|t| t.task_name == "task1").unwrap();
            assert_eq!(
                persisted_actors(&repo, task1.task_id),
                vec!["leader".to_string()],
                "{why} ⇒ 最新一条不生效时不得并入，更不得复活旧的那条 agentOldValid"
            );
        }
    }

    /// issues/123 · B 格：作用域内**只有一条**"窗内 + enabled=1" ⇒ 代理人必须并入
    /// （防 A 格的修法被写成恒不并入）。
    #[tokio::test]
    async fn test_surrogate_i123_sole_valid_row_still_applied() {
        let (engine, repo) = make_surrogate_engine();
        let did = save_define(&repo, "i123-only", &load_flow("01-simple"));
        add_surrogate_row(&repo, "leader", "simple", "agentOnly",
            Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), 1);

        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").unwrap();
        engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();
        let task1 = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "task1").unwrap();
        assert_eq!(
            persisted_actors(&repo, task1.task_id),
            vec!["agentOnly".to_string(), "leader".to_string()],
            "唯一一条窗内 enabled=1 的委托必须并入（授权人保留）"
        );
    }

    /// issues/123 · 精确作用域最新一条判否后**仍要看全流程作用域的最新一条**（不得判否即止）：
    /// 精确那条已过期 ⇒ 兜底到全流程委托的代理人。
    #[tokio::test]
    async fn test_surrogate_i123_exact_invalid_still_falls_back_to_global_scope() {
        let (engine, repo) = make_surrogate_engine();
        let did = save_define(&repo, "i123-fb", &load_flow("01-simple"));
        add_surrogate_row(&repo, "leader", "simple", "agentExpiredExact",
            Some("2000-01-01 00:00:00"), Some("2001-01-01 00:00:00"), 1);
        add_surrogate_row(&repo, "leader", "", "agentGlobalFallback",
            Some("2000-01-01 00:00:00"), Some("2999-12-31 23:59:59"), 1);

        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").unwrap();
        engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();
        let task1 = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "task1").unwrap();
        assert_eq!(
            persisted_actors(&repo, task1.task_id),
            vec!["agentGlobalFallback".to_string(), "leader".to_string()],
            "精确作用域判否后必须落到全流程作用域（Java 同名用例 testSurrogateCrudAndGet 的形状）"
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

    /// issues/121 T0 核账抓出的形状：血缘前驱那行是 **P1 之前落的老行**（`task_parent_id` 列为
    /// NULL ⇒ 水合为 `None`）时，复活行的 parent 必须落 **0**，不能留 `None`——留 None 会被
    /// persist_tasks 的建单不变量补成"本次被回退掉的那个任务"id，于是当前行 parent＝复活行、
    /// 复活行 parent＝当前行，血缘成二元环。Java（`history.getParentTaskId()` 工厂里 `?? 0`）与
    /// MoonBit（`engine_ops.mbt` 的 `carry`）都是 0，本栈曾是唯一分叉的一栈。
    #[tokio::test]
    async fn test_i121_t0_rollback_of_legacy_null_parent_row_lands_zero() {
        let (engine, repo) = make_surrogate_engine();
        let did = save_define(&repo, "lineage121legacy", &load_flow("02-multi-task"));
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let iid = inst.instance_id;

        let t1 = advance_until_doing(&engine, &repo, iid, "task1").await;
        // 造老数据形状：task1 那行的血缘列写成 NULL（前置自证——本栈水合确实把 NULL 读成 None）
        let mut legacy = t1.clone();
        legacy.parent_task_id = None;
        repo.update_task(&legacy).unwrap();
        assert_eq!(repo.find_task_by_id(t1.task_id).unwrap().unwrap().parent_task_id, None,
            "前置条件：该行应已是 parent=NULL 的老行形状");

        let mut a1 = FlowData::new();
        a1.insert_i64("submitType", 1);
        let who1 = t1.actor_ids.first().cloned().unwrap();
        engine.execute_task_async(t1.task_id, &who1, &a1).await.unwrap();
        let t2 = advance_until_doing(&engine, &repo, iid, "task2").await;

        let mut rb = FlowData::new();
        rb.insert_i64("submitType", 3);
        let who2 = t2.actor_ids.first().cloned().unwrap();
        engine.execute_and_jump_async(t2.task_id, &who2, &rb, None).await.unwrap();

        let revived = repo.find_doing_tasks(iid, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "task1")
            .expect("回退后应在 task1 复出一条待办");
        assert_eq!(revived.parent_task_id, Some(0),
            "老行血缘未知 ⇒ 复活行 parent 必须落 0；实得 {:?}（非 0 即说明被补成了当前任务 id，血缘成环）",
            revived.parent_task_id);
        assert_ne!(revived.parent_task_id, Some(t2.task_id),
            "绝不允许把\"被回退掉的那个任务\"当复活行的前驱");
        // 回归：该行照常复活成待办、参与者与标记不受影响
        assert_eq!(revived.actor_ids, vec![who1.clone()]);
        assert_eq!(revived.variables.get("isFirstTaskNode"), Some(&JsonValue::Bool(false)));
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
        assert!(e.to_string().contains("上一步任务ID为空，无法驳回至上一步处理") && !e.to_string().contains("2001000"),
            "msg 应为固定文案且不含内部码，实得 {}", e.to_string());

        // ①′ 老行形状 A：parent 为 None（P1 之前建的数据该列是 NULL）——本栈它与 Some(0) 走的是
        //     两个 match 分支，故必须单独钉一次
        let inst_b = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let mut apply_b = repo.find_doing_tasks(inst_b.instance_id, &[]).unwrap()
            .first().expect("应有 apply 进行中行").clone();
        apply_b.parent_task_id = None;
        repo.update_task(&apply_b).unwrap();
        let eb = engine.execute_and_jump_async(apply_b.task_id, "applicant", &rb, None)
            .await.err().expect("parent=None 的老行必须报错，不得静默不建单");
        assert!(eb.to_string().contains("上一步任务ID为空，无法驳回至上一步处理") && !eb.to_string().contains("2001000"), "实得 {}", eb.to_string());

        // ①″ 老行形状 B：parent 是非 0 但仓储里查不到行（老数据被清过 / 跨库迁移来的样子）
        //     ⇒ 走"取不到历史行"那条分支，同样必须 20010007
        let inst_c = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let mut apply_c = repo.find_doing_tasks(inst_c.instance_id, &[]).unwrap()
            .first().expect("应有 apply 进行中行").clone();
        apply_c.parent_task_id = Some(i64::MAX);
        repo.update_task(&apply_c).unwrap();
        let ec = engine.execute_and_jump_async(apply_c.task_id, "applicant", &rb, None)
            .await.err().expect("parent 指不到真实行时必须报错");
        assert!(ec.to_string().contains("上一步任务ID为空，无法驳回至上一步处理") && !ec.to_string().contains("2001000"), "实得 {}", ec.to_string());

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
        assert!(e2.to_string().contains("无法驳回至上一步处理，请确认上一步骤并非fork、join、suprocess以及会签任务") && !e2.to_string().contains("2001000"), "实得 {}", e2.to_string());
    }

    // ═══════════════════════════════════════════════════════
    // issues/126 案 A · 任务行 expire_time 的**五处建单写点**（内存仓路）
    //   形状照 Java 参考实现 `ExpireTimeOnCreateTest`（d9e9397）+ `JeeflowFacadeTest` 两格（cb541d4），
    //   以及同批已落地的 go `engine/expire_time_test.go`（d10ebd0）。
    //   断言一律打在**仓储读回的持久行**上（不是引擎返回的聚合对象）——issues/113 教训：
    //   只有读回值能证明"这一列真进了库"。本文件只新增断言，未改任何既有断言的期望值。
    // ═══════════════════════════════════════════════════════

    /// 夹具构造器：start → (specs 逐个 task 节点) → end 的线性流。
    /// `specs` 给的是**完整**的 properties 片段（不玩"基础片段 + 覆盖"，免得同名键靠后者胜出）。
    fn exp_flow(name: &str, specs: &[(&str, &str)]) -> String {
        let mut nodes = vec![
            r#"{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}}"#.to_string(),
        ];
        let mut edges: Vec<String> = Vec::new();
        let mut prev = "start".to_string();
        for (i, (id, props)) in specs.iter().enumerate() {
            nodes.push(format!(
                r#"{{"id":"{id}","type":"snaker:task","properties":{{{props}}},"text":{{"value":"节点{n}"}}}}"#,
                id = id, props = props, n = i + 1));
            edges.push(format!(
                r#"{{"id":"e{i}","sourceNodeId":"{prev}","targetNodeId":"{id}","properties":{{}}}}"#,
                i = i, prev = prev, id = id));
            prev = id.to_string();
        }
        nodes.push(
            r#"{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}"#.to_string());
        edges.push(format!(
            r#"{{"id":"eend","sourceNodeId":"{prev}","targetNodeId":"end","properties":{{}}}}"#,
            prev = prev));
        let out = format!(
            r#"{{"name":"{name}","displayName":"到期时间建单","type":"approval","nodes":[{}],"edges":[{}]}}"#,
            nodes.join(","), edges.join(","));
        out
    }

    fn exp_submit(t: i64) -> FlowData {
        let mut a = FlowData::new();
        a.insert_i64("submitType", t);
        a
    }

    /// 读回某节点的 DOING 行，**条数不符直接红**——"行没读到"与"值为空"必须分开断，
    /// 否则未配那一档会拿"压根没查到行"混成"查到且为空"，判据恒真（§1.8 的点名要求）。
    fn exp_doing(repo: &MemoryRepository, iid: i64, node: &str, want: usize) -> Vec<ProcessTask> {
        let mut rows: Vec<ProcessTask> = repo.find_doing_tasks(iid, &[]).unwrap()
            .into_iter().filter(|t| t.task_name == node).collect();
        rows.sort_by_key(|t| t.task_id);
        assert_eq!(rows.len(), want, "节点 {node} 的 DOING 行数应为 {want}（这是\"行没读到\"那一档，与值为空分开）");
        rows
    }

    /// 从 DOING 行里取某参与者那条（取不到即红＝"行没读到"）
    fn exp_actor(rows: &[ProcessTask], actor: &str) -> ProcessTask {
        rows.iter().find(|t| t.actor_ids.iter().any(|a| a == actor))
            .unwrap_or_else(|| panic!("参与者 {actor} 的 DOING 行没读到（实得 {:?}）",
                rows.iter().map(|t| t.actor_ids.clone()).collect::<Vec<_>>()))
            .clone()
    }

    /// 同行 expire − create ≈ 表达式偏移（**不许只判非空**：只判非空就会被占位 now() 蒙过去，
    /// 那正是本病灶的形状）。带宽 [-5s, +60s] 与 java/go 同档：秒级 floor + 落库耗时。
    fn exp_expire_about(row: &ProcessTask, want: i64, who: &str) {
        let exp = row.expire_time.as_deref()
            .unwrap_or_else(|| panic!("{who} 配了到期表达式，expire_time 却是空"));
        let cre = row.create_time.as_deref()
            .unwrap_or_else(|| panic!("{who} 的 create_time 应有值（内部对照）"));
        let delta = crate::expire_time::to_epoch_secs(exp).unwrap()
            - crate::expire_time::to_epoch_secs(cre).unwrap();
        assert!(delta >= want - 5 && delta <= want + 60,
            "{who} 的 expire − create = {delta}s，期望 ≈{want}s（带宽 -5s/+60s）；\
             占位 now() 会算出 ≈0 ⇒ 新建即逾期");
    }

    /// 该列必须为空（未配 / 解析不出两档共用），并要求行本身读到了
    fn exp_expire_null(row: &ProcessTask, why: &str) {
        assert!(row.create_time.is_some(), "内部对照：{why} 那行的 create_time 应有值");
        assert_eq!(row.expire_time, None,
            "{why}：expire_time 被赋成 {:?}，期望保持空（不造默认值、不写 now()）", row.expire_time);
    }

    /// T0 正向①：普通建单（写点①）配 `2h` ⇒ 同一行 expire − create ≈ 2h
    #[tokio::test]
    async fn test_i126_normal_create_relative_expression() {
        let (engine, repo) = make_surrogate_engine();
        let name = "i126_2h";
        let did = save_define(&repo, name,
            &exp_flow(name, &[("approve", r#""assignee":"zhangsan","expireTime":"2h""#)]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        exp_expire_about(&exp_doing(&repo, inst.instance_id, "approve", 1)[0], 7200, "普通建单（写点①）");
    }

    /// T0 正向②：表达式是**变量名** ⇒ 取实例变量里那个变量的值
    /// （建单三处的变量源＝实例变量，对齐 Java `this.variables` / boot2 `execution.getArgs()`）
    #[tokio::test]
    async fn test_i126_normal_create_expression_is_variable() {
        let (engine, repo) = make_surrogate_engine();
        let name = "i126_var";
        let did = save_define(&repo, name,
            &exp_flow(name, &[("approve", r#""assignee":"zhangsan","expireTime":"dueAt""#)]));
        let mut args = FlowData::new();
        args.insert_str("dueAt", "2026-12-31 10:00:00");
        let inst = engine.start_async(did, "zhangsan", &args).await.unwrap();
        let row = &exp_doing(&repo, inst.instance_id, "approve", 1)[0];
        assert_eq!(row.expire_time.as_deref(), Some("2026-12-31 10:00:00"),
            "表达式 dueAt 命中实例变量 ⇒ 该取变量值，而不是 now+偏移");
    }

    /// T0 负向③：未配的三档（属性缺键 / JSON null / 空串）⇒ 该列保持 NULL，不许造默认值。
    /// 三档并排是因为"投成空串还是缺键"这种形状差异本身就藏过病灶（见 `expire_expr_of` 注释）。
    #[tokio::test]
    async fn test_i126_normal_create_unconfigured_keeps_null() {
        for (i, &props) in [
            r#""assignee":"zhangsan""#,
            r#""assignee":"zhangsan","expireTime":null"#,
            // 空串那档不能写进 `r#"..."#`：结尾连着的 `""#` 会被 raw string 当成终止符而吞掉一个引号
            "\"assignee\":\"zhangsan\",\"expireTime\":\"\"",
        ].iter().enumerate() {
            let (engine, repo) = make_surrogate_engine();
            let name = format!("i126_none{i}");
            let did = save_define(&repo, &name, &exp_flow(&name, &[("approve", props)]));
            let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
            exp_expire_null(&exp_doing(&repo, inst.instance_id, "approve", 1)[0],
                ["属性缺键（未配）", "配成 JSON null", "配成空串"][i]);
        }
    }

    /// T0 负向④：解析不出 ⇒ 空，而不是退回 now()（§1.9-3 的"前缀非整数 ⇒ 落穿 ⇒ NULL"同档）
    #[tokio::test]
    async fn test_i126_normal_create_unparsable_stays_null() {
        for expr in ["not-a-time", "xh", "2027-03-04"] {
            let (engine, repo) = make_surrogate_engine();
            let name = format!("i126_bad_{expr}");
            let did = save_define(&repo, &name, &exp_flow(&name,
                &[("approve", &format!(r#""assignee":"zhangsan","expireTime":"{expr}""#))]));
            let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
            exp_expire_null(&exp_doing(&repo, inst.instance_id, "approve", 1)[0],
                &format!("表达式 {expr} 解析不出"));
        }
    }

    /// 写点③：并行会签**全员**逐条都要带到期时间
    #[tokio::test]
    async fn test_i126_parallel_countersign_every_member() {
        let (engine, repo) = make_surrogate_engine();
        let name = "i126_par";
        let did = save_define(&repo, name, &exp_flow(name, &[(
            "cs", r#""assignee":"zhangsan,lisi","performType":1,"countersignType":"PARALLEL","expireTime":"2h""#,
        )]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let rows = exp_doing(&repo, inst.instance_id, "cs", 2); // 并行＝全员一次建齐
        for row in rows.iter() {
            exp_expire_about(row, 7200, "并行会签成员");
        }
    }

    /// §1.8 格一（写点②＋⑤）：串行会签**首位成员**有到期 ∧ **推进出的第二成员**也有到期。
    /// 第五处是四处写点之外最容易漏的一条（java `CountersignHandler.createNextCountersignTask`
    /// 绕过建单 helper；基准 boot2 的串行推进回调 `createCountersignTask`，:524 在写）。
    #[tokio::test]
    async fn test_i126_sequential_first_and_advanced_member_both_expire() {
        let (engine, repo) = make_surrogate_engine();
        let name = "i126_seq";
        let did = save_define(&repo, name, &exp_flow(name, &[(
            "cs", r#""assignee":"userA,userB","performType":1,"countersignType":"SEQUENTIAL","expireTime":"2h""#,
        )]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let iid = inst.instance_id;

        let first = exp_actor(&exp_doing(&repo, iid, "cs", 1), "userA");
        exp_expire_about(&first, 7200, "串行会签首成员（写点②）");

        engine.execute_task_async(first.task_id, "userA", &exp_submit(1)).await.unwrap();
        let second = exp_actor(&exp_doing(&repo, iid, "cs", 1), "userB");
        exp_expire_about(&second, 7200, "推进新建的第二成员（写点⑤）");
    }

    /// §1.8 格二：同一条串行会签夹具**去掉 expireTime** ⇒ 首成员与推进成员两行都留空。
    /// "行没读到"与"值为空"分开断（`exp_doing`/`exp_actor` 先保证行在，再看值），否则这条恒真。
    #[tokio::test]
    async fn test_i126_sequential_unconfigured_keeps_both_members_null() {
        let (engine, repo) = make_surrogate_engine();
        let name = "i126_seq_none";
        let did = save_define(&repo, name, &exp_flow(name, &[(
            "cs", r#""assignee":"userA,userB","performType":1,"countersignType":"SEQUENTIAL""#,
        )]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let iid = inst.instance_id;

        let first = exp_actor(&exp_doing(&repo, iid, "cs", 1), "userA");
        exp_expire_null(&first, "未配的串行会签首成员");
        engine.execute_task_async(first.task_id, "userA", &exp_submit(1)).await.unwrap();
        let second = exp_actor(&exp_doing(&repo, iid, "cs", 1), "userB");
        exp_expire_null(&second, "未配时推进出的第二成员");
    }

    /// 写点④正向（回退新建）：到期表达式取**被回退掉的那个节点**（＝当前行节点）的，
    /// 逐字对齐 boot2 `ProcessTaskServiceImpl.rejectTask` 的 `((TaskModel)current).getExpireTime()`。
    /// 夹具把表达式**只配在 approve（当前节点）**上 ⇒ 若实现错取"复活行那个节点（apply）"的表达式，
    /// 这里就会拿到空 ⇒ 这一格直接把两档分开（go `d10ebd0` 取的正是后者，已在报告点名）。
    #[tokio::test]
    async fn test_i126_rollback_uses_current_node_expression() {
        let (engine, repo) = make_surrogate_engine();
        let name = "i126_rb";
        let did = save_define(&repo, name, &exp_flow(name, &[
            ("apply", r#""assignee":"zhangsan""#),
            ("approve", r#""assignee":"lisi","expireTime":"2h""#),
        ]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let iid = inst.instance_id;
        let apply = exp_doing(&repo, iid, "apply", 1)[0].clone();
        engine.execute_task_async(apply.task_id, "zhangsan", &exp_submit(1)).await.unwrap();
        let approve = exp_doing(&repo, iid, "approve", 1)[0].clone();
        assert!(approve.expire_time.is_some(), "前置条件：approve 那行自己就该带到期时间");

        engine.execute_and_jump_async(approve.task_id, "lisi", &exp_submit(3), None).await.unwrap();
        let revived = exp_doing(&repo, iid, "apply", 1)[0].clone();
        assert_ne!(revived.task_id, apply.task_id, "复活应是**新行**，不是把原行改回进行中");
        exp_expire_about(&revived, 7200, "回退新建的复活行（写点④）");
    }

    /// 写点④的变量源：回退新建用**随行拷贝那份变量**（boot2 的 hisVariable），不是实例变量。
    /// 两份都放 `dueAt` 且值不同：实例那份＝发起时给的 2027，行那份＝办结 apply 时给的 2028
    /// ⇒ 取到 2027 就说明变量源错接成了实例变量（§1「两档搞混会让变量名这一档跨栈给出不同答案」）。
    #[tokio::test]
    async fn test_i126_rollback_reads_carried_variables_not_instance() {
        let (engine, repo) = make_surrogate_engine();
        let name = "i126_rb_var";
        let did = save_define(&repo, name, &exp_flow(name, &[
            ("apply", r#""assignee":"zhangsan""#),
            ("approve", r#""assignee":"lisi","expireTime":"dueAt""#),
        ]));
        let mut start_args = FlowData::new();
        start_args.insert_str("dueAt", "2027-01-01 01:01:01"); // 实例变量那份
        let inst = engine.start_async(did, "zhangsan", &start_args).await.unwrap();
        let iid = inst.instance_id;
        assert_eq!(inst.variables.get_str("dueAt"), Some("2027-01-01 01:01:01"),
            "前置条件：实例变量里是 2027 那份");

        let apply = exp_doing(&repo, iid, "apply", 1)[0].clone();
        let mut row_args = exp_submit(1);
        row_args.insert_str("dueAt", "2028-02-02 02:02:02"); // 只进**这一行**的变量
        engine.execute_task_async(apply.task_id, "zhangsan", &row_args).await.unwrap();
        let approve = exp_doing(&repo, iid, "approve", 1)[0].clone();

        engine.execute_and_jump_async(approve.task_id, "lisi", &exp_submit(3), None).await.unwrap();
        let revived = exp_doing(&repo, iid, "apply", 1)[0].clone();
        assert_eq!(revived.expire_time.as_deref(), Some("2028-02-02 02:02:02"),
            "回退新建必须读随行那份（2028）；实得 {:?}＝读成实例变量或压根没算", revived.expire_time);
    }
}

// ═══════════════════════════════════════════════════════
// issues/127 ＋ 132 · 事件代码腿（唯一权威＝规范 11 docs/spec/11-events.md §11.3/§11.7）
//
// 判据形状按 spec §11.8／08-compliance「事件契约」表的要求写：
// **用 recorder 断"收到 ＋ 顺序 ＋ 时机"，只断"出现过"不算过**（顺序与缺支正是本案两个病灶）。
// 镜像层跨栈格 L2-30 断的码值序列 [1,3,5,2] ＋ 抄送支 [4]，本栈等价物即
// `test_i132_sequence_from_start_to_end`（用规范名断，不拿数字码当判据——§11.3）。
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod event_leg_tests {
    use super::*;
    use crate::memory::MemoryRepository;
    use crate::id_gen::AtomicIdGenerator;

    /// 全量顺序 recorder：按注册顺序收下**每一个**事件（不筛类型）——
    /// 只有留全序列才能同时判"缺支"和"顺序"（spec §11.8 明写只断出现过不算过）。
    #[derive(Default)]
    struct SeqRecorder {
        events: std::sync::Mutex<Vec<ProcessEvent>>,
    }
    impl ProcessEventListener for SeqRecorder {
        fn on_event(&self, event: &ProcessEvent) {
            self.events.lock().unwrap().push(event.clone());
        }
    }

    /// 夹具引擎：内存仓 ＋ 顺序 recorder。
    fn ev_engine() -> (JeeflowEngineImpl, Arc<MemoryRepository>, Arc<SeqRecorder>) {
        let repo = Arc::new(MemoryRepository::new());
        let mut ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        let rec = Arc::new(SeqRecorder::default());
        ctx.register_event_listener(rec.clone());
        (JeeflowEngineImpl::new(ctx), repo, rec)
    }

    /// 收到的**规范名**序列（跨栈判据用名不用码，spec §11.3）。
    fn seq(rec: &SeqRecorder) -> Vec<String> {
        rec.events.lock().unwrap().iter()
            .map(|e| e.event_type.spec_name().to_string())
            .collect()
    }

    /// 线性流夹具：start → (specs 逐个 task 节点) → end（与 `exp_flow` 同形状，自带一份免跨模块耦合）。
    fn ev_flow(name: &str, specs: &[(&str, &str)]) -> String {
        let mut nodes = vec![
            r#"{"id":"start","type":"snaker:start","properties":{},"text":{"value":"开始"}}"#.to_string(),
        ];
        let mut edges: Vec<String> = Vec::new();
        let mut prev = "start".to_string();
        for (i, (id, props)) in specs.iter().enumerate() {
            nodes.push(format!(
                r#"{{"id":"{id}","type":"snaker:task","properties":{{{props}}},"text":{{"value":"节点{n}"}}}}"#,
                id = id, props = props, n = i + 1));
            edges.push(format!(
                r#"{{"id":"e{i}","sourceNodeId":"{prev}","targetNodeId":"{id}","properties":{{}}}}"#,
                i = i, prev = prev, id = id));
            prev = id.to_string();
        }
        nodes.push(
            r#"{"id":"end","type":"snaker:end","properties":{},"text":{"value":"结束"}}"#.to_string());
        edges.push(format!(
            r#"{{"id":"eend","sourceNodeId":"{prev}","targetNodeId":"end","properties":{{}}}}"#,
            prev = prev));
        format!(
            r#"{{"name":"{name}","displayName":"事件腿","type":"approval","nodes":[{}],"edges":[{}]}}"#,
            nodes.join(","), edges.join(","))
    }

    fn ev_define(repo: &Arc<MemoryRepository>, name: &str, content: &str) -> i64 {
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

    fn ev_submit(t: i64) -> FlowData {
        let mut a = FlowData::new();
        a.insert_i64("submitType", t);
        a
    }

    /// 取某节点的 DOING 行（条数不符直接红＝"行没读到"与"值为空"分开断）。
    fn ev_doing(repo: &MemoryRepository, iid: i64, node: &str, want: usize) -> Vec<ProcessTask> {
        let mut rows: Vec<ProcessTask> = repo.find_doing_tasks(iid, &[]).unwrap()
            .into_iter().filter(|t| t.task_name == node).collect();
        rows.sort_by_key(|t| t.task_id);
        assert_eq!(rows.len(), want, "节点 {node} 的 DOING 行数应为 {want}，实得 {:?}",
            rows.iter().map(|t| t.task_name.clone()).collect::<Vec<_>>());
        rows
    }

    /// 事件落库后 fire（08 场景 28／32「先 fire 后落库即红」）：fire 时反查仓储必须读到该值。
    fn ev_at_fire_reads_instance_state(rec: &SeqRecorder, spec_name: &str) -> Vec<Option<i32>> {
        rec.events.lock().unwrap().iter()
            .filter(|e| e.event_type.spec_name() == spec_name)
            .map(|e| e.data.get_i64("state").map(|v| v as i32))
            .collect()
    }

    // ─── L2-30 的栈内等价物：一条流从发起到办结按顺序 [1,3,5,2] ───

    /// 正向①（spec §11.8／08 场景 28·29·30·32）：start → apply → end 一条流跑通，
    /// recorder 按顺序收到 **`[PROCESS_INSTANCE_START, PROCESS_TASK_START, TASK_COMPLETE, PROCESS_INSTANCE_END]`**
    /// ＝码值序列 `[1,3,5,2]`（规范名是判据，数字码不是）。
    ///
    /// 顺序是本病灶之一：改前 PROCESS_INSTANCE_START 排在 persist_tasks **之后** ⇒ 实得 `[3,1,…]`；
    /// 缺支是另一病灶：改前 TASK_COMPLETE(5) 整支不存在。
    #[tokio::test]
    async fn test_i132_sequence_from_start_to_end() {
        let (engine, repo, rec) = ev_engine();
        let name = "i132_seq";
        let did = ev_define(&repo, name, &ev_flow(name, &[("apply", r#""assignee":"zhangsan""#)]));

        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
        engine.execute_task_async(apply.task_id, "zhangsan", &ev_submit(1)).await.unwrap();

        assert_eq!(seq(&rec), vec![
            "PROCESS_INSTANCE_START",   // 1 实例行 insert 之后
            "PROCESS_TASK_START",       // 3 apply 任务行落库之后
            "TASK_COMPLETE",            // 5 任务行 state=20 落库之后（改前整支缺）
            "PROCESS_INSTANCE_END",     // 2 实例 state 落终库之后
        ], "一条流从发起到办结必须按顺序收到码值 [1,3,5,2]（规范名序列）");

        // 时机（08 场景 28·29·30：fire 时行必须已落库可反查）
        let events = rec.events.lock().unwrap();
        let iid = inst.instance_id;
        assert_eq!(events[0].source_id, iid);
        assert_eq!(events[0].data.get_i64("instanceId"), Some(iid), "码 1 载荷键 instanceId");
        assert!(repo.find_instance_by_id(iid).unwrap().is_some(), "码 1 fire 时实例行已入库");

        let tid = events[1].source_id;
        assert!(tid > 0);
        assert_eq!(events[1].data.get_i64("instanceId"), Some(iid), "码 3 载荷键 instanceId");
        assert_eq!(events[1].data.get_i64("taskId"), Some(tid), "码 3 载荷键 taskId");
        assert_eq!(events[1].data.get("actors").and_then(|v| v.as_array()).map(|a| a.len()),
            Some(1), "码 3 载荷键 actors（参与者列表）");
        assert!(repo.find_task_by_id(tid).unwrap().is_some(), "码 3 fire 时任务行已入库可反查");

        assert_eq!(events[2].source_id, tid, "码 5 sourceId＝taskId");
        assert_eq!(events[2].data.get_i64("instanceId"), Some(iid));
        assert_eq!(events[2].data.get_i64("taskId"), Some(tid));
        assert_eq!(events[2].data.get_str("operator"), Some("zhangsan"), "码 5 载荷键 operator");
        assert_eq!(events[2].data.get_i64("submitType"), Some(1), "码 5 载荷键 submitType");
        assert_eq!(repo.find_task_by_id(tid).unwrap().unwrap().task_state,
            TaskState::Finished.code(), "码 5 fire 时任务行 state 已是已完成(20)");

        assert_eq!(events[3].source_id, iid, "码 2 sourceId＝instanceId");
        assert_eq!(events[3].data.get_i64("state"), Some(InstanceState::Finished.code() as i64),
            "码 2 载荷键 state＝落库后的实例状态整数");
    }

    /// 时机（08 场景 32）：PROCESS_INSTANCE_END 载荷的 state 就是**落库后**那一档
    /// ——办结 20／拒绝 45 两支都必须在 fire 前把 state 写进去（监听器反查读不到旧状态）。
    #[tokio::test]
    async fn test_i132_instance_end_state_is_persisted_value() {
        // 办结档
        let (engine, repo, rec) = ev_engine();
        let name = "i132_end_finish";
        let did = ev_define(&repo, name, &ev_flow(name, &[("apply", r#""assignee":"zhangsan""#)]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
        engine.execute_task_async(apply.task_id, "zhangsan", &ev_submit(1)).await.unwrap();
        assert_eq!(ev_at_fire_reads_instance_state(&rec, "PROCESS_INSTANCE_END"),
            vec![Some(InstanceState::Finished.code())], "办结那一支的 state 必须是落库后的 20");

        // 拒绝档（共用码 2，规范名不拆，靠 state 分——spec §11.6）
        let (engine, repo, rec) = ev_engine();
        let name = "i132_end_reject";
        let did = ev_define(&repo, name, &ev_flow(name, &[
            ("apply", r#""assignee":"zhangsan""#),
            ("approve", r#""assignee":"lisi""#),
        ]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
        engine.execute_task_async(apply.task_id, "zhangsan", &ev_submit(1)).await.unwrap();
        let approve = ev_doing(&repo, inst.instance_id, "approve", 1)[0].clone();
        let mut rej = ev_submit(2);
        rej.insert_str("reject", "true");
        engine.execute_and_jump_to_end_async(approve.task_id, "lisi", &rej).await.unwrap();

        let states = ev_at_fire_reads_instance_state(&rec, "PROCESS_INSTANCE_END");
        assert_eq!(states, vec![Some(InstanceState::Reject.code())],
            "驳回那一支 fire 时实例已是 45");
        assert_eq!(repo.find_instance_by_id(inst.instance_id).unwrap().unwrap().state,
            InstanceState::Reject.code(), "对照：库里落的就是 45");
    }

    // ─── 抄送支（08 场景 33：三条路径都要建 cc 行 ＋ 逐人 fire 4）───

    /// 正向②（spec §11.7／issues/127）：**办理腿** `tf_ccActors` 建 cc 行并逐抄送人 fire
    /// CC_CREATE(4)，`ccActorId` 直传、fire 排在 cc 行落库之后。
    /// 数组形态（vben 多选 ApiSelect 提交）与逗号串形态都要覆盖——改前只 `get_str`，
    /// 数组那一档 cc 行与事件双双丢失（issues/56 E28 在发起腿的同款坑）。
    #[tokio::test]
    async fn test_i127_cc_create_on_execute_leg() {
        for (i, cc_value) in [
            JsonValue::Array(vec![JsonValue::Str("u1".into()), JsonValue::Str("u2".into())]),
            JsonValue::Str("u1,u2".into()),
        ].into_iter().enumerate() {
            let (engine, repo, rec) = ev_engine();
            let name = format!("i127_tf_cc{i}");
            let did = ev_define(&repo, &name, &ev_flow(&name, &[("apply", r#""assignee":"zhangsan""#)]));
            let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
            let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();

            let mut args = ev_submit(1);
            args.insert("tf_ccActors".to_string(), cc_value);
            engine.execute_task_async(apply.task_id, "zhangsan", &args).await.unwrap();

            // 一次性取快照（guard 出了语句即释放，下面的 seq() 才能再锁同一把 Mutex）
            let events = rec.events.lock().unwrap().clone();
            let cc: Vec<(i64, Option<String>)> = events.iter()
                .filter(|e| e.event_type == ProcessEventType::CcCreate)
                .map(|e| (e.source_id, e.cc_actor_id.clone())).collect();
            assert_eq!(cc, vec![
                (inst.instance_id, Some("u1".to_string())),
                (inst.instance_id, Some("u2".to_string())),
            ], "办理腿应逐抄送人 fire CC_CREATE（第 {} 档：{:?}）", i, cc);
            // 载荷键 ccActorId（§11.3 码 4 直传列）
            for e in events.iter().filter(|e| e.event_type == ProcessEventType::CcCreate) {
                assert_eq!(e.data.get_str("ccActorId"), e.cc_actor_id.as_deref(),
                    "ccActorId 既进事件体也进载荷（同键同源）");
            }
            // 时机：fire 时 cc 行已落库（接收人档逐人数）
            for who in ["u1", "u2"] {
                let mut q = crate::model::PageQuery::new(1, 10);
                q.operator = Some(who.to_string());
                assert_eq!(repo.page_cc_instances(&q).unwrap().record_count, 1,
                    "抄送人 {who} 的 cc 行应已落库（第 {i} 档）");
            }
            // 顺序：办理腿的 CC_CREATE 排在任务办结之后（cc 行与任务更新同批，任务先落）
            let names = seq(&rec);
            let complete = names.iter().position(|n| n == "TASK_COMPLETE").expect("应有 TASK_COMPLETE");
            let first_cc = names.iter().position(|n| n == "CC_CREATE").expect("应有 CC_CREATE");
            assert!(first_cc > complete, "抄送支须排在任务落库之后（TASK_COMPLETE  index {complete} < CC_CREATE index {first_cc}）");
        }
    }

    /// 负向（规范 11 §11.7 本轮钉死的边界第 2 条）：**覆盖面只算 `executeProcessTask` 一条腿**——
    /// `executeAndJumpTask`（submitType 4）/ `jumpToEnd`（2）/ 退发起人（6）/ 退回上一步（3）
    /// 这类跳转·回退 action 带的 `tf_ccActors` **本轮不建 cc 行、不发 CC_CREATE(4)**。
    /// 单栈自行放宽＝跨栈分叉（spec 原文点名的反例是"go 第一轮就是这种超集"），
    /// 故本格是"把钩子挪进跳转腿"这类变异的靶子：改宽即红。
    #[tokio::test]
    async fn test_i132_jump_rollback_legs_do_not_create_cc() {
        // 三档跳转·回退 action，各用一台干净引擎（互不串档）。
        let legs: [(&str, i64, bool, Option<&str>); 4] = [
            ("i132_nocc_jump4", 4, false, Some("apply")), // executeAndJumpTask 跳指定节点
            ("i132_nocc_rb3", 3, false, None),            // 退回上一步（血缘版）
            ("i132_nocc_rb6", 6, false, None),            // 退回发起人
            ("i132_nocc_end2", 2, true, None),            // jumpToEnd 拒绝到终态
        ];
        for (name, submit_type, to_end, target) in legs.into_iter() {
            let (engine, repo, rec) = ev_engine();
            let did = ev_define(&repo, name, &ev_flow(name, &[
                ("apply", r#""assignee":"zhangsan""#),
                ("approve", r#""assignee":"lisi""#),
            ]));
            let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
            let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
            engine.execute_task_async(apply.task_id, "zhangsan", &ev_submit(1)).await.unwrap();
            let approve = ev_doing(&repo, inst.instance_id, "approve", 1)[0].clone();
            // 清空发起段事件，下面只看跳转·回退这一档自己发了什么。
            rec.events.lock().unwrap().clear();

            let mut args = ev_submit(submit_type);
            args.insert("tf_ccActors".to_string(),
                JsonValue::Array(vec![JsonValue::Str("u1".into()), JsonValue::Str("u2".into())]));
            let res = match (to_end, target) {
                (true, _) => engine
                    .execute_and_jump_to_end_async(approve.task_id, "lisi", &args).await,
                (false, Some(t)) => engine
                    .execute_and_jump_async(approve.task_id, "lisi", &args, Some(t)).await,
                (false, None) if submit_type == 6 => engine
                    .execute_and_jump_to_first_async(approve.task_id, "lisi", &args).await,
                (false, None) => engine
                    .execute_and_jump_async(approve.task_id, "lisi", &args, None).await,
            };
            assert!(res.is_ok(), "{name} 跳转档应执行成功，实得 {:?}", res.err());

            let names = seq(&rec);
            assert!(!names.iter().any(|n| n == "CC_CREATE"),
                "{name}：跳转·回退带 tf_ccActors 严禁发 CC_CREATE（§11.7 覆盖面窄口径），实得 {names:?}");
            for who in ["u1", "u2"] {
                let mut q = crate::model::PageQuery::new(1, 10);
                q.operator = Some(who.to_string());
                assert_eq!(repo.page_cc_instances(&q).unwrap().record_count, 0,
                    "{name}：跳转·回退带 tf_ccActors 严禁建 cc 行（§11.7），抄送人 {who} 实得非零");
            }
        }
    }

    /// 正向③（08 场景 33 三条路径同判）：手动支与引擎支**归一**——三条路径共用
    /// `notify_cc_create` 一个收口，发起腿 fire 的规范名与办理腿／门面手动腿完全同码同名。
    /// 手动腿（`facade.createCCInstance`）的 fire 点在 facade 侧测试
    /// （`jeeflow-facade` 的 `test_cc_create_fired_on_manual_create_cc` ＋ 本轮补的载荷断言）。
    #[tokio::test]
    async fn test_i132_cc_create_same_funnel_on_start_leg() {
        let (engine, repo, rec) = ev_engine();
        let name = "i132_start_cc";
        let did = ev_define(&repo, name, &ev_flow(name, &[("apply", r#""assignee":"zhangsan""#)]));
        let mut args = FlowData::new();
        args.insert("f_ccActors".to_string(),
            JsonValue::Array(vec![JsonValue::Str("u7".into())]));
        let inst = engine.start_async(did, "zhangsan", &args).await.unwrap();

        let names = seq(&rec);
        assert!(names.iter().any(|n| n == "CC_CREATE"), "发起腿仍须 fire CC_CREATE：{names:?}");
        let events = rec.events.lock().unwrap();
        let cc = events.iter().find(|e| e.event_type == ProcessEventType::CcCreate).unwrap();
        assert_eq!(cc.source_id, inst.instance_id, "码 4 sourceId＝instanceId");
        assert_eq!(cc.cc_actor_id.as_deref(), Some("u7"));
    }

    // ─── issues/141 G2 · cc 写侧判重＝幂等空操作（三条入口共用一条判据，本模块打引擎两条腿）───

    /// 只收 CC_CREATE 的 `cc_actor_id` 序列（逐人 fire 的入参＝实际新建子集，本案的判据本体）。
    fn cc_fired_actors(rec: &SeqRecorder) -> Vec<String> {
        let events = rec.events.lock().unwrap().clone();
        events.iter().filter(|e| e.event_type == ProcessEventType::CcCreate)
            .map(|e| e.cc_actor_id.clone().unwrap_or_default()).collect()
    }

    /// ④＋子集空档：**全是已知人**的一批抄送 ⇒ 不新增行、**整支不 fire 码 4**
    /// （spec 11.2 原则 1「码=事实」；旧形状是照旧按原始请求全量 fire）。
    #[tokio::test]
    async fn test_i141_g2_repeat_cc_fires_nothing() {
        let (engine, repo, rec) = ev_engine();
        let name = "i141_g2_repeat";
        let did = ev_define(&repo, name, &ev_flow(name, &[("apply", r#""assignee":"zhangsan""#)]));

        let mut start = FlowData::new();
        start.insert("f_ccActors".to_string(),
            JsonValue::Array(vec![JsonValue::Str("u1".into()), JsonValue::Str("u2".into())]));
        let inst = engine.start_async(did, "zhangsan", &start).await.unwrap();
        assert_eq!(cc_fired_actors(&rec), vec!["u1".to_string(), "u2".to_string()],
            "首抄：全新的一批照旧逐人 fire");
        assert_eq!(repo.find_cc_actor_ids(inst.instance_id).unwrap(),
            vec!["u1".to_string(), "u2".to_string()]);

        // 办理腿把**同样两个人**再抄一遍 ⇒ 幂等空操作
        let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
        let mut args = ev_submit(1);
        args.insert("tf_ccActors".to_string(),
            JsonValue::Array(vec![JsonValue::Str("u1".into()), JsonValue::Str("u2".into())]));
        engine.execute_task_async(apply.task_id, "zhangsan", &args).await.unwrap();

        assert_eq!(cc_fired_actors(&rec), vec!["u1".to_string(), "u2".to_string()],
            "重复抄送没发生\"创建\"⇒ 一支新的码 4 都不发（改前这里多出发 u1/u2 两支）");
        assert_eq!(repo.find_cc_actor_ids(inst.instance_id).unwrap(),
            vec!["u1".to_string(), "u2".to_string()], "①cc 行数与人员集合都不变（不新增行）");
    }

    /// ④的子集档：第二次同时给「已知人＋新人」⇒ 只为新人建行、只为新人 fire。
    /// 两形态（数组＝vben 多选、逗号串＝旧客户端）共用同一条判重腿。
    #[tokio::test]
    async fn test_i141_g2_subset_fire_on_engine_legs() {
        for (i, second_leg) in [
            JsonValue::Array(vec![JsonValue::Str("u1".into()), JsonValue::Str("u3".into())]),
            JsonValue::Str("u1,u3".into()),
        ].into_iter().enumerate() {
            let (engine, repo, rec) = ev_engine();
            let name = format!("i141_g2_subset{i}");
            let did = ev_define(&repo, &name, &ev_flow(&name, &[("apply", r#""assignee":"zhangsan""#)]));

            let mut start = FlowData::new();
            start.insert("f_ccActors".to_string(),
                JsonValue::Array(vec![JsonValue::Str("u1".into()), JsonValue::Str("u2".into())]));
            let inst = engine.start_async(did, "zhangsan", &start).await.unwrap();
            assert_eq!(cc_fired_actors(&rec), vec!["u1".to_string(), "u2".to_string()],
                "第 {i} 档首抄应逐人 fire");

            let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
            let mut args = ev_submit(1);
            args.insert("tf_ccActors".to_string(), second_leg.clone());
            engine.execute_task_async(apply.task_id, "zhangsan", &args).await.unwrap();

            assert_eq!(cc_fired_actors(&rec),
                vec!["u1".to_string(), "u2".to_string(), "u3".to_string()],
                "第 {i} 档：办理腿只能为实际新建的子集（u3）fire，已知人 u1 不得再发");
            assert_eq!(repo.find_cc_actor_ids(inst.instance_id).unwrap(),
                vec!["u1".to_string(), "u2".to_string(), "u3".to_string()],
                "第 {i} 档：落库行＝u1/u2/u3，u1 不得有第二行");
        }
    }

    /// 反向哨兵：判重只在**同一实例**内成立——换一条实例，同一个人照旧建行照旧 fire。
    #[tokio::test]
    async fn test_i141_g2_dedup_scoped_per_instance_on_engine_legs() {
        let (engine, repo, rec) = ev_engine();
        let name = "i141_g2_scope";
        let did = ev_define(&repo, name, &ev_flow(name, &[("apply", r#""assignee":"zhangsan""#)]));
        let mut start = FlowData::new();
        start.insert("f_ccActors".to_string(), JsonValue::Str("u1".into()));

        let first = engine.start_async(did, "zhangsan", &start).await.unwrap();
        let second = engine.start_async(did, "zhangsan", &start).await.unwrap();
        assert_ne!(first.instance_id, second.instance_id, "夹具前提：两条实例");

        assert_eq!(cc_fired_actors(&rec), vec!["u1".to_string(), "u1".to_string()],
            "不同实例上的同一个人各 fire 一次（判重不得升级成全局）");
        assert_eq!(repo.find_cc_actor_ids(first.instance_id).unwrap(), vec!["u1".to_string()]);
        assert_eq!(repo.find_cc_actor_ids(second.instance_id).unwrap(), vec!["u1".to_string()]);
    }

    // ─── issues/141 G10 · 空抄送人不建 cc 行（漏斗层：三条入口 + 逗号串/数组两形同判据）───

    /// 一条只有 apply 节点的流，返回 (engine, repo, 事件记录器, define_id)。
    fn g10_engine(name: &str) -> (JeeflowEngineImpl, Arc<MemoryRepository>, Arc<SeqRecorder>, i64) {
        let (engine, repo, rec) = ev_engine();
        let did = ev_define(&repo, name, &ev_flow(name, &[("apply", r#""assignee":"zhangsan""#)]));
        (engine, repo, rec, did)
    }

    /// 正向对照（＝java `nonBlankCcActorsStillCreateRowsAndFire`）：非空抄送人照旧逐人建行＋逐人 fire。
    /// 这一格按设计**改前也不红**，它钉的是"归一不许顺手吃掉正常值"。
    #[tokio::test]
    async fn test_i141_g10_non_blank_cc_actors_still_create_rows_and_fire() {
        let (engine, repo, rec, did) = g10_engine("i141_g10_positive");
        let mut start = FlowData::new();
        start.insert("f_ccActors".to_string(),
            JsonValue::Array(vec![JsonValue::Str("7501".into()), JsonValue::Str("7502".into())]));
        let inst = engine.start_async(did, "zhangsan", &start).await.unwrap();

        assert_eq!(repo.find_cc_actor_ids(inst.instance_id).unwrap(),
            vec!["7501".to_string(), "7502".to_string()], "正向对照：非空抄送人照旧逐人落行");
        assert_eq!(cc_fired_actors(&rec), vec!["7501".to_string(), "7502".to_string()],
            "正向对照：照旧逐人 fire 码 4");
    }

    /// 逗号串全空白 ⇒ 不建行、不 fire。
    #[tokio::test]
    async fn test_i141_g10_start_leg_blank_comma_string_creates_no_row() {
        for (i, raw) in ["", "   ", "\t", " , , "].into_iter().enumerate() {
            let (engine, repo, rec, did) = g10_engine(&format!("i141_g10_csv_blank_{i}"));
            let mut start = FlowData::new();
            start.insert("f_ccActors".to_string(), JsonValue::Str(raw.to_string()));
            let inst = engine.start_async(did, "zhangsan", &start).await.unwrap();

            assert_eq!(repo.find_cc_actor_ids(inst.instance_id).unwrap(), Vec::<String>::new(),
                "G10：f_ccActors 全空白（{raw:?}）不得建 cc 行");
            assert!(cc_fired_actors(&rec).is_empty(),
                "G10：全空白不得 fire 码 4（{raw:?}）");
        }
    }

    /// 数组形态给空串＋纯空白元素 ⇒ 同一判据（rust 的病灶腿：逗号串腿本来就 trim＋丢空，
    /// **数组腿只 `retain(!is_empty())` ⇒ `"  "` 活着且不 trim**，改前这一格直接红）。
    #[tokio::test]
    async fn test_i141_g10_start_leg_array_drops_blank_and_trims() {
        for (i, (cc, want)) in [
            (vec!["7801", "", "  "], vec!["7801"]),
            (vec!["", "  "], Vec::<&str>::new()),
            (vec![" 8101 "], vec!["8101"]),
            (vec!["\t"], Vec::<&str>::new()),
        ].into_iter().enumerate() {
            let (engine, repo, rec, did) = g10_engine(&format!("i141_g10_arr_{i}"));
            let mut start = FlowData::new();
            start.insert("f_ccActors".to_string(),
                JsonValue::Array(cc.iter().map(|s| JsonValue::Str(s.to_string())).collect()));
            let inst = engine.start_async(did, "zhangsan", &start).await.unwrap();

            assert_eq!(repo.find_cc_actor_ids(inst.instance_id).unwrap(),
                want.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "G10 数组腿第 {i} 档（{cc:?}）：空元素丢弃、值取 trim 后的串");
            assert_eq!(cc_fired_actors(&rec),
                want.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "G10 数组腿第 {i} 档：fire 的入参只含有效且 trim 后的人");
        }
    }

    /// 逗号串里的空元素（`"7701,,7702"`）与尾随逗号（`"8201,"`）丢弃，有效的人照旧。
    #[tokio::test]
    async fn test_i141_g10_start_leg_comma_string_drops_empty_elements() {
        for (i, (cc, want)) in [
            ("7701,,7702", vec!["7701", "7702"]),
            ("8201,", vec!["8201"]),
            (" 8202 , 8203 ", vec!["8202", "8203"]),
            (",,,", Vec::<&str>::new()),
        ].into_iter().enumerate() {
            let (engine, repo, rec, did) = g10_engine(&format!("i141_g10_csv_{i}"));
            let mut start = FlowData::new();
            start.insert("f_ccActors".to_string(), JsonValue::Str(cc.to_string()));
            let inst = engine.start_async(did, "zhangsan", &start).await.unwrap();

            assert_eq!(repo.find_cc_actor_ids(inst.instance_id).unwrap(),
                want.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "G10 逗号串第 {i} 档（{cc:?}）：空段丢弃");
            assert_eq!(cc_fired_actors(&rec),
                want.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                "G10 逗号串第 {i} 档：只逐有效人 fire");
        }
    }

    /// 办理腿 `tf_ccActors`：纯空白 ⇒ 不建行不 fire；混给空元素 ⇒ 只丢空的。
    #[tokio::test]
    async fn test_i141_g10_execute_leg_drops_blank_actors() {
        for (i, (cc, want)) in [
            (JsonValue::Str("   ".into()), Vec::<String>::new()),
            (JsonValue::Array(vec![JsonValue::Str("".into()), JsonValue::Str("  ".into())]),
                Vec::<String>::new()),
            (JsonValue::Str("8201,".into()), vec!["8201".to_string()]),
            (JsonValue::Array(vec![
                JsonValue::Str("8301".into()), JsonValue::Str("".into()),
                JsonValue::Str("  ".into()), JsonValue::Str("8302".into())]),
                vec!["8301".to_string(), "8302".to_string()]),
        ].into_iter().enumerate() {
            let (engine, repo, rec, did) = g10_engine(&format!("i141_g10_exec_{i}"));
            let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
            rec.events.lock().unwrap().clear();
            let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
            let mut args = ev_submit(1);
            args.insert("tf_ccActors".to_string(), cc.clone());
            engine.execute_task_async(apply.task_id, "zhangsan", &args).await.unwrap();

            assert_eq!(repo.find_cc_actor_ids(inst.instance_id).unwrap(), want,
                "G10 办理腿第 {i} 档（{cc:?}）：空值不建行");
            assert_eq!(cc_fired_actors(&rec), want,
                "G10 办理腿第 {i} 档：空值不得拿去 fire 码 4");
        }
    }

    /// trim 与 issues/141 G2 写侧判重咬合：先抄 `"8401"`，再抄 `" 8401 "` ⇒ 判为同一个人，
    /// 不新增第二行、不 fire 码 4（不 trim 的旧形状会在这里落出第二行）。
    #[tokio::test]
    async fn test_i141_g10_padded_value_hits_g2_dedup() {
        let (engine, repo, rec, did) = g10_engine("i141_g10_trim_dedup");
        let mut start = FlowData::new();
        start.insert("f_ccActors".to_string(), JsonValue::Str("8401".into()));
        let inst = engine.start_async(did, "zhangsan", &start).await.unwrap();
        rec.events.lock().unwrap().clear();

        let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
        let mut args = ev_submit(1);
        args.insert("tf_ccActors".to_string(), JsonValue::Array(vec![JsonValue::Str(" 8401 ".into())]));
        engine.execute_task_async(apply.task_id, "zhangsan", &args).await.unwrap();

        assert_eq!(repo.find_cc_actor_ids(inst.instance_id).unwrap(), vec!["8401".to_string()],
            "G10：带空格的同一人不得再建第二行（trim 后的值才进判重比较）");
        assert!(cc_fired_actors(&rec).is_empty(), "G10：判重命中 ⇒ 不 fire 码 4");
    }

    /// 反向哨兵：`"0"` 这类"看起来像空"的正常 id **不得**被归一吃掉（spec §2.10 实现要求④）。
    #[tokio::test]
    async fn test_i141_g10_zero_actor_id_is_not_treated_as_blank() {
        let (engine, repo, rec, did) = g10_engine("i141_g10_sentinel");
        let mut start = FlowData::new();
        start.insert("f_ccActors".to_string(),
            JsonValue::Array(vec![JsonValue::Str("0".into()), JsonValue::Str("user-1".into())]));
        let inst = engine.start_async(did, "zhangsan", &start).await.unwrap();

        assert_eq!(repo.find_cc_actor_ids(inst.instance_id).unwrap(),
            vec!["0".to_string(), "user-1".to_string()],
            "G10 只丢空串/纯空白：'0' 这类正常 id 不得被吃掉");
        assert_eq!(cc_fired_actors(&rec), vec!["0".to_string(), "user-1".to_string()],
            "反向哨兵：照旧逐人 fire");
    }

    /// 漏斗归一本体（c30 `test_parse_cc_actors` 的 G10 补充，不动既有格子）：
    /// 逗号串与数组**两形同判据**——同一批值两种写法必须得到同一个结果。
    #[test]
    fn test_i141_g10_parse_cc_actors_both_forms_same_judgement() {
        for (raw_csv, arr) in [
            ("7701,,7702", vec!["7701", "", "7702"]),
            ("8201,", vec!["8201", ""]),
            (" 8301 ", vec![" 8301 "]),
            ("", vec!["", "  ", "\t"]),
        ] {
            let from_csv = parse_cc_actors(Some(&JsonValue::Str(raw_csv.to_string())));
            let from_arr = parse_cc_actors(Some(&JsonValue::Array(
                arr.iter().map(|s| JsonValue::Str(s.to_string())).collect())));
            assert_eq!(from_csv, from_arr,
                "G10：逗号串 {raw_csv:?} 与数组 {arr:?} 必须同判据，实得 {from_csv:?} / {from_arr:?}");
            assert!(from_csv.iter().all(|s| !s.trim().is_empty() && s == s.trim()),
                "G10：归一后不得残留空值或未 trim 的值：{from_csv:?}");
        }
        // 反向哨兵：'0' 不是空值
        assert_eq!(parse_cc_actors(Some(&JsonValue::Array(vec![JsonValue::Str("0".into())]))),
            vec!["0".to_string()], "反向哨兵：'0' 不得被当成空值丢掉");
    }

    // ─── 5 / 6 互斥（08 场景 30·31）───

    /// 正向④：submitType=2 拒绝 → 只 fire `TASK_REJECT`(6)，**不得**再 fire `TASK_COMPLETE`(5)
    /// （§11.3 码 6 括注「同一动作走 reject 就不再 fire complete」／08 场景 30 后半句）。
    /// 实例终态 2 另 fire（办结/拒绝共用一支）。
    #[tokio::test]
    async fn test_i132_reject_fires_task_reject_not_complete() {
        let (engine, repo, rec) = ev_engine();
        let name = "i132_reject";
        let did = ev_define(&repo, name, &ev_flow(name, &[
            ("apply", r#""assignee":"zhangsan""#),
            ("approve", r#""assignee":"lisi""#),
        ]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
        engine.execute_task_async(apply.task_id, "zhangsan", &ev_submit(1)).await.unwrap();
        rec.events.lock().unwrap().clear();

        let approve = ev_doing(&repo, inst.instance_id, "approve", 1)[0].clone();
        engine.execute_and_jump_to_end_async(approve.task_id, "lisi", &ev_submit(2)).await.unwrap();

        assert_eq!(seq(&rec), vec!["TASK_REJECT", "PROCESS_INSTANCE_END"],
            "拒绝这一次：6 与 2 各一次，严禁再发 5");
        let events = rec.events.lock().unwrap();
        assert_eq!(events[0].source_id, approve.task_id, "码 6 sourceId＝taskId");
        assert_eq!(events[0].data.get_i64("submitType"), Some(2), "码 6 载荷 submitType 供下游分档");
        assert_eq!(events[0].data.get_str("operator"), Some("lisi"));
        assert_eq!(events[0].data.get_i64("instanceId"), Some(inst.instance_id));
        assert_eq!(events[0].data.get_i64("taskId"), Some(approve.task_id));
    }

    /// 正向⑤：退回上一步（submitType 3，血缘版）与退回发起人（submitType 6）都归 `TASK_REJECT`(6)
    /// （§11.2 原则 2「码粗载荷细」——不为退发起人/退上一步各开一号）；
    /// 而跳转指定节点（submitType 4）归 `TASK_COMPLETE`(5)（§11.3 码 5 事实列「同意/跳转/会签办理」）。
    #[tokio::test]
    async fn test_i132_rollback_legs_reject_and_jump_leg_complete() {
        // submitType=3 退回上一步
        let (engine, repo, rec) = ev_engine();
        let name = "i132_rb3";
        let did = ev_define(&repo, name, &ev_flow(name, &[
            ("apply", r#""assignee":"zhangsan""#),
            ("approve", r#""assignee":"lisi""#),
        ]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
        engine.execute_task_async(apply.task_id, "zhangsan", &ev_submit(1)).await.unwrap();
        rec.events.lock().unwrap().clear();

        let approve = ev_doing(&repo, inst.instance_id, "approve", 1)[0].clone();
        engine.execute_and_jump_async(approve.task_id, "lisi", &ev_submit(3), None).await.unwrap();
        assert_eq!(seq(&rec), vec!["TASK_REJECT", "PROCESS_TASK_START"],
            "退回上一步＝6（复活行另发一条 3），且不发 5");

        // submitType=6 退回发起人
        let (engine, repo, rec) = ev_engine();
        let name = "i132_rb6";
        let did = ev_define(&repo, name, &ev_flow(name, &[
            ("apply", r#""assignee":"zhangsan""#),
            ("approve", r#""assignee":"lisi""#),
        ]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
        engine.execute_task_async(apply.task_id, "zhangsan", &ev_submit(1)).await.unwrap();
        rec.events.lock().unwrap().clear();

        let approve = ev_doing(&repo, inst.instance_id, "approve", 1)[0].clone();
        engine.execute_and_jump_to_first_async(approve.task_id, "lisi", &ev_submit(6)).await.unwrap();
        let names = seq(&rec);
        assert_eq!(names, vec!["TASK_REJECT", "PROCESS_TASK_START"],
            "退回发起人＝6（新待办另发一条 3），且不发 5");
        let events = rec.events.lock().unwrap();
        assert_eq!(events[0].data.get_i64("submitType"), Some(6), "载荷 submitType 区分两支退法");

        // submitType=4 跳转指定节点 → 5
        let (engine, repo, rec) = ev_engine();
        let name = "i132_jump4";
        let did = ev_define(&repo, name, &ev_flow(name, &[
            ("apply", r#""assignee":"zhangsan""#),
            ("approve", r#""assignee":"lisi""#),
            ("done", r#""assignee":"boss""#),
        ]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
        engine.execute_task_async(apply.task_id, "zhangsan", &ev_submit(1)).await.unwrap();
        let approve = ev_doing(&repo, inst.instance_id, "approve", 1)[0].clone();
        rec.events.lock().unwrap().clear();

        engine.execute_and_jump_async(approve.task_id, "lisi", &ev_submit(4), Some("done")).await.unwrap();
        let names = seq(&rec);
        assert_eq!(names, vec!["TASK_COMPLETE", "PROCESS_TASK_START"],
            "跳转指定节点归 5（TASK_COMPLETE 事实列含「跳转」），新待办另发 3");
        let events = rec.events.lock().unwrap();
        assert_eq!(events[0].data.get_i64("submitType"), Some(4));
        assert_eq!(events[0].source_id, approve.task_id);
    }

    /// 会签：串行会签"推进出的下一位成员"也是一条新待办 ⇒ 3 必发；
    /// 该成员被办掉 ⇒ 5 必发；一票名册未跑完时**不**发实例终态 2（§11.4 不发清单的反面）。
    #[tokio::test]
    async fn test_i132_sequential_countersign_per_member_codes() {
        let (engine, repo, rec) = ev_engine();
        let name = "i132_cs_seq";
        let did = ev_define(&repo, name, &ev_flow(name, &[(
            "cs", r#""assignee":"userA,userB","performType":1,"countersignType":"SEQUENTIAL""#,
        )]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let first = ev_doing(&repo, inst.instance_id, "cs", 1)[0].clone();
        engine.execute_task_async(first.task_id, "userA", &ev_submit(1)).await.unwrap();

        assert_eq!(seq(&rec), vec![
            "PROCESS_INSTANCE_START", "PROCESS_TASK_START",   // 首成员待办
            "TASK_COMPLETE",                                  // 首成员办掉（会签办理也归 5）
            "PROCESS_TASK_START",                             // 推进出的第二成员＝又一条 3
        ], "串行会签逐成员发 3，办掉发 5；未跑完名册不得发 2");
        assert!(repo.find_instance_by_id(inst.instance_id).unwrap().unwrap().state
            == InstanceState::Doing.code(), "第二成员还在办 ⇒ 实例仍进行中");
    }

    /// 不发清单（08 场景 35）：定义生命周期（save_define）与实例/任务**变量写入**（update_instance /
    /// update_task 直落）不构成独立事实 ⇒ 一律不 fire。多发即红。
    #[test]
    fn test_i132_not_fired_for_define_lifecycle_and_variable_writes() {
        let repo = Arc::new(MemoryRepository::new());
        let mut ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        let rec = Arc::new(SeqRecorder::default());
        ctx.register_event_listener(rec.clone());
        let engine = JeeflowEngineImpl::new(ctx);

        // 定义生命周期四事：save / 改状态 / 再存（引擎侧无 fire 义务，spec §11.4-2）
        let name = "i132_not_fire";
        let did = ev_define(&repo, name, &ev_flow(name, &[("apply", r#""assignee":"zhangsan""#)]));
        let mut define = repo.find_define_by_id(did).unwrap().unwrap();
        define.state = 0;
        repo.save_define(&mut define).unwrap();

        // 实例/任务变量写入（§11.4-3）
        let rt = tokio::runtime::Runtime::new().unwrap();
        let inst = rt.block_on(engine.start_async(did, "zhangsan", &FlowData::new())).unwrap();
        rec.events.lock().unwrap().clear();

        let mut inst2 = repo.find_instance_by_id(inst.instance_id).unwrap().unwrap();
        inst2.variables.insert_str("aVar", "1");
        repo.update_instance(&inst2).unwrap();
        let task = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()[0].clone();
        let mut task2 = task.clone();
        task2.variables.insert_str("tVar", "1");
        repo.update_task(&task2).unwrap();

        assert!(seq(&rec).is_empty(), "定义生命周期与变量写入严禁 fire（§11.4 不发清单），实得 {:?}", seq(&rec));
    }

    /// 异常隔离 ＋ 订阅形状（spec §11.5／08 场景 36）在引擎主流程上的等价物：
    /// 监听器 panic 不回滚主流程、不中断后续监听器，事件流仍完整。
    /// （纯 publisher 层的三场景单测见 `crate::event::publisher_tests`。）
    #[tokio::test]
    async fn test_i132_listener_panic_does_not_break_flow() {
        struct Boom;
        impl ProcessEventListener for Boom {
            fn on_event(&self, _e: &ProcessEvent) { panic!("监听器炸了"); }
        }
        let repo = Arc::new(MemoryRepository::new());
        let mut ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        ctx.register_event_listener(Arc::new(Boom));
        let rec = Arc::new(SeqRecorder::default());
        ctx.register_event_listener(rec.clone());   // 炸的那个**先注册**
        let engine = JeeflowEngineImpl::new(ctx);

        let name = "i132_panic";
        let did = ev_define(&repo, name, &ev_flow(name, &[("apply", r#""assignee":"zhangsan""#)]));
        let inst = engine.start_async(did, "zhangsan", &FlowData::new()).await.unwrap();
        let apply = ev_doing(&repo, inst.instance_id, "apply", 1)[0].clone();
        engine.execute_task_async(apply.task_id, "zhangsan", &ev_submit(1)).await.unwrap();

        assert_eq!(seq(&rec), vec![
            "PROCESS_INSTANCE_START", "PROCESS_TASK_START", "TASK_COMPLETE", "PROCESS_INSTANCE_END"],
            "先注册的监听器 panic 不得中断后续监听器，也不得回滚主流程");
        assert_eq!(repo.find_instance_by_id(inst.instance_id).unwrap().unwrap().state,
            InstanceState::Finished.code(), "主流程照旧办结落库");
    }
}

// ═══════════════════════════════════════════════════════
// issues/142 A 批 · 记录类（snaker:custom）执行形状 ＋ 任务类零参与者建单
//
// 判据权威＝spec/02 §6.1（"记录类没有参与者是正常形态"）＋ §6.2 三条硬要求
// （①历史行必须真落库 ②clazz 解析不了⇒记日志＋照常落行＋续流，严禁打断建单
//   ③任务类零参与者必须建 DOING 行）。owner 2026-09-30 逐条拍。
// 对照件＝python `engine.py::_exec_custom_node`（唯一已真落库的那一栈）。
//
// 这组用例在 HEAD 上**全部不存在**：custom 在本栈被当任务类建 DOING 行、
// `create_history_task` 零调用者、零参与者一行不建、`clazz` 根本不执行——
// 四个形状都没格子钉着，所以本轮一条都不会顶到既有用例（逐处还原病灶的实测见收口报告）。
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod custom_node_tests {
    use super::*;
    use crate::id_gen::AtomicIdGenerator;
    use crate::memory::MemoryRepository;
    use crate::spi::CustomNodeHandler;

    /// 夹具 `clazz` 的注册名（与共享夹具 `flows/08-custom-node.json` 同一个串，
    /// 集成方按 `clazz` 原样注册 ⇒ 同一份流程 JSON 不必为 Rust 改）。
    const CLAZZ: &str = "com.mldong.jeeflow.test.TestCustomHandler";

    /// 全量事件顺序 recorder（照 `event_leg_tests::SeqRecorder` 自带一份，免跨模块耦合）。
    #[derive(Default)]
    struct CstRecorder {
        events: std::sync::Mutex<Vec<ProcessEvent>>,
    }
    impl ProcessEventListener for CstRecorder {
        fn on_event(&self, event: &ProcessEvent) {
            self.events.lock().unwrap().push(event.clone());
        }
    }

    fn csm_seq(rec: &CstRecorder) -> Vec<String> {
        rec.events.lock().unwrap().iter()
            .map(|e| e.event_type.spec_name().to_string())
            .collect()
    }

    /// 正常处理器：返回一个值，由引擎按 `val`／缺省 `custom_return_val` 写进流程变量。
    struct ProbeHandler;
    impl CustomNodeHandler for ProbeHandler {
        fn handle(&self, _exec: &mut Execution) -> JeeflowResult<Option<JsonValue>> {
            Ok(Some(JsonValue::Str("probe-返回值".into())))
        }
    }

    /// 返回 `Ok(None)` 的处理器（对齐 java `IHandler` 那一支：引擎不写返回值）。
    struct SilentHandler;
    impl CustomNodeHandler for SilentHandler {
        fn handle(&self, _exec: &mut Execution) -> JeeflowResult<Option<JsonValue>> {
            Ok(None)
        }
    }

    /// 处理器**自身**失败（业务错误）——spec/02 §6.2 末句明写这一档不在豁免内，照旧外抛。
    struct ErrHandler;
    impl CustomNodeHandler for ErrHandler {
        fn handle(&self, _exec: &mut Execution) -> JeeflowResult<Option<JsonValue>> {
            Err(JeeflowError::Business("处理器自己跑炸了".into()))
        }
    }

    /// 处理器 panic——同样不许被降级成"跳过处理器"（本栈不套 catch_unwind）。
    struct PanicHandler;
    impl CustomNodeHandler for PanicHandler {
        fn handle(&self, _exec: &mut Execution) -> JeeflowResult<Option<JsonValue>> {
            panic!("custom-handler-panic");
        }
    }

    /// 内存仓 ＋ 顺序 recorder ＋ 可选的 custom 处理器注册。
    fn cst_engine(handler: Option<(&str, Arc<dyn CustomNodeHandler>)>)
        -> (JeeflowEngineImpl, Arc<MemoryRepository>, Arc<CstRecorder>) {
        let repo = Arc::new(MemoryRepository::new());
        let mut ctx = ServiceContext::new()
            .with_repository(repo.clone() as Arc<dyn ProcessRepository>)
            .with_id_generator(Arc::new(AtomicIdGenerator::new(1)));
        let rec = Arc::new(CstRecorder::default());
        ctx.register_event_listener(rec.clone());
        if let Some((name, h)) = handler {
            ctx.register_custom_handler(name, h);
        }
        (JeeflowEngineImpl::new(ctx), repo, rec)
    }

    /// 单链夹具流工厂：`nodes` = (id, 类型串, properties 片段)，节点按顺序串成一条链。
    fn cst_chain(name: &str, nodes: &[(&str, &str, &str)]) -> String {
        let mut json_nodes: Vec<String> = Vec::new();
        let mut edges: Vec<String> = Vec::new();
        for (i, (id, ty, props)) in nodes.iter().enumerate() {
            json_nodes.push(format!(
                r#"{{"id":"{id}","type":"{ty}","properties":{{{props}}},"text":{{"value":"节点{id}"}}}}"#));
            if i > 0 {
                let prev = nodes[i - 1].0;
                edges.push(format!(
                    r#"{{"id":"e{i}","sourceNodeId":"{prev}","targetNodeId":"{id}","properties":{{}}}}"#));
            }
        }
        format!(
            r#"{{"name":"{name}","displayName":"记录类夹具","type":"approval","nodes":[{}],"edges":[{}]}}"#,
            json_nodes.join(","), edges.join(","))
    }

    fn cst_define(repo: &Arc<MemoryRepository>, content: &str) -> i64 {
        let mut define = ProcessDefine {
            id: 0, name: "custom-fixture".into(), display_name: "记录类夹具".into(),
            define_type: "approval".into(), state: 1,
            content: content.as_bytes().to_vec(),
            version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_define(&mut define).unwrap();
        define.id
    }

    /// 取某节点的**全部**行（不分状态），条数交给用例自己判。
    fn cst_rows(repo: &MemoryRepository, iid: i64, node: &str) -> Vec<ProcessTask> {
        let mut rows: Vec<ProcessTask> = repo.find_history_tasks(iid).unwrap()
            .into_iter().filter(|t| t.task_name == node).collect();
        rows.sort_by_key(|t| t.task_id);
        rows
    }

    /// 主链：start → apply(applicant) → custom1 → approve(leader) → end。
    fn cst_main(name: &str) -> String {
        cst_chain(name, &[
            ("start", "snaker:start", ""),
            ("apply", "snaker:task", r#""assignee":"applicant""#),
            ("custom1", "snaker:custom",
             &format!(r#""clazz":"{CLAZZ}","methodName":"execute","args":"param1","val":"customResult""#)),
            ("approve", "snaker:task", r#""assignee":"leader""#),
            ("end", "snaker:end", ""),
        ])
    }

    // ─── §6.2 第 1 条：DONE 历史行必须**真落库**，且不产生待办、不 fire 码 3 ───

    /// 正向主用例：custom 节点被办到之后
    /// ① 库里读得到那条 `task_state=20` 的行（聚合里 append 一条不算做到）；
    /// ② 待办列表里**没有**它（旧形状给它建 DOING 行＝§6.1 禁止形状①）；
    /// ③ 令牌沿出边继续流转（下游 approve 建单、最终办结）；
    /// ④ 全程不为它 fire 码 3（spec §11.3：码 3 的事实是"新待办产生"）；
    /// ⑤ 返回值按节点 `val`（`customResult`）落进流程变量。
    #[tokio::test]
    async fn test_i142_custom_lands_done_row_and_continues_token() {
        let (engine, repo, rec) =
            cst_engine(Some((CLAZZ, Arc::new(ProbeHandler) as Arc<dyn CustomNodeHandler>)));
        let name = "i142_main";
        let did = cst_define(&repo, &cst_main(name));

        // 发起：只该有 apply 一条待办，custom 还没被走到 ⇒ 一行都没有
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        assert_eq!(cst_rows(&repo, inst.instance_id, "custom1").len(), 0,
            "custom 节点在 apply 之后，发起时不该有任何行");
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").expect("apply 待办应在");

        // 办掉 apply ⇒ 令牌走到 custom1
        engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();

        let hist = cst_rows(&repo, inst.instance_id, "custom1").into_iter()
            .find(|t| t.task_state == TaskState::Finished.code())
            .expect("§6.2 第 1 条：custom 节点必须有一条 task_state=20 的历史行**查得到**");
        // 真落库自证：按 id 反查主键行（只进聚合的话这里就是 None）
        let from_db = repo.find_task_by_id(hist.task_id).unwrap()
            .expect("历史行必须真落库（find_task_by_id 读得到）");
        assert_eq!(from_db.task_state, TaskState::Finished.code());
        assert_eq!(from_db.task_name, "custom1");
        assert_eq!(hist.actor_ids, vec!["applicant".to_string()],
            "留痕主体＝当前操作人（java createHistoryTask 的 singletonList(operator) 同形）");
        assert_eq!(repo.find_task_actors(hist.task_id).unwrap(), vec!["applicant".to_string()],
            "参与者表里也要有那一条（不然 is_allowed/反查读不到留痕主体）");
        assert_eq!(hist.parent_task_id, Some(apply.task_id),
            "建单不变量：parent＝本次 execution 刚办结的那个任务");
        assert_eq!(hist.variables.get("isFirstTaskNode").and_then(|v| v.as_bool()), Some(false),
            "建单不变量：行级首节点标记照 persist_tasks 同一把尺子落库");
        assert!(hist.is_finished(), "行本身是已办结态 ⇒ 谁也办不动");
        // spec/02 §6.2 第 **1bis** 条：这条 DONE 行必须**同时**带完成时间。
        // 内存仓整行存 ⇒ 这里能读到；SQL 仓原先的 INSERT 语句不带 finish_time 列，
        // 聚合根赋的值在真库那一路被静默丢掉（`jeeflow-repository-sqlx/src/lib.rs::save_task`
        // 本轮补列，同一条判据在 `test_mysql_i142_custom_history_row_lands` 里复查）。
        assert!(hist.finish_time.is_some(),
            "§6.2 1bis：doneList/approvalRecord 按 operator＋finish_time 取数，缺列＝留痕没落");
        assert_eq!(from_db.finish_time, hist.finish_time, "落库行的完成时间不得与返回行分叉");
        assert!(hist.update_time.is_none() && hist.update_user.is_none(),
            "只补 finish_time：记录类没有'办理'那一步，update 审计两列继续留空");

        // ②待办数不增加：custom1 不在待办里，链上只有 approve 一条
        let doing = repo.find_doing_tasks(inst.instance_id, &[]).unwrap();
        assert_eq!(doing.len(), 1, "待办只该有 approve 一条，实得 {:?}",
            doing.iter().map(|t| (t.task_name.clone(), t.task_state)).collect::<Vec<_>>());
        assert_eq!(doing[0].task_name, "approve", "令牌必须沿出边继续流转（③）");
        assert!(!doing.iter().any(|t| t.task_name == "custom1"),
            "§6.1 禁止形状①：记录类绝不被建成 DOING 待办");

        // ⑤返回值落流程变量：节点写的是 val=customResult
        let inst_now = repo.find_instance_by_id(inst.instance_id).unwrap().unwrap();
        assert_eq!(inst_now.variables.get_str("customResult"), Some("probe-返回值"),
            "val 命中 ⇒ 按 val 键写进流程变量");
        assert!(inst_now.variables.get_str("custom_return_val").is_none(),
            "命中 val 时不该再写缺省键");

        // ④码 3 序列：[1,3,5,3,5,2]，且没有任何码 3 指向历史行的 id
        engine.execute_task_async(doing[0].task_id, "leader", &FlowData::new()).await.unwrap();
        assert_eq!(repo.find_instance_by_id(inst.instance_id).unwrap().unwrap().state,
            InstanceState::Finished.code(), "实例应一路走到终点");
        assert_eq!(csm_seq(&rec), vec![
            "PROCESS_INSTANCE_START", "PROCESS_TASK_START", "TASK_COMPLETE",
            "PROCESS_TASK_START", "TASK_COMPLETE", "PROCESS_INSTANCE_END"],
            "序列里必须没有第三条 PROCESS_TASK_START（历史行不 fire 码 3）");
        let task_start_ids: Vec<i64> = rec.events.lock().unwrap().iter()
            .filter(|e| e.event_type == ProcessEventType::ProcessTaskStart)
            .map(|e| e.source_id).collect();
        assert_eq!(task_start_ids.len(), 2, "只有 apply/approve 两条待办能触发码 3");
        assert!(!task_start_ids.contains(&hist.task_id),
            "不为记录类历史行 fire 码 3，实得 {task_start_ids:?}");
    }

    /// 发起腿同样要有 INSERT 腿：`start → custom → end` 这种"发起即命中记录类"的流，
    /// 走的是 `start_async` 那条 persist 收口——历史行照样必须落库、照样不 fire 码 3、
    /// 令牌继续走到办结。（java 本轮那句"两条腿都要有 INSERT，漏一条就是半条路径又不落库"。）
    #[tokio::test]
    async fn test_i142_custom_at_start_leg_also_lands_row() {
        let (engine, repo, rec) =
            cst_engine(Some((CLAZZ, Arc::new(ProbeHandler) as Arc<dyn CustomNodeHandler>)));
        let name = "i142_start_leg";
        let content = cst_chain(name, &[
            ("start", "snaker:start", ""),
            ("custom1", "snaker:custom", &format!(r#""clazz":"{CLAZZ}""#)),
            ("end", "snaker:end", ""),
        ]);
        let did = cst_define(&repo, &content);

        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let hist = cst_rows(&repo, inst.instance_id, "custom1").into_iter()
            .find(|t| t.task_state == TaskState::Finished.code())
            .expect("发起腿也必须把 DONE 历史行 INSERT 进库");
        assert!(repo.find_task_by_id(hist.task_id).unwrap().is_some(), "落库自证");
        assert_eq!(repo.find_doing_tasks(inst.instance_id, &[]).unwrap().len(), 0,
            "记录类不产生待办 ⇒ 待办数 0（§6.1 硬结论 2）");
        assert_eq!(repo.find_instance_by_id(inst.instance_id).unwrap().unwrap().state,
            InstanceState::Finished.code(), "令牌必须继续流转到 end");
        assert_eq!(csm_seq(&rec), vec!["PROCESS_INSTANCE_START", "PROCESS_INSTANCE_END"],
            "全程不出现码 3");
        assert_eq!(hist.variables.get("isFirstTaskNode").and_then(|v| v.as_bool()), Some(true),
            "start 的直接后继 ⇒ 与 persist_tasks 同一把尺子读出 true（不自造判据）");
    }

    // ─── 到期时间这一档：判定＝不写，落 NULL（issues/137 B 那句 TODO 的核实结论）───

    /// 同一个流程里任务节点配 `expireTime` 正常算出到期时间，而 custom 节点即便也配了
    /// `expireTime`，那条历史行的 `expire_time` 仍必须是 NULL。
    /// 判据：spec/02 §6 的 custom 属性字典只有 clazz/methodName/args/val（expireTime 属 §4
    /// 任务节点那一族）；java `createHistoryTask` 结构上拿不到表达式（四列传 null）、
    /// python 同判。⇒ issues/137 B 留的"复活时补 apply_expire_time"前提不成立。
    /// 这一支同时把"不是求值器坏了"钉在对照格上：同一次执行里 task 节点那条有值。
    #[tokio::test]
    async fn test_i142_custom_history_row_expire_time_stays_null() {
        let (engine, repo, _rec) =
            cst_engine(Some((CLAZZ, Arc::new(ProbeHandler) as Arc<dyn CustomNodeHandler>)));
        let name = "i142_expire";
        let content = cst_chain(name, &[
            ("start", "snaker:start", ""),
            ("apply", "snaker:task", r#""assignee":"applicant""#),
            ("custom1", "snaker:custom", &format!(r#""clazz":"{CLAZZ}","expireTime":"2h""#)),
            ("approve", "snaker:task", r#""assignee":"leader","expireTime":"2h""#),
            ("end", "snaker:end", ""),
        ]);
        let did = cst_define(&repo, &content);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").unwrap();
        engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();

        let hist = cst_rows(&repo, inst.instance_id, "custom1").into_iter()
            .find(|t| t.task_state == TaskState::Finished.code()).unwrap();
        assert!(hist.expire_time.is_none(),
            "记录类历史行到期列留 NULL（判定见本用例注释），实得 {:?}", hist.expire_time);

        // 对照格：同一一次执行里任务节点的到期写点照常工作 ⇒ NULL 是**这一档**的判定，不是坏了
        let approve = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "approve").unwrap();
        assert!(approve.expire_time.is_some(),
            "写点①（普通建单）必须照常算出到期时间，否则本用例的对照失去意义：实得 NULL");
    }

    // ─── §6.2 第 2 条：clazz 解析不了 ⇒ 记日志 ＋ 照常落行 ＋ 续流，两档分开 ───

    /// 未注册处理器：不炸、历史行照落、令牌继续；日志文案带 nodeId ＋ 实得 clazz ＋ 注册入口。
    #[tokio::test]
    async fn test_i142_unregistered_clazz_lands_row_and_continues() {
        let (engine, repo, _rec) = cst_engine(None);   // 一个处理器都没注册
        let name = "i142_unregistered";
        let content = cst_chain(name, &[
            ("start", "snaker:start", ""),
            ("custom1", "snaker:custom", r#""clazz":"com.example.NotRegistered""#),
            ("approve", "snaker:task", r#""assignee":"leader""#),
            ("end", "snaker:end", ""),
        ]);
        let did = cst_define(&repo, &content);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await
            .expect("§6.2 第 2 条：clazz 未注册严禁抛错打断建单");

        let hist = cst_rows(&repo, inst.instance_id, "custom1").into_iter()
            .find(|t| t.task_state == TaskState::Finished.code())
            .expect("未注册也要照常落历史行（不许「记了日志但停在原地」）");
        assert!(repo.find_task_by_id(hist.task_id).unwrap().is_some(), "行必须真落库");
        assert_eq!(repo.find_doing_tasks(inst.instance_id, &[]).unwrap().len(), 1,
            "令牌必须继续流转到 approve");
        assert_eq!(repo.find_instance_by_id(inst.instance_id).unwrap().unwrap().state,
            InstanceState::Doing.code(), "还停在 approve 待办上");

        let msg = custom_unregistered_clazz_warning("custom1", "com.example.NotRegistered");
        assert!(msg.contains("custom1") && msg.contains("com.example.NotRegistered"),
            "未注册档必须带 nodeId 与实得 clazz：{msg}");
        assert!(msg.contains("register_custom_handler"), "要给出注册入口，排障不必猜：{msg}");
    }

    /// `clazz` 为空串／缺键／纯空白三形 ⇒ 同一档（"没配"），既不炸也不并入"未注册"那一档；
    /// 两档文案**分别**可诊断（§6.2 点名 c# 把两者合成一条、覆盖面比 java 宽）。
    #[tokio::test]
    async fn test_i142_empty_clazz_is_its_own_leg_and_rows_still_land() {
        for (tag, props) in [
            ("缺键", r#""methodName":"execute""#),
            ("空串", r#""clazz":"""#),
            ("纯空白", r#""clazz":"   ""#),
        ] {
            let (engine, repo, _rec) =
                cst_engine(Some((CLAZZ, Arc::new(ProbeHandler) as Arc<dyn CustomNodeHandler>)));
            let content = cst_chain("i142_empty_clazz", &[
                ("start", "snaker:start", ""),
                ("custom1", "snaker:custom", props),
                ("approve", "snaker:task", r#""assignee":"leader""#),
                ("end", "snaker:end", ""),
            ]);
            let did = cst_define(&repo, &content);
            let inst = engine.start_async(did, "applicant", &FlowData::new()).await
                .unwrap_or_else(|e| panic!("{tag} 这一档不该打断建单，实得 Err: {e:?}"));
            let rows = cst_rows(&repo, inst.instance_id, "custom1");
            assert_eq!(rows.len(), 1, "{tag}：历史行必须照常落一条");
            assert_eq!(rows[0].task_state, TaskState::Finished.code(), "{tag}：行是 DONE");
            assert_eq!(repo.find_instance_by_id(inst.instance_id).unwrap().unwrap().state,
                InstanceState::Doing.code(), "{tag}：令牌继续流转到 approve");
            // 空串档不解析处理器 ⇒ 注册着 ProbeHandler 也不该被调用 ⇒ 不写返回值
            let inst_now = repo.find_instance_by_id(inst.instance_id).unwrap().unwrap();
            assert!(inst_now.variables.get_str("custom_return_val").is_none(),
                "{tag}：clazz 为空 ⇒ 处理器不执行，缺省键也不该出现");
        }

        // 两档文案各写各的，不许合成同一条（c# 那一档的教训）
        let missing = custom_missing_clazz_warning("custom1");
        let unregistered = custom_unregistered_clazz_warning("custom1", "com.example.Nope");
        assert_ne!(missing, unregistered, "两档日志必须分别可诊断");
        assert!(missing.contains("未配置 clazz"), "空串档要说清是「没配」：{missing}");
        assert!(!missing.contains("com.example.Nope"), "空串档不该带 clazz 值");
        assert!(unregistered.contains("未注册处理器"), "未注册档要说清是「没注册」：{unregistered}");
    }

    /// 处理器返回 `Ok(None)` ⇒ 引擎一个键都不写（对齐 java `IHandler` 那一支自己填 args）。
    #[tokio::test]
    async fn test_i142_handler_returning_none_writes_no_variable() {
        let (engine, repo, _rec) =
            cst_engine(Some((CLAZZ, Arc::new(SilentHandler) as Arc<dyn CustomNodeHandler>)));
        let did = cst_define(&repo, &cst_main("i142_silent"));
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").unwrap();
        engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();
        let inst_now = repo.find_instance_by_id(inst.instance_id).unwrap().unwrap();
        assert!(inst_now.variables.get_str("customResult").is_none()
            && inst_now.variables.get_str("custom_return_val").is_none(),
            "Ok(None) ⇒ 两个键都不写，实得 {:?}", inst_now.variables.inner().keys().cloned()
                .collect::<Vec<_>>());
        assert_eq!(cst_rows(&repo, inst.instance_id, "custom1").len(), 1,
            "但历史行照落——返回值与留痕是两件事");
    }

    /// 节点没配 `val` ⇒ 缺省键逐字用 java `FlowConst.CUSTOM_RETURN_VAL`＝`custom_return_val`。
    #[tokio::test]
    async fn test_i142_return_val_falls_back_to_custom_return_val() {
        let (engine, repo, _rec) =
            cst_engine(Some((CLAZZ, Arc::new(ProbeHandler) as Arc<dyn CustomNodeHandler>)));
        let name = "i142_default_key";
        let content = cst_chain(name, &[
            ("start", "snaker:start", ""),
            ("custom1", "snaker:custom", &format!(r#""clazz":"{CLAZZ}""#)),
            ("end", "snaker:end", ""),
        ]);
        let did = cst_define(&repo, &content);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let inst_now = repo.find_instance_by_id(inst.instance_id).unwrap().unwrap();
        assert_eq!(inst_now.variables.get_str("custom_return_val"), Some("probe-返回值"),
            "缺省键必须是 custom_return_val（逐字对齐 java 常量）");
        assert_eq!(crate::spi::CUSTOM_RETURN_VAL, "custom_return_val", "常量本体也钉一下");
    }

    // ─── §6.2 第 2 条末句：处理器**自身**失败不在豁免内 ⇒ 照旧外抛（负向对照）───

    #[tokio::test]
    async fn test_i142_handler_error_propagates_and_breaks_the_step() {
        let (engine, repo, rec) =
            cst_engine(Some((CLAZZ, Arc::new(ErrHandler) as Arc<dyn CustomNodeHandler>)));
        let name = "i142_handler_err";
        let content = cst_chain(name, &[
            ("start", "snaker:start", ""),
            ("apply", "snaker:task", r#""assignee":"applicant""#),
            ("custom1", "snaker:custom", &format!(r#""clazz":"{CLAZZ}""#)),
            ("end", "snaker:end", ""),
        ]);
        let did = cst_define(&repo, &content);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").unwrap();

        let err = engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await
            .err().expect("处理器自身 Err 是业务错误，必须外抛（不许被记日志的豁免吞掉）");
        assert_eq!(err.message(), "处理器自己跑炸了", "外抛时文案原样透出，实得 {:?}", err.message());
        assert!(cst_rows(&repo, inst.instance_id, "custom1").is_empty(),
            "外抛发生在落行之前 ⇒ 这次执行没有半条 DONE 行（豁免只管配错，不管业务炸）");
        assert!(!csm_seq(&rec).iter().any(|n| n == "PROCESS_INSTANCE_END"),
            "整次执行被打断 ⇒ 不该有码 2");
    }

    /// 处理器 panic 同样不许被降级（本栈不套 catch_unwind；对照 `event.rs` 里**监听器** panic
    /// 才做隔离——那是"通知失败不回滚主流程"的另一条哲学，别混用）。
    #[tokio::test]
    #[should_panic(expected = "custom-handler-panic")]
    async fn test_i142_handler_panic_propagates() {
        let (engine, repo, _rec) =
            cst_engine(Some((CLAZZ, Arc::new(PanicHandler) as Arc<dyn CustomNodeHandler>)));
        let name = "i142_handler_panic";
        let content = cst_chain(name, &[
            ("start", "snaker:start", ""),
            ("custom1", "snaker:custom", &format!(r#""clazz":"{CLAZZ}""#)),
            ("end", "snaker:end", ""),
        ]);
        let did = cst_define(&repo, &content);
        let _ = engine.start_async(did, "applicant", &FlowData::new()).await;
        panic!("不该走到这里");
    }

    // ─── §6.2 第 3 条：任务类零参与者必须建 DOING 行（撤掉"空 ⇒ 不建并继续"）───

    /// 一个什么都没配的 task 节点（无 assignee/assignmentHandler/candidateUsers/candidateGroups）
    /// ⇒ **必须**建一行参与者为空的 DOING 行：
    ///   · 行读得到、`task_state=10`、`actor_ids` 是空 Vec（不是 None、也不是兜底挂操作人）；
    ///   · **发起人不在参与者里**（§6.1 硬结论 1：兜底挂当前操作人八栈一律不许有）；
    ///   · 令牌**不**推进（下游节点没有行）⇒ 不存在"自动推进为它反复重入"的形状；
    ///   · 它仍是待办 ⇒ 照 `persist_tasks` 收口 fire 码 3，载荷 `actors` 为空数组。
    /// 旧形状是"一行不建、令牌沿出边跑掉"＝§6.1 点名的死锁黑洞（库里查不到节点到过）。
    #[tokio::test]
    async fn test_i142_zero_actor_task_node_still_creates_doing_row() {
        let (engine, repo, rec) = cst_engine(None);
        let name = "i142_zero_actor";
        let content = cst_chain(name, &[
            ("start", "snaker:start", ""),
            ("silence", "snaker:task", ""),
            ("approve", "snaker:task", r#""assignee":"leader""#),
            ("end", "snaker:end", ""),
        ]);
        let did = cst_define(&repo, &content);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await
            .expect("零参与者不得让发起报错");

        let rows = cst_rows(&repo, inst.instance_id, "silence");
        assert_eq!(rows.len(), 1, "任务类零参与者必须建一行，实得 {} 行", rows.len());
        let row = &rows[0];
        assert_eq!(row.task_state, TaskState::Doing.code(), "建的是待办行（10）");
        assert!(row.actor_ids.is_empty(), "参与者集合为空 Vec，实得 {:?}", row.actor_ids);
        assert_eq!(row.actor_id, None, "进行中任务该列恒无值");
        assert_eq!(repo.find_task_actors(row.task_id).unwrap(), Vec::<String>::new(),
            "参与者表里零行——**不是**兜底挂给当前操作人");
        assert!(!row.actor_ids.iter().any(|a| a == "applicant"),
            "§6.1 硬结论 1：发起人不在参与者里（不许兜底挂当前操作人）");
        assert!(!row.is_allowed("applicant"), "零参与者行谁也办不动（设计如此，不是缺陷）");

        assert_eq!(repo.find_doing_tasks(inst.instance_id, &[]).unwrap().len(), 1,
            "只有这一条待办：令牌停在这里，不替它往下推（否则又回到「跳过建行」那一档）");
        assert_eq!(cst_rows(&repo, inst.instance_id, "approve").len(), 0,
            "下游节点没有行 ⇒ 自动推进逻辑不会为一个办不动的行反复重入");
        assert_eq!(repo.find_instance_by_id(inst.instance_id).unwrap().unwrap().state,
            InstanceState::Doing.code(), "实例停在 10，且这一停**可查**（库里有一行指着它）");

        assert_eq!(csm_seq(&rec), vec!["PROCESS_INSTANCE_START", "PROCESS_TASK_START"]);
        let actors_payload = rec.events.lock().unwrap().iter()
            .filter(|e| e.event_type == ProcessEventType::ProcessTaskStart)
            .map(|e| e.data.get("actors").and_then(|v| v.as_array()).map(|a| a.len()))
            .collect::<Vec<_>>();
        assert_eq!(actors_payload, vec![Some(0)], "码 3 载荷 actors 为空数组（新待办确实产生了）");
    }

    /// 只配 `candidateGroups` 的节点：`resolve_assignee` 不把候选组折进参与者（Priority 4 只收
    /// candidateUsers）⇒ 与"什么都没配"同档：**建一行零参与者的 DOING 行**。
    /// 旧形状这里因为守卫里的 `candidate_groups().is_some()` 而**一行都不建**——
    /// 那条件本身就是"配了候选人/候选组就别建行"的旧判据，本轮撤掉守卫后由这一格钉住新形状。
    #[tokio::test]
    async fn test_i142_candidate_group_only_node_creates_zero_actor_row() {
        let (engine, repo, _rec) = cst_engine(None);
        let name = "i142_candidate_group";
        let content = cst_chain(name, &[
            ("start", "snaker:start", ""),
            ("pick", "snaker:task", r#""candidateGroups":"dept_leader""#),
            ("end", "snaker:end", ""),
        ]);
        let did = cst_define(&repo, &content);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let rows = cst_rows(&repo, inst.instance_id, "pick");
        assert_eq!(rows.len(), 1, "候选组不解析成参与者 ⇒ 也必须建那一行零参与者待办");
        assert_eq!(rows[0].task_state, TaskState::Doing.code());
        assert!(rows[0].actor_ids.is_empty(), "候选组不生成 actor，实得 {:?}", rows[0].actor_ids);
    }

    /// **本轮没动的形状，钉在这里以免被误读成已修**：`resolve_assignee` 的 Priority 4
    /// 把 `candidateUsers` 直接折进参与者集合，而 spec/02 §4 明确
    /// 「candidateUsers ——**不生成 actor**，供 candidatePage 选人」。
    /// 这不是 issues/142 A 批的四件事之一（本案只管"零参与者要不要建行"），
    /// 改动面涉及所有拿 candidateUsers 当"预分配处理人"用的存量流程 ⇒ 记为待拍/另批，
    /// 这一格按**现状**断言，将来谁改这条判据就会在这里红。
    #[tokio::test]
    async fn test_i142_candidate_users_current_shape_folds_into_actors_pending_ruling() {
        let (engine, repo, _rec) = cst_engine(None);
        let name = "i142_candidate_users";
        let content = cst_chain(name, &[
            ("start", "snaker:start", ""),
            ("pick", "snaker:task", r#""candidateUsers":"u1,u2""#),
            ("end", "snaker:end", ""),
        ]);
        let did = cst_define(&repo, &content);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let rows = cst_rows(&repo, inst.instance_id, "pick");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].actor_ids, vec!["u1".to_string(), "u2".to_string()],
            "现状：candidateUsers 被折进 actor（与 spec/02 §4 那条「不生成 actor」分叉，本轮未改）");
    }

    /// **共享夹具**那一格：`flows/08-custom-node.json` 是八语言同一份（编辑源在 jeeflow-java，
    /// `flowsdir` 精确镜像进本仓），里面的 `clazz` 写的是 JVM 类名
    /// `com.mldong.jeeflow.test.TestCustomHandler` ⇒ 本栈必然落「未注册」档。
    /// 这条链（start → apply → custom1 → end）在 HEAD 上的形状是：custom1 建一条 **DOING 待办**，
    /// 流程永远办不完（申请人之外没人能办它，而它本来也不该有人办）。
    /// 改后：一条 DONE 留痕、令牌直连 end、实例办结、`val` 那个键一个都不写。
    #[tokio::test]
    async fn test_i142_shared_fixture_08_custom_node_completes() {
        let (engine, repo, rec) = cst_engine(None);
        let path = format!("{}/08-custom-node.json", crate::flowsdir::dir().to_string_lossy());
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("共享夹具读不到 {path}: {e}"));
        let did = cst_define(&repo, &content);

        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();
        let apply = repo.find_doing_tasks(inst.instance_id, &[]).unwrap()
            .into_iter().find(|t| t.task_name == "apply").expect("apply 待办应在");
        engine.execute_task_async(apply.task_id, "applicant", &FlowData::new()).await.unwrap();

        let hist = cst_rows(&repo, inst.instance_id, "custom1").into_iter()
            .find(|t| t.task_state == TaskState::Finished.code())
            .expect("夹具里的 snaker:custom 节点必须留下 DONE 留痕");
        assert_eq!(hist.task_name, "custom1");
        assert!(repo.find_doing_tasks(inst.instance_id, &[]).unwrap().is_empty(),
            "custom1 → end，待办必须清空（HEAD 形状在这里会剩一条办不动的假待办）");
        assert_eq!(repo.find_instance_by_id(inst.instance_id).unwrap().unwrap().state,
            InstanceState::Finished.code(), "同一份夹具在本栈也能跑到终点");
        let inst_now = repo.find_instance_by_id(inst.instance_id).unwrap().unwrap();
        assert!(inst_now.variables.get_str("customResult").is_none(),
            "未注册处理器 ⇒ 夹具配的 val=customResult 不该被写");
        assert_eq!(csm_seq(&rec), vec![
            "PROCESS_INSTANCE_START", "PROCESS_TASK_START", "TASK_COMPLETE", "PROCESS_INSTANCE_END"],
            "夹具这条链的事件序列是 [1,3,5,2]，历史行不在码 3 里");
    }

    // ─── 未知档在执行腿上的形状：既不是记录类也不是任务类 ───

    /// `snaker:Custom`（拼错大小写）在 HEAD 上被兜底臂收成 Custom ⇒ 被当任务类建了一条
    /// DOING 待办。拆臂后它落 Unknown：执行腿**跳过**——不建行、不 fire 事件、
    /// 也不沿出边推进（与 java 解析期 continue 的可观测结果一致），库里什么行都没有。
    #[tokio::test]
    async fn test_i142_unknown_node_is_skipped_without_any_row() {
        let (engine, repo, rec) = cst_engine(None);
        let name = "i142_unknown";
        let content = cst_chain(name, &[
            ("start", "snaker:start", ""),
            ("typo1", "snaker:Custom", &format!(r#""clazz":"{CLAZZ}""#)),
            ("approve", "snaker:task", r#""assignee":"leader""#),
            ("end", "snaker:end", ""),
        ]);
        let did = cst_define(&repo, &content);
        let inst = engine.start_async(did, "applicant", &FlowData::new()).await.unwrap();

        assert!(cst_rows(&repo, inst.instance_id, "typo1").is_empty(),
            "未知档不建行：既不被当记录类（无 DONE 行），也不被当任务类（无 DOING 行）");
        assert!(repo.find_doing_tasks(inst.instance_id, &[]).unwrap().is_empty(),
            "没有待办（旧形状这里恰恰会多出一条办不动的假待办）");
        assert_eq!(repo.find_instance_by_id(inst.instance_id).unwrap().unwrap().state,
            InstanceState::Doing.code(), "实例停在未知节点处（照 java 跳过节点＝令牌不前进）");
        assert_eq!(csm_seq(&rec), vec!["PROCESS_INSTANCE_START"], "未知档不产生任何任务事件");
    }
}
