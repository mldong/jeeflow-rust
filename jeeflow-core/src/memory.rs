//! In-memory repository implementation for testing (T0).
//! Implements ProcessRepository + ProcessExtRepository with HashMap storage.
//! All methods are SYNCHRONOUS — no async/await.

use crate::error::JeeflowResult;
use crate::model::*;
use crate::spi::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

/// In-memory process repository for testing.

fn flow_data_json_string(fd: &crate::json::FlowData) -> Option<String> {
    let entries: Vec<(String, crate::json::JsonValue)> = fd
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Some(crate::json::JsonValue::Object(entries).to_json_string())
}

// ── m_ 过滤下推（issues/106）：page_* 先过滤全量 → 再算 total → 再切片 ──
// 列白名单对齐 java pushdown（spec/06 §2.2）；未命中白名单的 (alias, column) → 不命中。
// Option 字段为 None → 不命中（对齐旧 facade matches_filter 的 None 语义）。

fn field_of(f: &QueryFilter, val: Option<String>) -> bool {
    match val {
        Some(v) => crate::filter_sql::op_matches(&f.op, &v, &f.value),
        None => false,
    }
}

fn define_row_matches(f: &QueryFilter, r: &DefineRow) -> bool {
    field_of(f, match (f.alias.as_str(), f.column.as_str()) {
        ("t", "name") => Some(r.name.clone()),
        ("t", "display_name") => Some(r.display_name.clone()),
        ("t", "type") => Some(r.define_type.clone()),
        ("t", "state") => Some(r.state.to_string()),
        ("t", "version") => Some(r.version.to_string()),
        ("t", "id") => Some(r.id.to_string()),
        ("t", "create_time") => r.create_time.clone(),
        ("t", "create_user") => r.create_user.clone(),
        ("t", "update_time") => r.update_time.clone(),
        ("t", "update_user") => r.update_user.clone(),
        _ => None,
    })
}

fn instance_row_matches(f: &QueryFilter, r: &InstanceRow) -> bool {
    field_of(f, match (f.alias.as_str(), f.column.as_str()) {
        ("t", "id") => Some(r.id.to_string()),
        ("t", "state") => Some(r.state.to_string()),
        ("t", "business_no") => r.business_no.clone(),
        ("t", "operator") => Some(r.operator.clone()),
        ("t", "parent_node_name") => r.parent_node_name.clone(),
        ("t", "process_define_id") => Some(r.process_define_id.to_string()),
        ("t", "expire_time") => r.expire_time.clone(),
        ("t", "create_time") => r.create_time.clone(),
        ("t", "create_user") => r.create_user.clone(),
        ("t", "update_time") => r.update_time.clone(),
        ("t", "update_user") => r.update_user.clone(),
        ("pd", "name") => r.define_name.clone(),
        ("pd", "display_name") => r.define_display_name.clone(),
        ("pd", "version") => r.define_version.map(|v| v.to_string()),
        _ => None,
    })
}

/// issues/138：实例行的**唯一投影出口**——`page_instances` 与 `page_cc_instances` 共用。
///
/// 规范 06 §processInstance/ccList 要求 ccList「rows 同 processInstance/page 行结构」；
/// PHP 侧的同名收口（`instanceRowWithDefine()`）是靠"两处调同一个投影函数"由构造保证这一点，
/// 这里照做：两处不再各自拼装，行形状漂移在写代码时就发生不了。
fn instance_row_of(i: &ProcessInstance, define: Option<&ProcessDefine>) -> InstanceRow {
    InstanceRow {
        id: i.instance_id,
        parent_id: i.parent_id,
        process_define_id: i.define_id,
        state: i.state,
        parent_node_name: i.parent_node_name.clone(),
        business_no: i.business_no.clone(),
        operator: i.operator.clone(),
        expire_time: i.expire_time.clone(),
        variable: flow_data_json_string(&i.variables),
        create_time: i.create_time.clone(),
        create_user: i.create_user.clone(),
        update_time: i.update_time.clone(),
        update_user: i.update_user.clone(),
        define_name: define.map(|d| d.name.clone()),
        define_display_name: define.map(|d| d.display_name.clone()),
        define_version: define.map(|d| d.version),
    }
}

fn task_row_matches(f: &QueryFilter, r: &TaskRow) -> bool {
    field_of(f, match (f.alias.as_str(), f.column.as_str()) {
        ("t", "id") => Some(r.id.to_string()),
        ("t", "task_name") => Some(r.task_name.clone()),
        ("t", "display_name") => Some(r.display_name.clone()),
        ("t", "task_type") => Some(r.task_type.to_string()),
        ("t", "perform_type") => Some(r.perform_type.to_string()),
        ("t", "task_state") => Some(r.task_state.to_string()),
        ("t", "operator") => r.operator.clone(),
        ("t", "finish_time") => r.finish_time.clone(),
        ("t", "expire_time") => r.expire_time.clone(),
        ("t", "form_key") => r.form_key.clone(),
        ("t", "task_parent_id") => r.task_parent_id.map(|v| v.to_string()),
        ("pi", "process_define_id") => r.process_define_id.map(|v| v.to_string()),
        ("pi", "state") => r.instance_state.map(|v| v.to_string()),
        ("pi", "operator") => r.instance_operator.clone(),
        ("pi", "business_no") => r.business_no.clone(),
        ("pd", "name") => r.define_name.clone(),
        ("pd", "display_name") => r.define_display_name.clone(),
        ("pd", "version") => r.define_version.map(|v| v.to_string()),
        _ => None,
    })
}

fn design_row_matches(f: &QueryFilter, r: &ProcessDesign) -> bool {
    field_of(f, match (f.alias.as_str(), f.column.as_str()) {
        ("t", "name") => Some(r.name.clone()),
        ("t", "display_name") => Some(r.display_name.clone()),
        ("t", "type") => Some(r.design_type.clone()),
        ("t", "is_deployed") => Some(r.is_deployed.to_string()),
        ("t", "icon") => r.icon.clone(),
        ("t", "remark") => r.remark.clone(),
        ("t", "id") => Some(r.id.to_string()),
        ("t", "create_time") => r.create_time.clone(),
        ("t", "create_user") => r.create_user.clone(),
        ("t", "update_time") => r.update_time.clone(),
        ("t", "update_user") => r.update_user.clone(),
        _ => None,
    })
}

pub struct MemoryRepository {
    id_counter: AtomicI64,
    defines: Mutex<HashMap<i64, ProcessDefine>>,
    instances: Mutex<HashMap<i64, ProcessInstance>>,
    tasks: Mutex<HashMap<i64, ProcessTask>>,
    task_actors: Mutex<HashMap<i64, Vec<String>>>,
    cc_instances: Mutex<Vec<CcInstance>>,
    designs: Mutex<HashMap<i64, ProcessDesign>>,
    design_his: Mutex<Vec<ProcessDesignHis>>,
    surrogates: Mutex<HashMap<i64, ProcessSurrogate>>,
}

impl MemoryRepository {
    pub fn new() -> Self {
        MemoryRepository {
            id_counter: AtomicI64::new(100000),
            defines: Mutex::new(HashMap::new()),
            instances: Mutex::new(HashMap::new()),
            tasks: Mutex::new(HashMap::new()),
            task_actors: Mutex::new(HashMap::new()),
            cc_instances: Mutex::new(Vec::new()),
            designs: Mutex::new(HashMap::new()),
            design_his: Mutex::new(Vec::new()),
            surrogates: Mutex::new(HashMap::new()),
        }
    }

    fn next_id(&self) -> i64 {
        self.id_counter.fetch_add(1, Ordering::SeqCst)
    }
}

impl Default for MemoryRepository {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessRepository for MemoryRepository {
    fn find_define_by_id(&self, define_id: i64) -> JeeflowResult<Option<ProcessDefine>> {
        let defines = self.defines.lock().unwrap();
        Ok(defines.get(&define_id).cloned())
    }

    fn save_define(&self, define: &mut ProcessDefine) -> JeeflowResult<()> {
        if define.id == 0 { define.id = self.next_id(); }
        let mut defines = self.defines.lock().unwrap();
        defines.insert(define.id, define.clone());
        Ok(())
    }

    fn update_define(&self, define: &ProcessDefine) -> JeeflowResult<()> {
        let mut defines = self.defines.lock().unwrap();
        defines.insert(define.id, define.clone());
        Ok(())
    }

    fn update_define_state(&self, define_id: i64, state: i32) -> JeeflowResult<()> {
        let mut defines = self.defines.lock().unwrap();
        if let Some(d) = defines.get_mut(&define_id) {
            d.state = state;
        }
        Ok(())
    }

    fn remove_define(&self, define_id: i64) -> JeeflowResult<()> {
        let mut defines = self.defines.lock().unwrap();
        defines.remove(&define_id);
        Ok(())
    }

    fn find_instance_by_id(&self, instance_id: i64) -> JeeflowResult<Option<ProcessInstance>> {
        let mut inst = {
            let instances = self.instances.lock().unwrap();
            instances.get(&instance_id).cloned()
        };
        // 任务落在独立 map；聚合内 tasks 可能仍是 task_id=0 的草稿——读侧用仓储真相回填
        if let Some(ref mut i) = inst {
            let tasks = self.tasks.lock().unwrap();
            i.tasks = tasks
                .values()
                .filter(|t| t.process_instance_id == instance_id)
                .cloned()
                .collect();
        }
        Ok(inst)
    }

    fn save_instance(&self, instance: &mut ProcessInstance) -> JeeflowResult<()> {
        if instance.instance_id == 0 { instance.instance_id = self.next_id(); }
        let mut instances = self.instances.lock().unwrap();
        instances.insert(instance.instance_id, instance.clone());
        Ok(())
    }

    fn update_instance(&self, instance: &ProcessInstance) -> JeeflowResult<()> {
        let mut instances = self.instances.lock().unwrap();
        instances.insert(instance.instance_id, instance.clone());
        Ok(())
    }

    fn find_task_by_id(&self, task_id: i64) -> JeeflowResult<Option<ProcessTask>> {
        let tasks = self.tasks.lock().unwrap();
        Ok(tasks.get(&task_id).cloned())
    }

    fn save_task(&self, task: &mut ProcessTask) -> JeeflowResult<()> {
        if task.task_id == 0 { task.task_id = self.next_id(); }
        let mut tasks = self.tasks.lock().unwrap();
        tasks.insert(task.task_id, task.clone());
        Ok(())
    }

    fn update_task(&self, task: &ProcessTask) -> JeeflowResult<()> {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.insert(task.task_id, task.clone());
        Ok(())
    }

    fn find_doing_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>> {
        let tasks = self.tasks.lock().unwrap();
        let result: Vec<ProcessTask> = tasks.values()
            .filter(|t| {
                t.process_instance_id == instance_id
                    && t.task_state == TaskState::Doing.code()
                    && (task_names.is_empty() || task_names.contains(&t.task_name))
            })
            .cloned()
            .collect();
        Ok(result)
    }

    fn find_done_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>> {
        let tasks = self.tasks.lock().unwrap();
        let result: Vec<ProcessTask> = tasks.values()
            .filter(|t| {
                t.process_instance_id == instance_id
                    && t.task_state == TaskState::Finished.code()
                    && (task_names.is_empty() || task_names.contains(&t.task_name))
            })
            .cloned()
            .collect();
        Ok(result)
    }

    fn find_history_tasks(&self, instance_id: i64) -> JeeflowResult<Vec<ProcessTask>> {
        let tasks = self.tasks.lock().unwrap();
        let result: Vec<ProcessTask> = tasks.values()
            .filter(|t| t.process_instance_id == instance_id)
            .cloned()
            .collect();
        Ok(result)
    }

    fn find_task_actors(&self, task_id: i64) -> JeeflowResult<Vec<String>> {
        let actors = self.task_actors.lock().unwrap();
        Ok(actors.get(&task_id).cloned().unwrap_or_default())
    }

    fn add_task_actor(&self, task_id: i64, new_actors: &[String]) -> JeeflowResult<()> {
        // issues/142 B 批 · spec 06-facade.md §2.11 的**写侧兜底层**（判据＝`model::normalize_actors`，
        // 与抄送侧 §2.10 同一枚单点，不抄第二份）：逐元素 trim ⇒ 空串/纯空白丢弃 ⇒ 同一次调用内折叠，
        // 落库与比较一律取 trim 后的串。归一排在取锁之前（本函数不 await，锁只在函数体内取放一次）。
        // 只修门面腿的话，绕过门面直连仓储的调用方照样能把空归属值灌进 `actor_id`——那正是
        // issues/129 那族"空 operator 读全库"的上游进水口。
        // ⚠️ 与 sqlx 仓那条必须**同答案**（issues/117 场景 27）：旧形状是本仓判重、sqlx 仓盲插
        //    （无判重、无判空、无 trim），同一串入参两仓两个结果。
        // 反向哨兵（§2.11 硬要求④）：`"0"`／`"00"` 是正常 id，不得被当成空值丢掉。
        let actors = crate::model::normalize_actors(new_actors);
        let mut actors_map = self.task_actors.lock().unwrap();
        let entry = actors_map.entry(task_id).or_insert_with(Vec::new);
        for a in &actors {
            if !entry.contains(a) { entry.push(a.clone()); }
        }
        Ok(())
    }

    fn remove_task_actor(&self, task_id: i64, remove_actors: &[String]) -> JeeflowResult<()> {
        // issues/137 §3-6（spec 06 §processTask/removeTaskActor 语义 6，owner 2026-10-02 拍
        // 「两形并集」）：删除列表过 `model::actor_delete_forms`——空值一律丢弃，非空值展开成
        // 「原值 ∪ trim 值」两形并集。为什么不能只取原值：第三方绕过门面直连仓储传「 8601 」时，
        // 删不掉写侧归一后落库的规范行 8601（issues/142 §9.2，静默 no-op 报成功）；为什么也
        // 不能只取 trim 形（1.8.36 之前本仓正是这个形状）：门面按语义 6 交出的是**行上的原值**，
        // 修复前落下的未 trim 历史脏行「 9101 」被削成 9101，真库 NO PAD 排序规则下那一行
        // 删不掉而门面报成功——被摘的人待办还在。两形并集同时满足两侧。
        // 并集为空 ⇒ 早退，一条删除都不发（空串入参在历史 actor_id='' 脏行上会批量误删，
        // issues/129 的删除位对偶）。与 sqlx 仓同一条判据、同一个答案（issues/117 场景 27）。
        // 展开排在取锁之前（本函数不 await，锁只在函数体内取放一次）。
        let forms = crate::model::actor_delete_forms(remove_actors);
        if forms.is_empty() { return Ok(()); }
        let mut actors = self.task_actors.lock().unwrap();
        if let Some(entry) = actors.get_mut(&task_id) {
            entry.retain(|a| !forms.contains(a));
        }
        Ok(())
    }

    fn create_cc_instance(&self, instance_id: i64, creator: &str, actor_ids: &[String]) -> JeeflowResult<()> {
        // issues/141 G2 写侧判重＝幂等空操作（spec 06 §4），与 SqlxRepository::create_cc_instance
        // 同一条判据：同一 `(实例, 被抄送人)` 已有 cc 行 ⇒ 直接跳过——①不新增行、②不重置未读
        // （state 保持原值）、③不更新原行时间（连 UPDATE 都不发，create_time/update_time 逐字不变）。
        // 判重放在**写侧**而不是查询侧：查询不引入 DISTINCT，历史重复行也不清理。
        // ⚠️ 本函数不 await（同步 SPI），锁只在函数体内取放一次；判重的读侧走
        //    `ProcessRepository::create_cc_instance_if_absent` 的 default，两次加锁也是**串行**
        //    而非嵌套——本仓有"持锁跨 await 自死锁"的前科（engine.rs 事件腿），不得在此处再犯。
        // issues/141 G10「空不创建行」（spec 06 §2.10）写侧兜底：归一在取锁之前——
        // 空串/纯空白一律丢弃、落库值取 trim 后的串（`" 123 "` 与 `"123"` 判为同一个人，
        // 与上面的 G2 判重同一条尺子）。绕过引擎漏斗与门面直连仓储的调用方同样建不出空行。
        let actors = crate::model::normalize_cc_actors(actor_ids);
        let mut ccs = self.cc_instances.lock().unwrap();
        for actor_id in &actors {
            if ccs.iter().any(|c| c.process_instance_id == instance_id && &c.actor_id == actor_id) {
                continue;
            }
            let now = current_time_str();
            ccs.push(CcInstance {
                id: self.next_id(),
                process_instance_id: instance_id,
                actor_id: actor_id.clone(),
                state: 0,
                // 行形状对齐 `wf_process_cc_instance`（issues/141 G2）：时间列真填，
                // "重复抄送不得刷新原行时间"那一档才照得出来；sqlx 仓那边一直是真列。
                create_time: Some(now.clone()),
                create_user: Some(creator.to_string()),
                update_time: Some(now),
                update_user: None,
            });
        }
        Ok(())
    }

    /// issues/141 G2 写侧判重的读侧（覆写 trait default）：逐行返回，**不加 DISTINCT**
    /// ——判重只看"这个人在这条实例上有没有行"，存量重复行原样留着（owner 2026-09-29 拍）。
    fn find_cc_actor_ids(&self, instance_id: i64) -> JeeflowResult<Vec<String>> {
        let ccs = self.cc_instances.lock().unwrap();
        Ok(ccs.iter().filter(|c| c.process_instance_id == instance_id)
            .map(|c| c.actor_id.clone()).collect())
    }

    fn update_cc_status(&self, instance_id: i64, actor_id: &str) -> JeeflowResult<()> {
        let mut ccs = self.cc_instances.lock().unwrap();
        for cc in ccs.iter_mut() {
            if cc.process_instance_id == instance_id && cc.actor_id == actor_id {
                cc.state = 1;
                // 已读是"人主动读"这个新事实，才动 update_time；重复抄送不动（issues/141 G2 ③）。
                cc.update_time = Some(current_time_str());
            }
        }
        Ok(())
    }

    fn page_todo_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> {
        let tasks = self.tasks.lock().unwrap();
        let instances = self.instances.lock().unwrap();
        let defines = self.defines.lock().unwrap();

        // issues/129：`is_none()` 短路＝放行全库待办（旁路型缺口，比缺过滤器更隐蔽）。
        // 与 page_done_tasks 的 117 姿势同形：operator 为空（None 或全空白）→ 空页。
        let op_s = query.operator.as_deref().map(str::trim).unwrap_or("");
        let mut rows: Vec<TaskRow> = tasks.values()
            .filter(|t| {
                t.task_state == TaskState::Doing.code()
                    && !op_s.is_empty()
                    && t.actor_ids.iter().any(|a| a.as_str() == op_s)
            })
            .map(|t| {
                let inst = instances.get(&t.process_instance_id);
                let define = inst.and_then(|i| defines.get(&i.define_id));
                TaskRow {
                    id: t.task_id,
                    process_instance_id: t.process_instance_id,
                    task_name: t.task_name.clone(),
                    display_name: t.display_name.clone(),
                    task_type: t.task_type,
                    perform_type: t.perform_type,
                    task_state: t.task_state,
                    operator: t.actor_id.clone(),
                    actor_id: t.actor_ids.first().cloned(),
                    finish_time: t.finish_time.clone(),
                    expire_time: t.expire_time.clone(),
                    form_key: t.form_key.clone(),
                    task_parent_id: t.parent_task_id,
                    variable: flow_data_json_string(&t.variables),
                    create_time: t.create_time.clone(),
                    create_user: t.create_user.clone(),
                    update_time: t.update_time.clone(),
                    update_user: t.update_user.clone(),
                    process_define_id: inst.map(|i| i.define_id),
                    instance_state: inst.map(|i| i.state),
                    instance_operator: inst.map(|i| i.operator.clone()),
                    business_no: inst.and_then(|i| i.business_no.clone()),
                    instance_variable: inst.and_then(|i| flow_data_json_string(&i.variables)),
                    instance_create_time: inst.and_then(|i| i.create_time.clone()),
                    define_name: define.map(|d| d.name.clone()),
                    define_display_name: define.map(|d| d.display_name.clone()),
                    define_version: define.map(|d| d.version),
                }
            })
            .collect();

        if !query.filters.is_empty() {
            rows.retain(|r| query.filters.iter().all(|f| task_row_matches(f, r)));
        }
        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() { rows[start..end].to_vec() } else { vec![] };

        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn page_done_tasks(&self, query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> {
        let tasks = self.tasks.lock().unwrap();
        let instances = self.instances.lock().unwrap();
        let defines = self.defines.lock().unwrap();

        // 「我已办」三处判据（issues/117，owner 2026-09-21 拍板；与 sqlx 仓**同判据**，
        // 否则"换仓储就换答案"——正是 issues/116 §5 给委托查询立过的那条病）：
        // ① 状态集合 `task_state <> 10`（六栈家族口径）：语义 = 我经手过且不再是我待办，
        //    含撤回 30 / 终止 40 / 废弃 99。原 `== Finished(20)` 会让撤回单从待办、已办
        //    两头同时消失（issues/113 刚把撤回态从 99 统一成 30）。
        // ② 归属只认 `t.operator`（即 actor_id 列，办结时写入）：契约 §2.5 点名
        //    "不含发起人 create_user，我发起但非我办理不算我的已办"，原实现多认一条
        //    `create_user = operator` 属**偏宽违约**（同一份数据 Java 栈看不到、本栈看得到）。
        // ③ operator 为空（None 或全空白）→ **返回空页**：原 `query.operator.is_none()`
        //    短路会放行全库已办（旁路型缺口，比缺过滤器更隐蔽）。本轮只堵泄漏，
        //    不把 doneList 的 operator 改成硬必填（前端四入口 + drift_gate 待另一轮统一收紧）。
        let op = query.operator.as_deref().map(str::trim).unwrap_or("");
        let mut rows: Vec<TaskRow> = tasks
            .values()
            .filter(|t| {
                t.task_state != TaskState::Doing.code()
                    && !op.is_empty()
                    && t.actor_id.as_deref() == Some(op)
            })
            .map(|t| {
                let inst = instances.get(&t.process_instance_id);
                let define = inst.and_then(|i| defines.get(&i.define_id));
                TaskRow {
                    id: t.task_id,
                    process_instance_id: t.process_instance_id,
                    task_name: t.task_name.clone(),
                    display_name: t.display_name.clone(),
                    task_type: t.task_type,
                    perform_type: t.perform_type,
                    task_state: t.task_state,
                    operator: t.actor_id.clone(),
                    actor_id: t.actor_ids.first().cloned(),
                    finish_time: t.finish_time.clone(),
                    expire_time: t.expire_time.clone(),
                    form_key: t.form_key.clone(),
                    task_parent_id: t.parent_task_id,
                    variable: flow_data_json_string(&t.variables),
                    create_time: t.create_time.clone(),
                    create_user: t.create_user.clone(),
                    update_time: t.update_time.clone(),
                    update_user: t.update_user.clone(),
                    process_define_id: inst.map(|i| i.define_id),
                    instance_state: inst.map(|i| i.state),
                    instance_operator: inst.map(|i| i.operator.clone()),
                    business_no: inst.and_then(|i| i.business_no.clone()),
                    instance_variable: inst.and_then(|i| flow_data_json_string(&i.variables)),
                    instance_create_time: inst.and_then(|i| i.create_time.clone()),
                    define_name: define.map(|d| d.name.clone()),
                    define_display_name: define.map(|d| d.display_name.clone()),
                    define_version: define.map(|d| d.version),
                }
            })
            .collect();

        if !query.filters.is_empty() {
            rows.retain(|r| query.filters.iter().all(|f| task_row_matches(f, r)));
        }
        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() {
            rows[start..end].to_vec()
        } else {
            vec![]
        };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn page_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> {
        let instances = self.instances.lock().unwrap();
        let defines = self.defines.lock().unwrap();
        let mut rows: Vec<InstanceRow> = instances
            .values()
            // issues/129：原 `.unwrap_or(true)`＝"没传就看全部"，一条不带 operator 的
            // page 请求能读到别人的实例（线上实测 4 → 25）。空值一律空页。
            .filter(|i| {
                let op = query.operator.as_deref().map(str::trim).unwrap_or("");
                !op.is_empty() && i.operator == op
            })
            .map(|i| {
                let define = defines.get(&i.define_id);
                instance_row_of(i, define)
            })
            .collect();

        if !query.filters.is_empty() {
            rows.retain(|r| query.filters.iter().all(|f| instance_row_matches(f, r)));
        }
        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() {
            rows[start..end].to_vec()
        } else {
            vec![]
        };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    /// issues/138 · ccList 行形状三要件（spec 06 §processInstance/ccList；基准＝boot2 内置版
    /// `JdbcProcessRepository.pageInstances(cc = true)`：`FROM wf_process_instance t
    /// LEFT JOIN wf_process_cc_instance cc ON t.id = cc.process_instance_id` + `SELECT … t.*`
    /// ⇒ **行源始终是实例表**，cc 表只做关联/过滤）。
    ///
    /// 旧实现直接把 cc 行原样投成 `InstanceRow`，三要件全违：
    ///   ① `id = cc.id`（cc 表主键，不是实例 id）；
    ///   ③ `operator = cc.actor_id`（被抄送人）——在被抄送人自己的查询里那一列恒等于他自己，
    ///      拿"非空"当判据永远照不出来，所以判据必须是**值对到具体那一方**；
    ///   附带 `state = cc.state`（0/1 已读位）冒充实例状态、`create_*/update_*` 投的是 cc 的。
    ///
    /// 归属谓词列不动：`query.operator` 依旧比 `cc.actor_id`（spec 06 §2.5 口径表钉的那一列，
    /// 也是 issues/129"空值即空页"守住的同一列），只是命中之后**换成实例行**输出。
    /// 其余语义与 `jeeflow-repository-sqlx::page_cc_instances` 逐条对齐（那边本就合规，本轮不动）：
    ///   · COUNT(DISTINCT pi.id) —— 同一实例多条 cc 命中同一接收人只出一行；
    ///   · 投影复用 `instance_row_of`，与 `page_instances` 同一函数；
    ///   · cc 行自己的 `create_user`（"这条抄送是谁发给我的"）按条文**不得占用 `operator` 键名**，
    ///     而实例行结构里也没有它的槽位 ⇒ 不透出（与 sqlx / boot2 的 `t.*` 同形）。
    /// 唯一分叉：孤儿 cc（实例不在）这里出**降级行**、sqlx 的 INNER JOIN 直接丢弃，理由见函数体注释。
    ///
    /// **issues/141 G1 归属条件必填**（spec 06 §2.5）：判据一律走
    /// [`crate::model::has_effective_cc_ownership`]，与 `SqlxRepository::page_cc_instances`
    /// 同一条——归属列 `cc.actor_id` 没给有效条件（整条没给 / 空值）⇒ **空页**，
    /// 不得退化成"这条不加"把实例摊出去。非归属列的空值放行语义不变。
    fn page_cc_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> {
        // issues/129（空 operator 不得折叠成"看全部"）＋ issues/141 G1（条件整条没给同样空页，
        // 且 `m_cc_actorId` 这条通道也算归属条件）：判据收在 has_effective_cc_ownership 一支里。
        if !crate::model::has_effective_cc_ownership(query) {
            return Ok(PageResult::new(query.page_num, query.page_size, 0, vec![]));
        }
        let op = query.operator.as_deref().map(str::trim).unwrap_or("");
        // 归属列上的 m_ 条件打在 cc.actor_id 上（sqlx 那边同一条：白名单把它解析成 cc.actor_id）；
        // 其余条件继续打在实例行上（issues/106 白名单语义不变）。
        let cc_filters = query.cc_ownership_filters();
        let row_filters = query.non_cc_ownership_filters();
        let ccs = self.cc_instances.lock().unwrap();
        let instances = self.instances.lock().unwrap();
        let defines = self.defines.lock().unwrap();
        let mut seen_instance_ids: Vec<i64> = Vec::new();
        let mut rows: Vec<InstanceRow> = Vec::new();
        for cc in ccs.iter() {
            if !op.is_empty() && cc.actor_id != op {
                continue;
            }
            if !cc_filters.iter()
                .all(|f| crate::filter_sql::op_matches(&f.op, &cc.actor_id, &f.value)) {
                continue;
            }
            if seen_instance_ids.contains(&cc.process_instance_id) {
                continue; // DISTINCT pi.id
            }
            seen_instance_ids.push(cc.process_instance_id);
            match instances.get(&cc.process_instance_id) {
                Some(inst) => rows.push(instance_row_of(inst, defines.get(&inst.define_id))),
                // 降级档：cc 指向的实例不存在（内存夹具用假 id ／ 实例已删）⇒ 没有实例行可投。
                // 主键仍挂**实例 id**（`cc.process_instance_id`），`operator` 留空——空值在门禁格
                // `judge_cc_row_shape` 里必判红（值对不到任何一方），绝不拿 `actor_id`/`create_user`
                // 冒充流程发起人。此处与 sqlx 有别（那边 INNER JOIN 直接丢孤儿 cc）：既有门面事件腿格
                // （facade lib.rs `test_i132_manual_cc_create_payload_and_timing`，假 id 2002）
                // 拿"归属档位的行数"当"cc 行已落库"的证据，本轮不动那一档的读数。分叉已上报。
                None => rows.push(InstanceRow { id: cc.process_instance_id, ..Default::default() }),
            }
        }
        if !row_filters.is_empty() {
            rows.retain(|r| row_filters.iter().all(|f| instance_row_matches(*f, r)));
        }
        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() { rows[start..end].to_vec() } else { vec![] };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn page_defines(&self, query: &PageQuery) -> JeeflowResult<PageResult<DefineRow>> {
        let defines = self.defines.lock().unwrap();
        let mut rows: Vec<DefineRow> = defines.values().map(|d| DefineRow {
            id: d.id,
            name: d.name.clone(),
            display_name: d.display_name.clone(),
            define_type: d.define_type.clone(),
            state: d.state,
            version: d.version,
            create_time: d.create_time.clone(),
            create_user: d.create_user.clone(),
            update_time: d.update_time.clone(),
            update_user: d.update_user.clone(),
        }).collect();
        if !query.filters.is_empty() {
            rows.retain(|r| query.filters.iter().all(|f| define_row_matches(f, r)));
        }
        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() { rows[start..end].to_vec() } else { vec![] };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn count_todo_tasks(&self, user_id: &str) -> JeeflowResult<i64> {
        let tasks = self.tasks.lock().unwrap();
        let count = tasks.values()
            .filter(|t| t.task_state == TaskState::Doing.code() && t.actor_ids.contains(&user_id.to_string()))
            .count() as i64;
        Ok(count)
    }

    fn get_all_instances(&self) -> JeeflowResult<Vec<ProcessInstance>> {
        let instances = self.instances.lock().unwrap();
        Ok(instances.values().cloned().collect())
    }

    fn get_all_tasks(&self) -> JeeflowResult<Vec<ProcessTask>> {
        let tasks = self.tasks.lock().unwrap();
        let task_actors = self.task_actors.lock().unwrap();
        Ok(tasks.values().map(|t| {
            let mut task = t.clone();
            if task.actor_ids.is_empty() {
                if let Some(actors) = task_actors.get(&t.task_id) {
                    task.actor_ids = actors.clone();
                }
            }
            task
        }).collect())
    }
}

impl ProcessExtRepository for MemoryRepository {
    fn find_design_by_id(&self, design_id: i64) -> JeeflowResult<Option<ProcessDesign>> {
        let designs = self.designs.lock().unwrap();
        Ok(designs.get(&design_id).cloned())
    }

    fn save_design(&self, design: &mut ProcessDesign) -> JeeflowResult<()> {
        if design.id == 0 { design.id = self.next_id(); }
        let mut designs = self.designs.lock().unwrap();
        designs.insert(design.id, design.clone());
        Ok(())
    }

    fn update_design(&self, design: &ProcessDesign) -> JeeflowResult<()> {
        let mut designs = self.designs.lock().unwrap();
        designs.insert(design.id, design.clone());
        Ok(())
    }

    fn remove_design(&self, design_id: i64) -> JeeflowResult<()> {
        let mut designs = self.designs.lock().unwrap();
        designs.remove(&design_id);
        Ok(())
    }

    fn page_designs(&self, query: &PageQuery) -> JeeflowResult<PageResult<ProcessDesign>> {
        let designs = self.designs.lock().unwrap();
        let mut rows: Vec<ProcessDesign> = designs.values().cloned().collect();
        if !query.filters.is_empty() {
            rows.retain(|r| query.filters.iter().all(|f| design_row_matches(f, r)));
        }
        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() { rows[start..end].to_vec() } else { vec![] };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    fn save_design_his(&self, his: &mut ProcessDesignHis) -> JeeflowResult<()> {
        if his.id == 0 { his.id = self.next_id(); }
        let mut h = self.design_his.lock().unwrap();
        h.push(his.clone());
        Ok(())
    }

    fn list_design_his(&self, design_id: i64) -> JeeflowResult<Vec<ProcessDesignHis>> {
        let h = self.design_his.lock().unwrap();
        // Newest first (index 0) — align Java JDBC order / design detail jsonObject
        let mut list: Vec<ProcessDesignHis> = h
            .iter()
            .filter(|d| d.process_design_id == design_id)
            .cloned()
            .collect();
        list.reverse();
        Ok(list)
    }

    fn find_surrogate_by_id(&self, surrogate_id: i64) -> JeeflowResult<Option<ProcessSurrogate>> {
        let s = self.surrogates.lock().unwrap();
        Ok(s.get(&surrogate_id).cloned())
    }

    fn save_surrogate(&self, surrogate: &mut ProcessSurrogate) -> JeeflowResult<()> {
        if surrogate.id == 0 { surrogate.id = self.next_id(); }
        let mut s = self.surrogates.lock().unwrap();
        s.insert(surrogate.id, surrogate.clone());
        Ok(())
    }

    fn update_surrogate(&self, surrogate: &ProcessSurrogate) -> JeeflowResult<()> {
        let mut s = self.surrogates.lock().unwrap();
        s.insert(surrogate.id, surrogate.clone());
        Ok(())
    }

    fn remove_surrogate(&self, surrogate_id: i64) -> JeeflowResult<()> {
        let mut s = self.surrogates.lock().unwrap();
        s.remove(&surrogate_id);
        Ok(())
    }

    fn page_surrogates(&self, query: &PageQuery) -> JeeflowResult<PageResult<ProcessSurrogate>> {
        let s = self.surrogates.lock().unwrap();
        let rows: Vec<ProcessSurrogate> = s.values().cloned().collect();
        let total = rows.len() as i64;
        let start = ((query.page_num - 1) * query.page_size) as usize;
        let end = std::cmp::min(start + query.page_size as usize, rows.len());
        let page_rows = if start < rows.len() { rows[start..end].to_vec() } else { vec![] };
        Ok(PageResult::new(query.page_num, query.page_size, total, page_rows))
    }

    /// 委托查询（四判据见 `crate::surrogate`，与 sqlx 仓必须同答案，契约 06 §4.5 条款 5/6）。
    ///
    /// 判序（条款 1.4 的正确形状，issues/123）：由 [`crate::surrogate::pick_surrogate`] 完成——
    /// **每个作用域各自先按主键 id 取最新一条，再交四判据裁决那一条**；不得"先按 enabled/窗口/
    /// 自委托滤掉候选、再从剩下的取最新"（那等于上一条窗内委托把用户后续设置永久盖掉）。
    ///
    /// 修复前本方法的三处欠账（issues/116 §5）：
    /// - 形参 `_time` **直接忽略时间窗** → 同一份数据 SQL 仓判窗外、内存仓判命中，
    ///   换仓储就换答案（现改为把 `time` 传给判据，按 `yyyy-MM-dd HH:mm:ss` 归一比较）；
    /// - 无自委托过滤 `surrogate <> operator` → 会命中"自己委托给自己"；
    /// - 多条命中按 `HashMap` **随机遍历序取首条** → 违条款 1.4（SQL 侧 `ORDER BY id DESC`），
    ///   现统一由 [`crate::surrogate::pick_surrogate`] 取 id 最大者；
    /// - 缺空 processName 全流程兜底（只认 `process_name == process_name` 精确相等）→
    ///   现"精确作用域取最新一条交裁决，判否/无记录再看全流程作用域的最新一条"两腿查。
    fn get_surrogate(&self, operator: &str, process_name: &str, time: &str) -> JeeflowResult<Option<ProcessSurrogate>> {
        let s = self.surrogates.lock().unwrap();
        Ok(crate::surrogate::pick_surrogate(
            s.values(),
            operator,
            process_name,
            time,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json::FlowData;

    #[test]
    fn test_define_crud() {
        let repo = MemoryRepository::new();
        let mut define = ProcessDefine {
            id: 0, name: "test".into(), display_name: "Test".into(),
            define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
            version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_define(&mut define).unwrap();
        assert!(define.id > 0);

        let found = repo.find_define_by_id(define.id).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().name, "test");

        repo.update_define_state(define.id, 0).unwrap();
        let found = repo.find_define_by_id(define.id).unwrap().unwrap();
        assert_eq!(found.state, 0);

        repo.remove_define(define.id).unwrap();
        let found = repo.find_define_by_id(define.id).unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn test_instance_and_tasks() {
        let repo = MemoryRepository::new();
        let define = ProcessDefine {
            id: 1, name: "test".into(), display_name: "Test".into(),
            define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
            version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        let mut defines = repo.defines.lock().unwrap();
        defines.insert(1, define);
        drop(defines);

        let mut inst = ProcessInstance {
            instance_id: 0, parent_id: None, define_id: 1, state: 10,
            parent_node_name: None, business_no: None, operator: "user1".into(),
            expire_time: None, variables: FlowData::new(), tasks: vec![],
            create_time: None, create_user: Some("user1".into()),
            update_time: None, update_user: None, define: None,
        };
        repo.save_instance(&mut inst).unwrap();
        assert!(inst.instance_id > 0);

        let mut task = ProcessTask {
            task_id: 0, process_instance_id: inst.instance_id,
            task_name: "apply".into(), display_name: "Apply".into(),
            task_type: 0, perform_type: 0, task_state: 10,
            actor_id: None, actor_ids: vec!["user1".into()],
            finish_time: None, expire_time: None, form_key: None,
            parent_task_id: None, variables: FlowData::new(),
            create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_task(&mut task).unwrap();
        assert!(task.task_id > 0);

        // Find doing tasks
        let doing = repo.find_doing_tasks(inst.instance_id, &[]).unwrap();
        assert_eq!(doing.len(), 1);

        // Add actor
        repo.add_task_actor(task.task_id, &["user2".into()]).unwrap();
        let actors = repo.find_task_actors(task.task_id).unwrap();
        assert!(actors.contains(&"user2".into()));

        // Count todo
        let count = repo.count_todo_tasks("user1").unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_cc_instance() {
        let repo = MemoryRepository::new();
        repo.create_cc_instance(1, "user1", &["user2".into(), "user3".into()]).unwrap();
        // CC created successfully (no error)
        repo.update_cc_status(1, "user2").unwrap();
        // Status updated
    }

    #[test]
    fn test_design_crud() {
        let repo = MemoryRepository::new();
        let mut design = ProcessDesign {
            id: 0, name: "design1".into(), display_name: "Design 1".into(),
            design_type: "approval".into(), icon: None, is_deployed: 0,
            remark: None, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_design(&mut design).unwrap();
        assert!(design.id > 0);

        let found = repo.find_design_by_id(design.id).unwrap();
        assert!(found.is_some());

        repo.remove_design(design.id).unwrap();
        let found = repo.find_design_by_id(design.id).unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn test_surrogate_crud() {
        let repo = MemoryRepository::new();
        let mut sg = ProcessSurrogate {
            id: 0, process_name: "test".into(), operator: "user1".into(),
            surrogate: "user2".into(), start_time: None, end_time: None,
            enabled: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_surrogate(&mut sg).unwrap();
        assert!(sg.id > 0);

        let found = repo.get_surrogate("user1", "test", "NOW").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().surrogate, "user2");
    }

    /// issues/116 条款 6：**双仓对拍**——内存仓跑与 sqlx 真机仓完全同一张判据矩阵
    /// （`crate::surrogate::parity`，14 行数据 × 15 组期望）。
    /// sqlx 侧用例：`jeeflow-repository-sqlx/src/lib.rs::test_mysql_i116_surrogate_query_parity`。
    /// 两侧各写一套断言迟早漂移，故共用一份"数据 + 期望"。
    #[test]
    fn test_get_surrogate_parity_matrix_i116() {
        use crate::surrogate::parity;
        let repo = MemoryRepository::new();
        for row in parity::ROWS {
            let mut sg = parity::memory_row(row);
            repo.save_surrogate(&mut sg).unwrap();
            assert_eq!(sg.id, row.id, "预置 id 必须保留（判据期望按 id 对账）");
        }
        for exp in parity::EXPECT {
            let got = repo
                .get_surrogate(exp.operator, exp.process_name, exp.time)
                .unwrap()
                .map(|h| h.id);
            assert_eq!(
                got, exp.hit_id,
                "判据矩阵[{}] operator={} process_name={:?} time={:?} → 期望 {:?} 实得 {:?}",
                exp.note, exp.operator, exp.process_name, exp.time, exp.hit_id, got
            );
        }
    }

    /// issues/123 A 格（内存仓）：同一 operator+processName 先落"窗内+enabled=1"，
    /// 再落一条更"新"的无效记录（窗外已过期 / 窗外未开始 / enabled=0 / 脏值 2 / 自委托）
    /// ⇒ 判否，**旧的那条有效记录不得复活**。sqlx 侧同数据同期望：
    /// `jeeflow-repository-sqlx::test_mysql_i123_*`；共用矩阵见 `surrogate::parity`。
    #[test]
    fn test_get_surrogate_i123_newest_invalid_beats_older_valid() {
        let repo = MemoryRepository::new();
        let now = "2026-09-21 12:00:00";
        let base = ProcessSurrogate {
            id: 0, process_name: "leave".into(), operator: "zs".into(),
            surrogate: "agent_old_valid".into(),
            start_time: Some("2026-09-01 00:00:00".into()),
            end_time: Some("2026-09-30 23:59:59".into()),
            enabled: 1, create_time: None, create_user: None, update_time: None, update_user: None,
        };
        let mut older_valid = base.clone();
        repo.save_surrogate(&mut older_valid).unwrap();

        let cases: Vec<(ProcessSurrogate, &str)> = vec![
            {
                let mut s = base.clone();
                s.surrogate = "agent_new_expired".into();
                s.start_time = Some("2020-01-01 00:00:00".into());
                s.end_time = Some("2020-12-31 23:59:59".into());
                (s, "窗外（已过期）")
            },
            {
                let mut s = base.clone();
                s.surrogate = "agent_new_notstarted".into();
                s.start_time = Some("2030-01-01 00:00:00".into());
                s.end_time = Some("2030-12-31 23:59:59".into());
                (s, "窗外（未开始）")
            },
            {
                let mut s = base.clone();
                s.surrogate = "agent_new_off".into();
                s.enabled = 0;
                (s, "enabled=0")
            },
            {
                let mut s = base.clone();
                s.surrogate = "agent_new_dirty".into();
                s.enabled = 2;
                (s, "enabled 脏值 2（只认 1）")
            },
            {
                let mut s = base.clone();
                s.surrogate = "zs".into();
                (s, "自委托")
            },
        ];
        for (mut fresh, why) in cases {
            repo.save_surrogate(&mut fresh).unwrap();
            assert!(fresh.id > older_valid.id, "新行 id 必须更大（用例前提：它是最新一条）");
            let got = repo.get_surrogate("zs", "leave", now).unwrap().map(|h| h.id);
            assert_eq!(
                got, None,
                "内存仓：最新一条{}（agent={} enabled={}）不生效时不得命中，旧的有效行（id={}）不得复活",
                why, fresh.surrogate, fresh.enabled, older_valid.id,
            );
            repo.remove_surrogate(fresh.id).unwrap();
        }
    }

    /// issues/123 B 格（内存仓）：作用域内**只有一条**"窗内+enabled=1" ⇒ 必须命中
    /// （防 A 格的修法被写成恒不并入）。
    #[test]
    fn test_get_surrogate_i123_sole_valid_row_still_hits() {
        let repo = MemoryRepository::new();
        let mut only = ProcessSurrogate {
            id: 0, process_name: "leave".into(), operator: "zs2".into(),
            surrogate: "agent_only".into(),
            start_time: Some("2026-09-01 00:00:00".into()),
            end_time: Some("2026-09-30 23:59:59".into()),
            enabled: 1, create_time: None, create_user: None, update_time: None, update_user: None,
        };
        repo.save_surrogate(&mut only).unwrap();
        let hit = repo.get_surrogate("zs2", "leave", "2026-09-21 12:00:00").unwrap();
        assert_eq!(
            hit.as_ref().map(|h| h.surrogate.as_str()),
            Some("agent_only"),
            "唯一一条有效委托必须命中（内存仓精确腿）"
        );

        // 全流程腿同样要接住：唯一一条空 processName 的有效委托
        let repo2 = MemoryRepository::new();
        let mut only_all = only.clone();
        only_all.id = 0;
        only_all.operator = "zs3".into();
        only_all.process_name = String::new();
        only_all.surrogate = "agent_all_only".into();
        repo2.save_surrogate(&mut only_all).unwrap();
        let hit = repo2.get_surrogate("zs3", "leave", "2026-09-21 12:00:00").unwrap();
        assert_eq!(
            hit.as_ref().map(|h| h.surrogate.as_str()),
            Some("agent_all_only"),
            "唯一一条全流程有效委托必须命中（内存仓兜底腿）"
        );
    }

    /// 「我已办」判据（issues/117，owner 2026-09-21 拍板三处一起改）：
    /// ① 状态集合 `task_state <> 10`（20/30/40/99 全进，10 不进）；
    /// ② 只按 `operator` 归属，**不认 create_user**（契约 §2.5 点名禁止偏宽）；
    /// ③ operator 为空（None / 全空白）→ 空页（原 `is_none()` 短路会放行全库已办）。
    /// sqlx 同判据用例：`test_mysql_i117_done_list_predicates`。
    #[test]
    fn test_page_done_tasks_predicates_i117() {
        let repo = MemoryRepository::new();
        let mk = |id: i64, state: i32, operator: Option<&str>, create_user: &str| ProcessTask {
            task_id: id, process_instance_id: 7001, task_name: format!("t{}", id),
            display_name: "T".into(), task_type: 0, perform_type: 0, task_state: state,
            actor_id: operator.map(str::to_string), actor_ids: vec![operator.unwrap_or("me").to_string()],
            finish_time: None, expire_time: None, form_key: None, parent_task_id: None,
            variables: FlowData::new(), create_time: None,
            create_user: Some(create_user.to_string()), update_time: None, update_user: None,
        };
        // me 经手的四种非待办态：20 已完成 / 30 已撤回 / 40 已终止 / 99 已废弃
        repo.save_task(&mut mk(8001, TaskState::Finished.code(), Some("me"), "me")).unwrap();
        repo.save_task(&mut mk(8002, TaskState::Withdraw.code(), Some("me"), "me")).unwrap();
        repo.save_task(&mut mk(8003, TaskState::Interrupt.code(), Some("me"), "me")).unwrap();
        repo.save_task(&mut mk(8004, TaskState::Abandon.code(), Some("me"), "other")).unwrap();
        // 诱饵 1：me 是发起人但**不是办理人** → 不得进 me 的已办（原 create_user 旁路会捞进来）
        repo.save_task(&mut mk(8005, TaskState::Finished.code(), Some("other"), "me")).unwrap();
        // 诱饵 2：me 名下的进行中任务 → 属待办不属已办
        repo.save_task(&mut mk(8006, TaskState::Doing.code(), Some("me"), "me")).unwrap();
        // 诱饵 3：脏空串办理人 → operator 传空白时不得被 `= ''` 摊给调用方（sqlx 同尺子 901129）
        repo.save_task(&mut mk(8007, TaskState::Finished.code(), Some(""), "me")).unwrap();

        let ids_for = |op: Option<&str>| {
            let mut q = PageQuery::new(1, 50);
            q.operator = op.map(str::to_string);
            let page = repo.page_done_tasks(&q).unwrap();
            assert_eq!(page.record_count as usize, page.rows.len(), "total 须与本页行数自洽");
            let mut ids: Vec<i64> = page.rows.iter().map(|r| r.id).collect();
            ids.sort();
            ids
        };

        assert_eq!(ids_for(Some("me")), vec![8001, 8002, 8003, 8004],
            "20/30/40/99 都算我已办，且发起人诱饵 8005 / 进行中 8006 不得混入");
        assert_eq!(ids_for(Some("other")), vec![8005], "办理人口径不受影响");
        assert_eq!(ids_for(None), Vec::<i64>::new(), "operator 缺省不得返回全库已办");
        assert_eq!(ids_for(Some("   ")), Vec::<i64>::new(), "operator 全空白同空值：空页");
    }
}

// ═══════════════════════════════════════════════════════
// issues/138 · ccList 行形状三要件（内存仓储这一支；基准＝boot2 内置版 JdbcProcessRepository）
// 独立 module ＋ i138 前缀函数名，避免与同文件既有 `tests`、engine.rs `event_leg_tests` 撞名。
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod cc_row_shape_i138_tests {
    use super::*;
    use crate::json::FlowData;

    const APPLICANT: &str = "i138_applicant";
    const CC_ACTOR: &str = "i138_cc_actor";
    const CC_ACTOR_2: &str = "i138_other_actor";
    /// 发起这次抄送的人（cc 行自己的 create_user）。条文把它点名为错法②的落点：
    /// 发起人 / 被抄送人 / 抄送发送人三方必须两两不同，值断言才照得出来。
    const CC_SENDER: &str = "i138_cc_sender";

    struct Fixture {
        repo: MemoryRepository,
        define_id: i64,
        instance_id: i64,
        cc_row_id: i64,
    }

    /// 一条实例（发起人 APPLICANT）＋两条 cc 行（接收人 CC_ACTOR / CC_ACTOR_2，发送人 CC_SENDER）。
    fn seed_i138() -> Fixture {
        let repo = MemoryRepository::new();
        let mut define = ProcessDefine {
            id: 0, name: "i138-flow".into(), display_name: "I138 Flow".into(),
            define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
            version: 7, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_define(&mut define).unwrap();

        let mut inst = ProcessInstance {
            instance_id: 0, parent_id: None, define_id: define.id, state: 10,
            parent_node_name: None, business_no: Some("i138-biz-no".into()),
            operator: APPLICANT.into(), expire_time: None, variables: FlowData::new(),
            tasks: vec![], create_time: Some("2026-09-29 10:00:00".into()),
            create_user: Some(APPLICANT.into()), update_time: None, update_user: None,
            define: None,
        };
        repo.save_instance(&mut inst).unwrap();
        let instance_id = inst.instance_id;

        repo.create_cc_instance(instance_id, CC_SENDER, &[CC_ACTOR.into()]).unwrap();
        let cc_row_id = repo.cc_instances.lock().unwrap().last().expect("cc 行应已落库").id;
        repo.create_cc_instance(instance_id, CC_SENDER, &[CC_ACTOR_2.into()]).unwrap();

        assert_ne!(cc_row_id, instance_id, "夹具前提：cc 表主键与实例 id 是两个不同的值");
        Fixture { repo, define_id: define.id, instance_id, cc_row_id }
    }

    fn cc_rows_of(repo: &MemoryRepository, actor: &str) -> Vec<InstanceRow> {
        let mut q = PageQuery::new(1, 50);
        q.operator = Some(actor.to_string());
        let page = repo.page_cc_instances(&q).unwrap();
        assert_eq!(page.record_count as usize, page.rows.len(), "total 须与本页行数自洽");
        page.rows
    }

    fn eq_filter(column: &str, value: &str) -> QueryFilter {
        QueryFilter { alias: "t".into(), op: FilterOp::Eq, column: column.into(), value: value.into() }
    }

    /// 三要件判据，与集成层门禁格 runner.py::judge_cc_row_shape 同判，不自创第四种形状：
    /// ① 行来自实例表 ⇒ 主键键名 id 且**值＝实例 id**；
    /// ② 必须有 operator 键；
    /// ③ operator 的值＝流程发起人，且**不得等于查询者本人**（等于＝投了 cc.actor_id，
    ///    在被抄送人自己的查询里恒等，"非空"判据永远照不出来 ⇒ 只能比"值对到具体那一方"）。
    /// 内存仓这一层 InstanceRow 的 id/operator 是必填字段（不可能缺键），故 ① 退成同判据的
    /// "值＝实例 id"形式、② 的键名维度打在出口 JSON 上
    /// （见 jeeflow-facade/tests/cc_list_row_shape_i138.rs）。
    fn judge_cc_row_shape(
        row: Option<&InstanceRow>,
        instance_id: i64,
        applicant: &str,
        cc_actor: &str,
    ) -> Vec<String> {
        let Some(row) = row else {
            return vec!["ccList 里找不到这一行（抄送数据腿本身没建 cc 行，形状无从判）".to_string()];
        };
        let mut reasons = Vec::new();
        if row.id != instance_id {
            reasons.push(format!("行主键 id={} 不是实例 id={} ⇒ 行来自 cc 表而不是实例表", row.id, instance_id));
        }
        let op = row.operator.trim();
        if op == cc_actor {
            reasons.push(format!("operator={} 是**被抄送人**（＝查询者本人，恒等）——基准要的是流程发起人；投 cc.actor_id 的栈在这一档永远自等，所以判据只能比值对到具体那一方", op));
        } else if !applicant.is_empty() && op != applicant {
            reasons.push(format!("operator={} 既不是流程发起人 {}、也不是被抄送人 ⇒ 投了别的列（多半是 cc.create_user）", op, applicant));
        }
        reasons
    }

    /// 正向：三要件全过（查询者＝被抄送人本人，这一档正是旧形状藏身的地方）。
    #[test]
    fn test_i138_cc_row_shape_three_requirements() {
        let f = seed_i138();
        let rows = cc_rows_of(&f.repo, CC_ACTOR);
        let reasons = judge_cc_row_shape(rows.first(), f.instance_id, APPLICANT, CC_ACTOR);
        assert!(reasons.is_empty(), "ccList 行形状三要件不合格：{:?}｜实得 {:?}", reasons, rows.first());

        let row = rows.first().unwrap();
        assert_eq!(row.id, f.instance_id, "要件①：id＝实例 id");
        assert_ne!(row.id, f.cc_row_id, "要件①：id 不得是 cc 表主键");
        assert_eq!(row.operator, APPLICANT, "要件③：operator＝流程发起人");
        assert_ne!(row.operator, CC_ACTOR, "要件③：operator 不得等于查询者本人");
        assert_ne!(row.operator, CC_SENDER, "要件③：operator 不得被 cc.create_user 占用");
        // 同判据的延伸：行既来自实例表，其余列也该是实例行的列（不是 cc 的已读位/时间戳）。
        assert_eq!(row.state, 10, "state 应是实例状态，不得投 cc 的 0/1 已读位");
        assert_eq!(row.process_define_id, f.define_id, "process_define_id 应来自实例");
        assert_eq!(row.business_no.as_deref(), Some("i138-biz-no"));
        assert_eq!(row.define_name.as_deref(), Some("i138-flow"));
        assert_eq!(row.define_display_name.as_deref(), Some("I138 Flow"));
        assert_eq!(row.define_version, Some(7));
        assert_eq!(row.create_user.as_deref(), Some(APPLICANT), "create_user 是实例的，不是 cc 的");
    }

    /// 负向自证（对应门禁格 --selftest-l2-31）：三种错法形状各自敢报红，基准形状必须绿。
    /// 变异自证 M1（operator←cc.actor_id）／M2（去 id、投 cc 表原样）正打在前两条上；
    /// 本格在共享树里长期保证"判据本身不哑"。
    #[test]
    fn test_i138_cc_row_shape_judge_rejects_wrong_shapes() {
        let f = seed_i138();
        let ok = InstanceRow { id: f.instance_id, operator: APPLICANT.into(), ..Default::default() };
        let m1_actor = InstanceRow { id: f.instance_id, operator: CC_ACTOR.into(), ..Default::default() };
        let m2_cc_pk = InstanceRow { id: f.cc_row_id, operator: APPLICANT.into(), ..Default::default() };
        let third = InstanceRow { id: f.instance_id, operator: CC_SENDER.into(), ..Default::default() };

        assert!(judge_cc_row_shape(Some(&ok), f.instance_id, APPLICANT, CC_ACTOR).is_empty(),
            "基准形状必须判绿");
        assert!(!judge_cc_row_shape(Some(&m1_actor), f.instance_id, APPLICANT, CC_ACTOR).is_empty(),
            "错法①（operator=被抄送人）必须判红");
        assert!(!judge_cc_row_shape(Some(&m2_cc_pk), f.instance_id, APPLICANT, CC_ACTOR).is_empty(),
            "错法③（主键投成 cc 表 id）必须判红");
        assert!(!judge_cc_row_shape(Some(&third), f.instance_id, APPLICANT, CC_ACTOR).is_empty(),
            "错法②（operator=cc.create_user 第三者）必须判红");
        assert!(!judge_cc_row_shape(None, f.instance_id, APPLICANT, CC_ACTOR).is_empty(),
            "零行必须判红（不得装绿）");
    }

    /// 「rows 同 processInstance/page 行结构」由构造保证：同一实例在两张列表里逐字段同形。
    /// InstanceRow 未 derive PartialEq，故用 Debug 渲染整行比对（等价于逐字段 assert_eq）。
    #[test]
    fn test_i138_cc_row_shape_identical_to_page_instances_row() {
        let f = seed_i138();
        let mut iq = PageQuery::new(1, 50);
        iq.operator = Some(APPLICANT.into());
        let mine = f.repo.page_instances(&iq).unwrap();
        assert_eq!(mine.rows.len(), 1, "夹具：发起人应有 1 条实例");

        let cc_rows = cc_rows_of(&f.repo, CC_ACTOR);
        assert_eq!(format!("{:?}", cc_rows), format!("{:?}", mine.rows),
            "ccList 行必须与 processInstance/page 行结构逐字段同形（共用同一投影 instance_row_of）");
    }

    /// 两个被抄送人各查各的：operator 恒等于流程发起人，且不等于查询者自己。
    /// 旧形状（投 actor_id）在这里会各自自等 ⇒ 该格是"值对到具体那一方"的正面证据。
    #[test]
    fn test_i138_cc_row_shape_per_actor_operator_is_applicant() {
        let f = seed_i138();
        for actor in [CC_ACTOR, CC_ACTOR_2] {
            let rows = cc_rows_of(&f.repo, actor);
            assert_eq!(rows.len(), 1, "抄送人 {} 应有 1 行", actor);
            assert_eq!(rows[0].operator, APPLICANT, "抄送人 {} 那行的 operator＝流程发起人", actor);
            assert_ne!(rows[0].operator, actor, "抄送人 {} 那行的 operator 不得等于他自己", actor);
            assert_eq!(rows[0].id, f.instance_id, "抄送人 {} 那行的主键＝实例 id", actor);
        }
    }

    /// 回归（issues/129）：空 operator 仍是空页——本轮只动投影，没动归属谓词的档位语义。
    #[test]
    fn test_i138_cc_row_shape_empty_operator_still_empty_page() {
        let f = seed_i138();
        let none = f.repo.page_cc_instances(&PageQuery::new(1, 50)).unwrap();
        assert_eq!(none.record_count, 0, "不传 operator 不得读到别人的抄送");
        assert!(none.rows.is_empty());
        assert!(cc_rows_of(&f.repo, "   ").is_empty(), "operator 全空白同空值：空页");
    }

    /// 回归：同一实例多条 cc 命中同一接收人只出一行（对齐 sqlx 的 COUNT(DISTINCT pi.id)）。
    /// 孤儿 cc（实例不在）出**降级行**：主键仍挂实例 id、`operator` 留空 ⇒ 判据必判红，
    /// 既不冒充发起人，也不把"cc 行已落库"那一档的读数改成零（门面事件腿既有格依赖它）。
    /// 与 sqlx 的 INNER JOIN（丢弃孤儿 cc）是本维度的已知分叉，已上报。
    #[test]
    fn test_i138_cc_row_shape_dedup_and_orphan_cc() {
        let f = seed_i138();
        f.repo.create_cc_instance(f.instance_id, CC_SENDER, &[CC_ACTOR.into()]).unwrap();
        f.repo.create_cc_instance(f.instance_id, CC_SENDER, &[CC_ACTOR.into()]).unwrap();
        let rows = cc_rows_of(&f.repo, CC_ACTOR);
        assert_eq!(rows.len(), 1, "同一实例重复抄送同一人 ⇒ DISTINCT pi.id，只出一行");
        assert_eq!(rows[0].id, f.instance_id);

        let ghost_id = 999_999;
        f.repo.create_cc_instance(ghost_id, CC_SENDER, &[CC_ACTOR.into()]).unwrap();
        let rows = cc_rows_of(&f.repo, CC_ACTOR);
        assert_eq!(rows.len(), 2, "孤儿 cc 仍占归属档位（读数不变）");
        let degraded = rows.iter().find(|r| r.id == ghost_id).expect("孤儿 cc 应出降级行");
        assert_eq!(degraded.operator, "", "降级行不得拿 actor_id/create_user 冒充发起人");
        assert!(!judge_cc_row_shape(Some(degraded), ghost_id, APPLICANT, CC_ACTOR).is_empty(),
            "降级行必须判红：无实例行 ⇒ 三要件③不成立，不得装绿");
        // 真实例那一行不受影响
        let real = rows.iter().find(|r| r.id == f.instance_id).unwrap();
        assert!(judge_cc_row_shape(Some(real), f.instance_id, APPLICANT, CC_ACTOR).is_empty());
    }

    /// m_ 过滤打在实例列上（issues/106 白名单）：投影换源后过滤语义不受影响，
    /// 且 m_operator 现在过滤的是"流程发起人"，不再是查询者自己。
    #[test]
    fn test_i138_cc_row_shape_m_filter_on_instance_columns() {
        let f = seed_i138();
        let hit = |column: &str, value: &str| -> Vec<i64> {
            let mut q = PageQuery::new(1, 50);
            q.operator = Some(CC_ACTOR.into());
            q.filters = vec![eq_filter(column, value)];
            f.repo.page_cc_instances(&q).unwrap().rows.iter().map(|r| r.id).collect()
        };
        assert_eq!(hit("operator", APPLICANT), vec![f.instance_id],
            "m_operator 过滤的是实例 operator＝流程发起人（旧形状下它恒等于查询者）");
        assert_eq!(hit("business_no", "i138-biz-no"), vec![f.instance_id]);
        assert_eq!(hit("state", "10"), vec![f.instance_id], "m_state 过滤实例状态");
        assert_eq!(hit("operator", CC_SENDER), Vec::<i64>::new(), "抄送发送人不在 operator 列上");
        assert_eq!(hit("operator", CC_ACTOR), Vec::<i64>::new(), "被抄送人也不在 operator 列上");
    }
}

// ═══════════════════════════════════════════════════════
// issues/141 G1 ＋ G2 · 抄送分页归属必填 ／ 写侧判重＝幂等空操作（内存仓储这一支）
// 基准形状＝jeeflow-java `3d1fc98`（内存仓 `CcPageOwnershipTest` ＋ `CcWriteIdempotentTest`）；
// sqlx 仓那一条腿与"两仓同答案"的对拍在 `jeeflow-repository-sqlx`（真库 MySQL）里钉。
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod cc_i141_tests {
    use super::*;
    use crate::clock::ClockScope;
    use crate::json::FlowData;
    use std::sync::atomic::{AtomicU64, Ordering};

    const APPLICANT: &str = "i141_applicant";
    const SENDER: &str = "i141_sender";
    const ACTOR_A: &str = "i141_actor_a";
    const ACTOR_B: &str = "i141_actor_b";

    /// 逐次取值的注入钟：让"原行时间被刷新"与"没被刷新"在断言上**立刻**分得开
    /// （默认 UTC 钟是秒级分辨率，靠它就得 sleep 1s 以上，还会撞上同批并发的用例）。
    static TICK: AtomicU64 = AtomicU64::new(0);
    fn tick_clock() -> String {
        let n = TICK.fetch_add(1, Ordering::SeqCst);
        format!("2026-09-29 10:00:{:02}", n % 60)
    }

    fn cc_filter(column: &str, value: &str) -> QueryFilter {
        QueryFilter { alias: "cc".into(), op: crate::model::FilterOp::Eq, column: column.into(), value: value.into() }
    }

    /// 一条实例（发起人 APPLICANT），返回实例 id。
    fn new_instance(repo: &MemoryRepository) -> i64 {
        let mut define = ProcessDefine {
            id: 0, name: "i141-flow".into(), display_name: "I141 Flow".into(),
            define_type: "approval".into(), state: 1, content: b"{}".to_vec(),
            version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        repo.save_define(&mut define).unwrap();
        let mut inst = ProcessInstance {
            instance_id: 0, parent_id: None, define_id: define.id, state: 10,
            parent_node_name: None, business_no: Some("i141-biz".into()),
            operator: APPLICANT.into(), expire_time: None, variables: FlowData::new(),
            tasks: vec![], create_time: Some("2026-09-29 10:00:00".into()),
            create_user: Some(APPLICANT.into()), update_time: None, update_user: None,
            define: None,
        };
        repo.save_instance(&mut inst).unwrap();
        inst.instance_id
    }

    fn cc_rows(repo: &MemoryRepository, instance_id: i64) -> Vec<CcInstance> {
        repo.cc_instances.lock().unwrap()
            .iter().filter(|c| c.process_instance_id == instance_id).cloned().collect()
    }

    fn cc_of(repo: &MemoryRepository, instance_id: i64, actor: &str) -> Option<CcInstance> {
        cc_rows(repo, instance_id).into_iter().find(|c| c.actor_id == actor)
    }

    fn ids(page: &PageResult<InstanceRow>) -> Vec<i64> {
        page.rows.iter().map(|r| r.id).collect()
    }

    // ─────────── G1 · 归属条件必填（内存仓这一支）───────────

    /// 缺条件（整条不给）⇒ 空页。改前这一格在内存仓已经绿（issues/129 那一档顶住了
    /// "空 operator 看全库"），本格把它从"实现顺带对"升格为**trait 文档写明的义务**并留读数；
    /// 同一份数据在 sqlx 仓旧形状下是 LEFT JOIN 不过滤＝全实例，两仓两个答案。
    #[test]
    fn test_i141_g1_missing_ownership_is_empty_page() {
        let repo = MemoryRepository::new();
        let first = new_instance(&repo);
        let second = new_instance(&repo);
        repo.create_cc_instance(first, SENDER, &[ACTOR_A.into()]).unwrap();
        repo.create_cc_instance(second, SENDER, &[ACTOR_B.into()]).unwrap();

        let page = repo.page_cc_instances(&PageQuery::new(1, 50)).unwrap();
        assert_eq!(page.record_count, 0, "缺归属条件必须空页，实得 {:?}", ids(&page));
        assert!(page.rows.is_empty(), "空页的 rows 也必须是空集合");

        let bare = repo.page_cc_instances(&PageQuery::default()).unwrap();
        assert_eq!(bare.record_count, 0, "默认分页参数同样缺归属条件 ⇒ 空页");
    }

    /// 空值三形（空串／全空白）与"整条没给"同档 ⇒ 空页。
    #[test]
    fn test_i141_g1_blank_ownership_values_are_empty_page() {
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);
        repo.create_cc_instance(iid, SENDER, &[ACTOR_A.into()]).unwrap();

        for blank in ["", "   ", "\t"] {
            let mut q = PageQuery::new(1, 50);
            q.operator = Some(blank.to_string());
            let page = repo.page_cc_instances(&q).unwrap();
            assert_eq!(page.record_count, 0, "空值归属条件（{blank:?}）必须空页，实得 {:?}", ids(&page));
        }
        // 归属列上给的是空值条件（m_cc_actorId_EQ_""）⇒ 同样空页，且不得被当成"这条不加"
        let mut q = PageQuery::new(1, 50);
        q.operator = Some(ACTOR_A.into());
        q.filters = vec![cc_filter("actor_id", "")];
        assert_eq!(repo.page_cc_instances(&q).unwrap().record_count, 0,
            "空值归属条件不得退化成\"这条不加\"把 {ACTOR_A} 的行摊出来");
    }

    /// 归属条件的第二个通道（`m_cc_actorId_EQ_xxx`）单独给定时必须**生效**：
    /// 旧形状是内存仓只认 `query.operator`，这一档直接空页（改前实测红）。
    #[test]
    fn test_i141_g1_cc_actor_filter_alone_is_honored() {
        let repo = MemoryRepository::new();
        let mine = new_instance(&repo);
        let theirs = new_instance(&repo);
        repo.create_cc_instance(mine, SENDER, &[ACTOR_A.into()]).unwrap();
        repo.create_cc_instance(theirs, SENDER, &[ACTOR_B.into()]).unwrap();

        let mut q = PageQuery::new(1, 50);
        q.filters = vec![cc_filter("actor_id", ACTOR_A)];
        let page = repo.page_cc_instances(&q).unwrap();
        assert_eq!(ids(&page), vec![mine], "只给 m_cc_actorId 这一条有效归属条件也必须命中我自己的那一行");

        // 两通道给**同一个人** ⇒ 判据是 AND，照常命中（旧形状下 m_ 通道打不到 cc 列 ⇒ 0 行）
        let mut both = PageQuery::new(1, 50);
        both.operator = Some(ACTOR_A.into());
        both.filters = vec![cc_filter("actor_id", ACTOR_A)];
        assert_eq!(ids(&repo.page_cc_instances(&both).unwrap()), vec![mine],
            "两通道同一个人 ⇒ 同答案，不得互相抵消成空");

        // 两通道给**不同的人** ⇒ AND ⇒ 空页（谁都不是"这条不加"）
        let mut conflict = PageQuery::new(1, 50);
        conflict.operator = Some(ACTOR_A.into());
        conflict.filters = vec![cc_filter("actor_id", ACTOR_B)];
        assert_eq!(repo.page_cc_instances(&conflict).unwrap().record_count, 0,
            "两通道不同的人 ⇒ AND，不得返回任一方的行");
    }

    /// 改动面哨兵：只收归属谓词，**非归属列的空值放行不变**。
    #[test]
    fn test_i141_g1_non_ownership_blank_filter_still_ignored() {
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);
        repo.create_cc_instance(iid, SENDER, &[ACTOR_A.into()]).unwrap();

        let mut q = PageQuery::new(1, 50);
        q.operator = Some(ACTOR_A.into());
        q.filters = vec![QueryFilter {
            alias: "t".into(), op: crate::model::FilterOp::Like,
            column: "business_no".into(), value: "".into(),
        }];
        let page = repo.page_cc_instances(&q).unwrap();
        assert_eq!(page.record_count, 1, "空值非归属条件应被放行（LIKE 空串＝没填），归属条件照常生效");
        assert_eq!(ids(&page), vec![iid]);
    }

    // ─────────── G2 · 写侧判重＝幂等空操作（内存仓这一支）───────────

    /// ①不新增行：同一 `(实例, 人)` 连抄两次只有一行；`find_cc_actor_ids` 也只有一个。
    #[test]
    fn test_i141_g2_repeat_cc_adds_no_row() {
        let _scope = ClockScope::injected(tick_clock);
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);

        repo.create_cc_instance(iid, SENDER, &[ACTOR_A.into()]).unwrap();
        repo.create_cc_instance(iid, SENDER, &[ACTOR_A.into()]).unwrap();

        assert_eq!(cc_rows(&repo, iid).len(), 1, "①重复抄送不得新增第二行");
        assert_eq!(repo.find_cc_actor_ids(iid).unwrap(), vec![ACTOR_A.to_string()],
            "读侧也只能看到那一个 actor");
    }

    /// ②不重置未读：先置已读，再重复抄送，`state` 必须仍是已读（不产生"再提醒一次"语义）。
    #[test]
    fn test_i141_g2_repeat_cc_does_not_reset_unread() {
        let _scope = ClockScope::injected(tick_clock);
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);
        repo.create_cc_instance(iid, SENDER, &[ACTOR_A.into()]).unwrap();
        assert_eq!(cc_of(&repo, iid, ACTOR_A).unwrap().state, 0, "新行应是未读");

        repo.update_cc_status(iid, ACTOR_A).unwrap();
        let read = cc_of(&repo, iid, ACTOR_A).unwrap();
        assert_eq!(read.state, 1, "置读后 state 应为 1");

        repo.create_cc_instance(iid, SENDER, &[ACTOR_A.into()]).unwrap();
        let after = cc_of(&repo, iid, ACTOR_A).unwrap();
        assert_eq!(after.state, 1, "②重复抄送不得把已读抹回未读");
        assert_eq!(after.update_time, read.update_time,
            "②已读那一格的 update_time 也不得被重复抄送再刷一次（与③同源）");
    }

    /// ③不更新原行时间：`create_time`/`update_time` 逐字不变（注入钟逐次取值 ⇒ 刷新一定照得出来）。
    #[test]
    fn test_i141_g2_repeat_cc_does_not_touch_original_row_times() {
        let _scope = ClockScope::injected(tick_clock);
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);
        repo.create_cc_instance(iid, SENDER, &[ACTOR_A.into()]).unwrap();

        let before = cc_of(&repo, iid, ACTOR_A).expect("首抄应已建行");
        assert!(before.create_time.is_some(), "cc 行必须带建行时间（③这一档才照得出来）");
        assert!(before.update_time.is_some(), "cc 行必须带更新时间（对齐 wf_process_cc_instance 行形状）");

        repo.create_cc_instance(iid, SENDER, &[ACTOR_A.into()]).unwrap();
        let after = cc_of(&repo, iid, ACTOR_A).unwrap();
        assert_eq!(after.create_time, before.create_time, "③重复抄送不得刷新原行 create_time");
        assert_eq!(after.update_time, before.update_time, "③重复抄送不得刷新原行 update_time");
        assert_eq!(after.id, before.id, "③原行就是原行（主键不变，也没被删掉重建）");
    }

    /// ④的子集档：`create_cc_instance_if_absent` 返回**实际新建**的子集，顺序与入参一致。
    #[test]
    fn test_i141_g2_if_absent_returns_only_the_new_subset() {
        let _scope = ClockScope::injected(tick_clock);
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);

        let first = repo.create_cc_instance_if_absent(iid, SENDER, &[ACTOR_A.into(), ACTOR_B.into()]).unwrap();
        assert_eq!(first, vec![ACTOR_A.to_string(), ACTOR_B.to_string()], "全新的一批 ⇒ 子集＝全量、顺序随入参");

        let c = "i141_actor_c".to_string();
        let second = repo.create_cc_instance_if_absent(iid, SENDER, &[ACTOR_A.into(), c.clone()]).unwrap();
        assert_eq!(second, vec![c.clone()], "第二次只给新人 ⇒ 子集只有新人（旧人不得混进去被再 fire）");
        assert_eq!(cc_rows(&repo, iid).len(), 3, "落库的行数＝A/B/C 三行");
        assert_eq!(repo.find_cc_actor_ids(iid).unwrap(), vec![ACTOR_A.to_string(), ACTOR_B.to_string(), c],
            "读侧顺序与建行顺序一致");

        let third = repo.create_cc_instance_if_absent(iid, SENDER, &[ACTOR_A.into(), ACTOR_B.into()]).unwrap();
        assert!(third.is_empty(), "全是已知人 ⇒ 子集为空（调用点据此整支不 fire 码 4）");
        assert_eq!(cc_rows(&repo, iid).len(), 3, "子集为空也不得多落一行");
    }

    /// 同一次调用内重复给同一个人 ⇒ 折叠（只落一行、子集里只出现一次）。
    #[test]
    fn test_i141_g2_duplicate_within_one_call_collapses() {
        let _scope = ClockScope::injected(tick_clock);
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);

        let fresh = repo.create_cc_instance_if_absent(iid, SENDER, &[ACTOR_A.into(), ACTOR_A.into()]).unwrap();
        assert_eq!(fresh, vec![ACTOR_A.to_string()], "同一次调用内的重复只能算一次创建");
        assert_eq!(cc_rows(&repo, iid).len(), 1, "同一次调用内的重复不得新增第二行");

        // 直调旧入口（两条腿共用同一条判据）也幂等：判重在仓储写侧，不在调用点
        repo.create_cc_instance(iid, SENDER, &[ACTOR_A.into(), ACTOR_A.into()]).unwrap();
        assert_eq!(cc_rows(&repo, iid).len(), 1, "create_cc_instance 自身也必须判重");
    }

    /// 反向哨兵：判重**按实例**作用域，别把别的实例上同一个人的行也吃掉。
    #[test]
    fn test_i141_g2_dedup_is_scoped_to_instance() {
        let _scope = ClockScope::injected(tick_clock);
        let repo = MemoryRepository::new();
        let first = new_instance(&repo);
        let second = new_instance(&repo);

        assert_eq!(repo.create_cc_instance_if_absent(first, SENDER, &[ACTOR_A.into()]).unwrap().len(), 1);
        assert_eq!(repo.create_cc_instance_if_absent(second, SENDER, &[ACTOR_A.into()]).unwrap().len(), 1,
            "同一个人换一个实例照样新建");
        assert_eq!(cc_rows(&repo, first).len(), 1);
        assert_eq!(cc_rows(&repo, second).len(), 1);
        assert_eq!(repo.find_cc_actor_ids(999_999).unwrap(), Vec::<String>::new(),
            "没有 cc 行的实例读侧给空集");
    }

    /// 查询侧不引入 DISTINCT、历史重复行不清（owner 2026-09-29 拍：接受既成事实）：
    /// 手工造两条重复行 ⇒ 仓储照旧读得到两行，判重只管今后。
    #[test]
    fn test_i141_g2_query_side_adds_no_distinct_and_keeps_legacy_dupes() {
        let _scope = ClockScope::injected(tick_clock);
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);
        repo.create_cc_instance(iid, SENDER, &[ACTOR_A.into()]).unwrap();
        // 绕过判重的存量形状（＝库里既成的重复行）：直接塞进 cc 台账
        repo.cc_instances.lock().unwrap().push(CcInstance {
            id: repo.next_id(), process_instance_id: iid, actor_id: ACTOR_A.into(),
            state: 0, create_time: Some("2026-01-01 00:00:00".into()),
            create_user: Some(SENDER.into()), update_time: None, update_user: None,
        });

        assert_eq!(cc_rows(&repo, iid).len(), 2, "存量重复行不得被清理");
        assert_eq!(repo.find_cc_actor_ids(iid).unwrap(), vec![ACTOR_A.to_string(), ACTOR_A.to_string()],
            "读侧照旧逐行返回，不加 DISTINCT");
        // 分页那一侧仍按实例聚合出一行（issues/138 既有语义，本轮不动）
        let mut q = PageQuery::new(1, 50);
        q.operator = Some(ACTOR_A.into());
        assert_eq!(repo.page_cc_instances(&q).unwrap().record_count, 1,
            "同一实例两条 cc 命中同一接收人仍出一行（DISTINCT pi.id 不变）");
    }

    /// 第三方仓储不覆写新能力 ⇒ 走 trait default ⇒ 与旧 `create_cc_instance` 逐字一致
    /// （全量建行、全量返回）。这条钉的是 SPI 源码兼容不破，也是 changelog 里
    /// "不覆写就吃不到判重"那句话的证据。
    #[test]
    fn test_i141_g2_third_party_repo_without_override_keeps_old_behaviour() {
        #[derive(Default)]
        struct NaiveRepo {
            rows: std::sync::Mutex<Vec<(i64, String)>>,
        }
        impl ProcessRepository for NaiveRepo {
            fn create_cc_instance(&self, instance_id: i64, _creator: &str, actor_ids: &[String]) -> JeeflowResult<()> {
                // 旧形状：来人就插，不判重
                let mut rows = self.rows.lock().unwrap();
                for a in actor_ids { rows.push((instance_id, a.clone())); }
                Ok(())
            }
            // find_cc_actor_ids / create_cc_instance_if_absent **不覆写** ⇒ 吃 default
            fn find_define_by_id(&self, _: i64) -> JeeflowResult<Option<ProcessDefine>> { unimplemented!() }
            fn save_define(&self, _: &mut ProcessDefine) -> JeeflowResult<()> { unimplemented!() }
            fn update_define(&self, _: &ProcessDefine) -> JeeflowResult<()> { unimplemented!() }
            fn update_define_state(&self, _: i64, _: i32) -> JeeflowResult<()> { unimplemented!() }
            fn remove_define(&self, _: i64) -> JeeflowResult<()> { unimplemented!() }
            fn find_instance_by_id(&self, _: i64) -> JeeflowResult<Option<ProcessInstance>> { unimplemented!() }
            fn save_instance(&self, _: &mut ProcessInstance) -> JeeflowResult<()> { unimplemented!() }
            fn update_instance(&self, _: &ProcessInstance) -> JeeflowResult<()> { unimplemented!() }
            fn find_task_by_id(&self, _: i64) -> JeeflowResult<Option<ProcessTask>> { unimplemented!() }
            fn save_task(&self, _: &mut ProcessTask) -> JeeflowResult<()> { unimplemented!() }
            fn update_task(&self, _: &ProcessTask) -> JeeflowResult<()> { unimplemented!() }
            fn find_doing_tasks(&self, _: i64, _: &[String]) -> JeeflowResult<Vec<ProcessTask>> { unimplemented!() }
            fn find_done_tasks(&self, _: i64, _: &[String]) -> JeeflowResult<Vec<ProcessTask>> { unimplemented!() }
            fn find_history_tasks(&self, _: i64) -> JeeflowResult<Vec<ProcessTask>> { unimplemented!() }
            fn find_task_actors(&self, _: i64) -> JeeflowResult<Vec<String>> { unimplemented!() }
            fn add_task_actor(&self, _: i64, _: &[String]) -> JeeflowResult<()> { unimplemented!() }
            fn remove_task_actor(&self, _: i64, _: &[String]) -> JeeflowResult<()> { unimplemented!() }
            fn update_cc_status(&self, _: i64, _: &str) -> JeeflowResult<()> { Ok(()) }
            fn page_todo_tasks(&self, _: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> { unimplemented!() }
            fn page_done_tasks(&self, _: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> { unimplemented!() }
            fn page_instances(&self, _: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> { unimplemented!() }
            fn page_cc_instances(&self, _: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> { unimplemented!() }
            fn page_defines(&self, _: &PageQuery) -> JeeflowResult<PageResult<DefineRow>> { unimplemented!() }
            fn count_todo_tasks(&self, _: &str) -> JeeflowResult<i64> { unimplemented!() }
            fn get_all_instances(&self) -> JeeflowResult<Vec<ProcessInstance>> { unimplemented!() }
            fn get_all_tasks(&self) -> JeeflowResult<Vec<ProcessTask>> { unimplemented!() }
        }

        let repo = NaiveRepo::default();
        // 两次都抄给同样的两个人：覆写了判重的仓储第二次该拿到空子集，这里照旧全量 ⇒ 旧行为不破
        assert_eq!(repo.create_cc_instance_if_absent(1, SENDER, &[ACTOR_A.into(), ACTOR_B.into()]).unwrap(),
            vec![ACTOR_A.to_string(), ACTOR_B.to_string()], "不覆写 ⇒ 全量返回");
        assert_eq!(repo.create_cc_instance_if_absent(1, SENDER, &[ACTOR_A.into(), ACTOR_B.into()]).unwrap(),
            vec![ACTOR_A.to_string(), ACTOR_B.to_string()],
            "不覆写 find_cc_actor_ids 的第三方仓储读不到既有 cc 行 ⇒ 第二次仍全量返回、全量 fire");
        assert_eq!(repo.rows.lock().unwrap().len(), 4, "旧行为不破：全量建行（2＋2）");
        // default 唯一的加固是"同一次调用内重复折叠"（与 java default 同一条，不依赖读侧）
        let folded = repo.create_cc_instance_if_absent(2, SENDER, &[ACTOR_A.into(), ACTOR_A.into()]).unwrap();
        assert_eq!(folded, vec![ACTOR_A.to_string()], "同一次调用内的重复在 default 里就折叠");
    }

    // ─────────── issues/141 G10 · 空抄送人不建 cc 行（内存仓写侧兜底这一支）───────────

    /// 写侧兜底：绕过引擎漏斗与门面、直连仓储灌空值 ⇒ 一行都建不出来（spec §2.10 实现要求①第二层）。
    /// 改前实测：`create_cc_instance(&[""])` 真落一条 `actor_id=''` 的行——空归属值正是
    /// issues/129 那族"空 operator 读全库"的病根。
    #[test]
    fn test_i141_g10_repo_write_side_drops_blank_actors() {
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);

        repo.create_cc_instance(iid, SENDER,
            &["".into(), "   ".into(), "\t".into(), ACTOR_A.into()]).unwrap();

        assert_eq!(cc_rows(&repo, iid).len(), 1, "G10：空串/纯空白一律不建行，只落有效那一行");
        assert_eq!(repo.find_cc_actor_ids(iid).unwrap(), vec![ACTOR_A.to_string()],
            "G10：内存仓写侧不得落出 actor_id='' 的行");
    }

    /// 全空值一批 ⇒ 零行（与"没给抄送人"同形状）。
    #[test]
    fn test_i141_g10_all_blank_batch_creates_no_rows_at_all() {
        for batch in [vec!["".to_string()], vec!["   ".to_string()], vec!["\t".to_string()],
                      vec!["".to_string(), " ".to_string()]] {
            let repo = MemoryRepository::new();
            let iid = new_instance(&repo);
            repo.create_cc_instance(iid, SENDER, &batch).unwrap();
            assert!(cc_rows(&repo, iid).is_empty(), "G10：{batch:?} 不得建任何 cc 行");
            // 同一批再走判重 default ⇒ 子集也必须为空（子集是拿去 fire 码 4 的那一批）
            let fresh = repo.create_cc_instance_if_absent(iid, SENDER, &batch).unwrap();
            assert!(fresh.is_empty(), "G10：{batch:?} 走 if_absent 也拿不到空值子集，实得 {fresh:?}");
        }
    }

    /// 落库值取 trim 后的串，且与 G2 判重咬合：`" 8401 "` 与 `"8401"` 是同一个人 ⇒ 只一行。
    #[test]
    fn test_i141_g10_write_side_trims_and_hits_g2_dedup() {
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);

        repo.create_cc_instance(iid, SENDER, &["  ".to_string() + ACTOR_A + "  "]).unwrap();
        assert_eq!(repo.find_cc_actor_ids(iid).unwrap(), vec![ACTOR_A.to_string()],
            "G10：入库值必须是 trim 后的串");

        let fresh = repo.create_cc_instance_if_absent(iid, SENDER, &[ACTOR_A.into()]).unwrap();
        assert!(fresh.is_empty(), "G10＋G2：带空格与不带空格判为同一人 ⇒ 子集为空、不 fire");
        assert_eq!(cc_rows(&repo, iid).len(), 1, "G10＋G2：带空格的同一人不得再建第二行");
    }

    /// `create_cc_instance_if_absent` 返回的子集只含有效且 trim 后的人（子集直接拿去 fire 码 4）。
    #[test]
    fn test_i141_g10_if_absent_subset_excludes_blank_actors() {
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);

        let fresh = repo.create_cc_instance_if_absent(iid, SENDER,
            &["".into(), ACTOR_A.into(), "  ".into(), "  ".to_string() + ACTOR_B + " "]).unwrap();

        assert_eq!(fresh, vec![ACTOR_A.to_string(), ACTOR_B.to_string()],
            "G10：实际新建子集只含有效且 trim 后的人");
        assert_eq!(repo.find_cc_actor_ids(iid).unwrap(), fresh, "G10：子集与落库行一致");
    }

    /// 反向哨兵（内存仓这一支）：`"0"` 是正常用户 id，写侧归一不得吃掉它。
    #[test]
    fn test_i141_g10_zero_actor_id_still_written_on_repo_side() {
        let repo = MemoryRepository::new();
        let iid = new_instance(&repo);

        repo.create_cc_instance(iid, SENDER, &["0".into()]).unwrap();
        assert_eq!(repo.find_cc_actor_ids(iid).unwrap(), vec!["0".to_string()],
            "G10 只丢空串/纯空白：'0' 不得被当成空值丢掉");
    }
}

// ═══════════════════════════════════════════════════════
// issues/142 B 批 · 任务参与者写侧归属值归一（内存仓这一支）
//   立法＝spec 06-facade.md §2.11（把 §2.10 的四点实现要求逐字搬到任务侧）；
//   owner 拍「八栈一起收：两形同判据＋写侧兜底＋trim＋哨兵」。
//   判据本体＝`crate::model::normalize_actors`，与抄送侧 §2.10 同一枚单点（不抄第二份）；
//   真库那一支的对拍在 `jeeflow-repository-sqlx` 的 `test_mysql_i142_b_*`——
//   改前本仓判重、sqlx 仓盲插（无判空、无 trim、无判重）＝同一串入参两仓两个答案，
//   正是 issues/117 场景 27 立过法的那一类形状。
// ═══════════════════════════════════════════════════════
#[cfg(test)]
mod actor_i142_tests {
    use super::*;

    const TASK: i64 = 914201;

    /// 正向对照：正常参与者一个不吃、顺序不动（钉"归一不许顺手吃掉正常值"，改前也绿）。
    #[test]
    fn test_i142_b_positive_control_keeps_valid_actors_in_order() {
        let repo = MemoryRepository::new();
        repo.add_task_actor(TASK, &["7501".into(), "7502".into()]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(),
            vec!["7501".to_string(), "7502".to_string()], "正向对照：顺序随入参，一个不吃");
    }

    /// 写侧兜底（§2.11 硬要求①）：空串／纯空白即使**绕过门面直连仓储**也进不了归属列。
    /// 改前实测（内存仓）：判重有、判空无 ⇒ `["", "   ", "\t"]` 三行原样进台账。
    #[test]
    fn test_i142_b_add_task_actor_drops_blank_values() {
        let repo = MemoryRepository::new();
        repo.add_task_actor(TASK, &["".into(), "   ".into(), "\t".into()]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(), Vec::<String>::new(),
            "§2.11：全空白批次不得落进 wf_process_task_actor.actor_id");
    }

    /// 落库与比较一律取 trim 后的值，同一次调用内的重复折叠（§2.11 硬要求②）。
    #[test]
    fn test_i142_b_add_task_actor_trims_and_folds() {
        let repo = MemoryRepository::new();
        repo.add_task_actor(TASK,
            &[" i142a ".into(), "".into(), "i142a".into(), "i142b".into()]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(),
            vec!["i142a".to_string(), "i142b".to_string()],
            "§2.11：trim 后同值＝同一个人，落库值是 trim 后的串");
    }

    /// 跨调用判重与 trim 咬合：先写 `"i142c"` 再写 `" i142c "` ⇒ 仍一行。
    /// 改前实测（内存仓，还原跑一次的红格读数）：判重**看着有、其实被未 trim 的值打穿** ⇒
    /// 台账落 `["i142c"," i142c "]` 两行，正是 §2.11 硬要求②点名的"不 trim 就会与写侧判重错开"。
    #[test]
    fn test_i142_b_padded_value_hits_dedup() {
        let repo = MemoryRepository::new();
        repo.add_task_actor(TASK, &["i142c".into()]).unwrap();
        repo.add_task_actor(TASK, &[" i142c ".into()]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(), vec!["i142c".to_string()],
            "§2.11 硬要求②：不 trim 就会与写侧判重错开，同一人落两行");
    }

    /// 反向哨兵（§2.11 硬要求④）：`"0"`、`"00"`、`" "`、`"a"` 是**三个人**。
    /// 判空一律 `trim().is_empty()`——旧形状不 trim 时纯空白 `' '` 被当成第四个人收进台账。
    #[test]
    fn test_i142_b_sentinel_four_are_three_people() {
        let repo = MemoryRepository::new();
        repo.add_task_actor(TASK, &["0".into(), "00".into(), " ".into(), "a".into()]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(),
            vec!["0".to_string(), "00".to_string(), "a".to_string()],
            "哨兵：'0'/'00'/'a' 都是正常 id，只有 ' ' 是空值");
        assert_eq!(repo.find_task_actors(TASK).unwrap().len(), 3,
            "'00' 与 '0' 是两个人（严禁松散比较静默吞掉第二个人）");
    }

    /// 删除位（issues/142 §9.2 第二批）：「 8601 」删得掉 trim 后的 8601；归一后为空 ⇒
    /// 什么都不删——历史 actor_id='' 脏行不得被空串入参批量误删。
    #[test]
    fn test_i142_b_remove_task_actor_trims_and_blank_is_noop() {
        let repo = MemoryRepository::new();
        repo.add_task_actor(TASK, &["  i142d  ".into()]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(), vec!["i142d".to_string()],
            "前置：写侧落库存 trim 值");

        repo.remove_task_actor(TASK, &["  i142d  ".into()]).unwrap();
        assert!(repo.find_task_actors(TASK).unwrap().is_empty(),
            "删除位必须按归一后的值比较（【 i142d 】删得掉 i142d）");

        // 历史脏行：库里有 actor_id=空串 的行。空串入参绝不能把它当"要删的人"。
        repo.task_actors.lock().unwrap().insert(TASK, vec!["i142e".to_string(), "".to_string()]);
        repo.remove_task_actor(TASK, &["".to_string(), "   ".to_string()]).unwrap();
        repo.remove_task_actor(TASK, &[]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(),
            vec!["i142e".to_string(), "".to_string()],
            "归一后为空 ⇒ 一行都不删（issues/129 删除位对偶）");
    }
}

// ═══════════════════════════════════════════════════════
// issues/137 §3-6 · 参与者删除腿「原值 ∪ trim 值」两形并集（内存仓这一支）
//   立法＝spec 06-facade.md §processTask/removeTaskActor 语义 6 ＋ §2.11 写点表删除腿行
//   （owner 2026-10-02 拍「两形并集」）。判据本体只有一枚＝`crate::model::actor_delete_forms`；
//   真库那一支的对拍在 `jeeflow-repository-sqlx` 的 `test_mysql_i137_*`，
//   两仓必须同一条判据、同一个答案（issues/117 场景 27）。
//   改前 rust 是"只取 trim 形"那一派（删除位过 `normalize_actors`）：门面按语义 6 交出的
//   行上原值「 9101 」被削成 9101，未 trim 历史脏行删不掉而门面报成功——本组第一格 N 档
//   在改前正是红的（rust 的假成功实证）。
//   种脏行必须**绕开写侧归一**（`add_task_actor` 会 trim＋丢空，正常路径建不出脏行）：
//   直接插 `task_actors` 台账；断言打在仓储里真实存着的值上（`find_task_actors` 读回）。
// ═══════════════════════════════════════════════════════
#[cfg(test)]
mod actor_delete_forms_tests {
    use super::*;

    const TASK: i64 = 913701;

    /// 直插台账种行（含未 trim 脏行/空值脏行——写侧归一挡得住的那些形状）。
    fn seed(repo: &MemoryRepository, actors: &[&str]) {
        repo.task_actors.lock().unwrap()
            .insert(TASK, actors.iter().map(|s| s.to_string()).collect());
    }

    /// N 档（假成功修复，改前必红）：库里躺着修复前落下的未 trim 历史脏行「 9101 」，
    /// 门面按语义 6 交出**行上的原值**去删 ⇒ 脏行必须真消失。只取 trim 形的实现
    /// （改前 rust）在这一格把「 9101 」削成 9101，脏行留在库里、门面报成功——被摘的人待办还在。
    #[test]
    fn test_i137_remove_task_actor_deletes_untrimmed_legacy_row_by_raw_form() {
        let repo = MemoryRepository::new();
        seed(&repo, &[" 9101 ", "leader"]);
        repo.remove_task_actor(TASK, &[" 9101 ".into()]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(), vec!["leader".to_string()],
            "语义 6：未 trim 的历史脏行必须被原值形删掉（否则是门面报成功的假成功）");
    }

    /// N 档（142 §9.2 那一路不破）：写侧归一后的规范行 8601，第三方绕过门面直连仓储
    /// 传「 8601 」⇒ 靠 trim 形也必须删得掉。
    #[test]
    fn test_i137_remove_task_actor_still_deletes_normalized_row_by_trimmed_form() {
        let repo = MemoryRepository::new();
        seed(&repo, &["8601", "leader"]);
        repo.remove_task_actor(TASK, &[" 8601 ".into()]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(), vec!["leader".to_string()],
            "规范行由 trim 形命中（issues/142 §9.2 的既有判据不破）");
    }

    /// 脏行与规范行并存 ⇒ 同一个人（§2.11 归一口径）名下两行都摘掉，
    /// 其余参与人一行不动（语义 1「只摘不加」）。
    #[test]
    fn test_i137_remove_task_actor_removes_both_forms_together() {
        let repo = MemoryRepository::new();
        seed(&repo, &[" 9101 ", "9101", "leader", "boss"]);
        repo.remove_task_actor(TASK, &[" 9101 ".into()]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(),
            vec!["leader".to_string(), "boss".to_string()],
            "归一后是同一个人 ⇒ 两行都摘，其余参与人原样保留");
    }

    /// P 档（脏行保护）：空值入参一律不参与匹配——历史 actor_id=''/纯空白脏行
    /// 是待另案清洗的取证痕迹，不得被一次空值入参批量做掉。
    #[test]
    fn test_i137_blank_input_never_deletes_empty_actor_id_dirty_row() {
        let repo = MemoryRepository::new();
        seed(&repo, &["", "   ", "leader"]);
        repo.remove_task_actor(TASK, &["".to_string(), "   ".to_string(), "\t".to_string()]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(),
            vec!["".to_string(), "   ".to_string(), "leader".to_string()],
            "空值入参一行都不许删（含历史 actor_id=''/纯空白脏行）");
    }

    /// P 档（不得退化成清空）：并集为空 ⇒ 早退，一次删除都不发生（语义 6 义务③）。
    #[test]
    fn test_i137_all_blank_input_is_noop_and_never_clears_all_actors() {
        let repo = MemoryRepository::new();
        seed(&repo, &["zhangsan", "leader"]);
        repo.remove_task_actor(TASK, &["".to_string(), "  ".to_string()]).unwrap();
        repo.remove_task_actor(TASK, &[]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(),
            vec!["zhangsan".to_string(), "leader".to_string()],
            "空入参两形（纯空白/空列表）都是零删除，不得清空全部参与者");
    }

    /// 非参与者静默忽略（语义 7 幂等）；任务不存在 ⇒ 零操作、不 panic。
    #[test]
    fn test_i137_unknown_actor_ignored_and_unknown_task_is_noop() {
        let repo = MemoryRepository::new();
        seed(&repo, &["zhangsan", "leader"]);
        repo.remove_task_actor(TASK, &["stranger".into(), " 9999 ".into()]).unwrap();
        assert_eq!(repo.find_task_actors(TASK).unwrap(),
            vec!["zhangsan".to_string(), "leader".to_string()],
            "非参与者静默忽略，既有参与者一行不动");
        repo.remove_task_actor(404404, &[" 9101 ".into()]).unwrap();
        assert!(repo.find_task_actors(404404).unwrap().is_empty(),
            "删除腿对不存在的 taskId 是零操作，不得 panic");
    }
}
