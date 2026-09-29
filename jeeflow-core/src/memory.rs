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
        let mut actors = self.task_actors.lock().unwrap();
        let entry = actors.entry(task_id).or_insert_with(Vec::new);
        for a in new_actors {
            if !entry.contains(a) { entry.push(a.clone()); }
        }
        Ok(())
    }

    fn remove_task_actor(&self, task_id: i64, remove_actors: &[String]) -> JeeflowResult<()> {
        let mut actors = self.task_actors.lock().unwrap();
        if let Some(entry) = actors.get_mut(&task_id) {
            entry.retain(|a| !remove_actors.contains(a));
        }
        Ok(())
    }

    fn create_cc_instance(&self, instance_id: i64, creator: &str, actor_ids: &[String]) -> JeeflowResult<()> {
        let mut ccs = self.cc_instances.lock().unwrap();
        for actor_id in actor_ids {
            ccs.push(CcInstance {
                id: self.next_id(),
                process_instance_id: instance_id,
                actor_id: actor_id.clone(),
                state: 0,
                create_time: None,
                create_user: Some(creator.to_string()),
                update_time: None,
                update_user: None,
            });
        }
        Ok(())
    }

    fn update_cc_status(&self, instance_id: i64, actor_id: &str) -> JeeflowResult<()> {
        let mut ccs = self.cc_instances.lock().unwrap();
        for cc in ccs.iter_mut() {
            if cc.process_instance_id == instance_id && cc.actor_id == actor_id {
                cc.state = 1;
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
    fn page_cc_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> {
        // issues/129：抄送列表也不得把"没传 operator"折叠成"看全部"。空值一律空页。
        let op = query.operator.as_deref().map(str::trim).unwrap_or("");
        if op.is_empty() {
            return Ok(PageResult::new(query.page_num, query.page_size, 0, vec![]));
        }
        let ccs = self.cc_instances.lock().unwrap();
        let instances = self.instances.lock().unwrap();
        let defines = self.defines.lock().unwrap();
        let mut seen_instance_ids: Vec<i64> = Vec::new();
        let mut rows: Vec<InstanceRow> = Vec::new();
        for cc in ccs.iter().filter(|cc| cc.actor_id == op) {
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
        if !query.filters.is_empty() {
            rows.retain(|r| query.filters.iter().all(|f| instance_row_matches(f, r)));
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
