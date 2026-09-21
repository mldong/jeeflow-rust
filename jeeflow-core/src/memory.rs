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

        let mut rows: Vec<TaskRow> = tasks.values()
            .filter(|t| {
                t.task_state == TaskState::Doing.code()
                    && (query.operator.is_none() || t.actor_ids.contains(query.operator.as_ref().unwrap()))
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
            .filter(|i| {
                query
                    .operator
                    .as_ref()
                    .map(|op| &i.operator == op)
                    .unwrap_or(true)
            })
            .map(|i| {
                let define = defines.get(&i.define_id);
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

    fn page_cc_instances(&self, query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> {
        let ccs = self.cc_instances.lock().unwrap();
        let instances = self.instances.lock().unwrap();
        let defines = self.defines.lock().unwrap();
        let mut rows: Vec<InstanceRow> = ccs
            .iter()
            .filter(|cc| {
                query
                    .operator
                    .as_ref()
                    .map(|op| &cc.actor_id == op)
                    .unwrap_or(true)
            })
            .map(|cc| {
            let inst = instances.get(&cc.process_instance_id);
            let define = inst.and_then(|i| defines.get(&i.define_id));
            InstanceRow {
                id: cc.id,
                parent_id: None,
                process_define_id: inst.map(|i| i.define_id).unwrap_or(0),
                state: cc.state,
                parent_node_name: None,
                business_no: inst.and_then(|i| i.business_no.clone()),
                operator: cc.actor_id.clone(),
                expire_time: None,
                variable: None,
                create_time: cc.create_time.clone(),
                create_user: cc.create_user.clone(),
                update_time: cc.update_time.clone(),
                update_user: cc.update_user.clone(),
                define_name: define.map(|d| d.name.clone()),
                define_display_name: define.map(|d| d.display_name.clone()),
                define_version: define.map(|d| d.version),
            }
        }).collect();
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
    /// 修复前本方法的三处欠账（issues/116 §5）：
    /// - 形参 `_time` **直接忽略时间窗** → 同一份数据 SQL 仓判窗外、内存仓判命中，
    ///   换仓储就换答案（现改为把 `time` 传给判据，按 `yyyy-MM-dd HH:mm:ss` 归一比较）；
    /// - 无自委托过滤 `surrogate <> operator` → 会命中"自己委托给自己"；
    /// - 多条命中按 `HashMap` **随机遍历序取首条** → 违条款 1.4（SQL 侧 `ORDER BY id DESC`），
    ///   现统一由 [`crate::surrogate::pick_surrogate`] 取 id 最大者；
    /// - 缺空 processName 全流程兜底（只认 `process_name == process_name` 精确相等）→
    ///   现"先精确、未命中再兜底全流程"两步查。
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
