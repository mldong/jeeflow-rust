//! jeeflow-repository-sqlx: sqlx-based repository implementation.
//! Provides MySQL DDL schema + SqlxRepository wrapping sqlx::MySqlPool.
//! The ProcessRepository trait is synchronous; we use sync-over-async internally.

use jeeflow_core::error::{JeeflowError, JeeflowResult};
use jeeflow_core::model::*;
use jeeflow_core::spi::*;
use sqlx::mysql::MySqlPool;
use sqlx::Row;

// ═══════════════════════════════════════════════════════
// MySQL DDL — 8 tables (spec/08)
// ═══════════════════════════════════════════════════════

/// Returns the 8-table DDL string for MySQL schema initialization.
pub fn schema_mysql() -> &'static str {
    MYSQL_SCHEMA
}

pub const MYSQL_SCHEMA: &str = r#"
-- 1. 流程定义表
CREATE TABLE IF NOT EXISTS wf_process_define (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    name VARCHAR(100) NOT NULL COMMENT '流程名称（英文，唯一标识）',
    display_name VARCHAR(200) NOT NULL COMMENT '显示名称',
    define_type VARCHAR(50) DEFAULT 'approval' COMMENT '流程类型',
    state INT NOT NULL DEFAULT 1 COMMENT '状态: 0=禁用, 1=启用',
    content LONGTEXT COMMENT '流程模型JSON（LogicFlow格式）',
    version INT NOT NULL DEFAULT 1 COMMENT '版本号',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    create_user VARCHAR(50) COMMENT '创建人',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    update_user VARCHAR(50) COMMENT '更新人',
    PRIMARY KEY (id),
    UNIQUE KEY uk_name_version (name, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程定义表';

-- 2. 流程实例表
CREATE TABLE IF NOT EXISTS wf_process_instance (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    parent_id BIGINT DEFAULT NULL COMMENT '父流程实例ID（子流程）',
    process_define_id BIGINT NOT NULL COMMENT '流程定义ID',
    state INT NOT NULL DEFAULT 10 COMMENT '状态: 10=进行中, 20=已完成, 30=已撤回, 40=终止, 45=拒绝, 50=挂起, 99=废弃',
    parent_node_name VARCHAR(100) DEFAULT NULL COMMENT '父流程节点名称',
    business_no VARCHAR(100) DEFAULT NULL COMMENT '业务编号',
    operator VARCHAR(50) NOT NULL COMMENT '发起人',
    expire_time DATETIME DEFAULT NULL COMMENT '过期时间',
    variable TEXT COMMENT '流程变量（JSON）',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    create_user VARCHAR(50) COMMENT '创建人',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    update_user VARCHAR(50) COMMENT '更新人',
    PRIMARY KEY (id),
    KEY idx_define_id (process_define_id),
    KEY idx_operator (operator),
    KEY idx_state (state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程实例表';

-- 3. 流程任务表
CREATE TABLE IF NOT EXISTS wf_process_task (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    process_instance_id BIGINT NOT NULL COMMENT '流程实例ID',
    task_name VARCHAR(100) NOT NULL COMMENT '任务名称（节点ID）',
    display_name VARCHAR(200) COMMENT '显示名称',
    task_type INT DEFAULT 0 COMMENT '任务类型: 0=主办, 1=协办, 2=记录',
    perform_type INT DEFAULT 0 COMMENT '参与类型: 0=普通, 1=会签',
    state INT NOT NULL DEFAULT 10 COMMENT '状态: 10=进行中, 20=已完成, 30=已撤回, 40=终止, 50=挂起, 99=废弃',
    actor_id VARCHAR(50) DEFAULT NULL COMMENT '实际处理人',
    finish_time DATETIME DEFAULT NULL COMMENT '完成时间',
    expire_time DATETIME DEFAULT NULL COMMENT '过期时间',
    form_key VARCHAR(100) DEFAULT NULL COMMENT '表单key',
    parent_task_id BIGINT DEFAULT NULL COMMENT '父任务ID',
    variable TEXT COMMENT '任务变量（JSON）',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    create_user VARCHAR(50) COMMENT '创建人',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    update_user VARCHAR(50) COMMENT '更新人',
    PRIMARY KEY (id),
    KEY idx_instance_id (process_instance_id),
    KEY idx_actor_id (actor_id),
    KEY idx_state (state)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程任务表';

-- 4. 任务参与者表
CREATE TABLE IF NOT EXISTS wf_process_task_actor (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    process_task_id BIGINT NOT NULL COMMENT '任务ID',
    actor_id VARCHAR(50) NOT NULL COMMENT '参与者ID',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    create_user VARCHAR(50) COMMENT '创建人',
    PRIMARY KEY (id),
    KEY idx_task_id (process_task_id),
    KEY idx_actor_id (actor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务参与者表';

-- 5. 抄送实例表
CREATE TABLE IF NOT EXISTS wf_cc_instance (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    process_instance_id BIGINT NOT NULL COMMENT '流程实例ID',
    actor_id VARCHAR(50) NOT NULL COMMENT '抄送人',
    state INT DEFAULT 0 COMMENT '状态: 0=未读, 1=已读',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    create_user VARCHAR(50) COMMENT '创建人',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    update_user VARCHAR(50) COMMENT '更新人',
    PRIMARY KEY (id),
    KEY idx_instance_id (process_instance_id),
    KEY idx_actor_id (actor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='抄送实例表';

-- 6. 流程设计表
CREATE TABLE IF NOT EXISTS wf_process_design (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    name VARCHAR(100) NOT NULL COMMENT '流程名称',
    display_name VARCHAR(200) COMMENT '显示名称',
    design_type VARCHAR(50) DEFAULT 'approval' COMMENT '设计类型',
    icon VARCHAR(200) DEFAULT NULL COMMENT '图标',
    is_deployed INT DEFAULT 0 COMMENT '是否已部署: 0=未部署, 1=已部署',
    remark VARCHAR(500) DEFAULT NULL COMMENT '备注',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    create_user VARCHAR(50) COMMENT '创建人',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    update_user VARCHAR(50) COMMENT '更新人',
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程设计表';

-- 7. 流程设计历史表
CREATE TABLE IF NOT EXISTS wf_process_design_his (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    process_design_id BIGINT NOT NULL COMMENT '流程设计ID',
    content LONGTEXT COMMENT '设计内容JSON',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    create_user VARCHAR(50) COMMENT '创建人',
    PRIMARY KEY (id),
    KEY idx_design_id (process_design_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='流程设计历史表';

-- 8. 委托代理表
CREATE TABLE IF NOT EXISTS wf_process_surrogate (
    id BIGINT NOT NULL AUTO_INCREMENT COMMENT '主键',
    process_name VARCHAR(100) NOT NULL COMMENT '流程名称',
    operator VARCHAR(50) NOT NULL COMMENT '委托人',
    surrogate VARCHAR(50) NOT NULL COMMENT '代理人',
    start_time DATETIME DEFAULT NULL COMMENT '开始时间',
    end_time DATETIME DEFAULT NULL COMMENT '结束时间',
    enabled INT DEFAULT 1 COMMENT '是否启用: 0=禁用, 1=启用',
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    create_user VARCHAR(50) COMMENT '创建人',
    update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    update_user VARCHAR(50) COMMENT '更新人',
    PRIMARY KEY (id),
    KEY idx_operator (operator)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='委托代理表';
"#;

// ═══════════════════════════════════════════════════════
// SqlxRepository
// ═══════════════════════════════════════════════════════

/// SQLx-based repository wrapping a MySqlPool.
/// Implements ProcessRepository + ProcessExtRepository using sync-over-async.
pub struct SqlxRepository {
    pool: MySqlPool,
}

impl SqlxRepository {
    pub fn new(pool: MySqlPool) -> Self {
        SqlxRepository { pool }
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Block on an async future using the current tokio runtime handle.
    fn block_on<F: std::future::Future>(&self, f: F) -> F::Output {
        tokio::runtime::Handle::current().block_on(f)
    }

    /// Initialize the schema by executing the DDL.
    pub async fn init_schema(pool: &MySqlPool) -> JeeflowResult<()> {
        for stmt in MYSQL_SCHEMA.split(';') {
            let trimmed = stmt.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }
            sqlx::query(trimmed)
                .execute(pool)
                .await
                .map_err(|e| JeeflowError::Internal(format!("Schema init error: {}", e)))?;
        }
        Ok(())
    }
}

// Helper: parse FlowData from JSON string
fn parse_flow_data(s: &Option<String>) -> jeeflow_core::json::FlowData {
    s.as_ref()
        .and_then(|v| jeeflow_core::json::parse_json(v).ok())
        .map(|jv| jeeflow_core::json::FlowData::from_map(jv.to_map()))
        .unwrap_or_default()
}

// Helper: serialize FlowData to JSON string
fn flow_data_to_json(fd: &jeeflow_core::json::FlowData) -> String {
    if fd.is_empty() {
        return "{}".to_string();
    }
    let entries: Vec<String> = fd.iter()
        .map(|(k, v)| format!("\"{}\":{}", k, v.to_json_string()))
        .collect();
    format!("{{{}}}", entries.join(","))
}

impl ProcessRepository for SqlxRepository {
    fn find_define_by_id(&self, define_id: i64) -> JeeflowResult<Option<ProcessDefine>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT id, name, display_name, define_type, state, content, version, create_time, create_user, update_time, update_user FROM wf_process_define WHERE id = ?"
            )
            .bind(define_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            Ok(row.map(|r| ProcessDefine {
                id: r.get("id"),
                name: r.get("name"),
                display_name: r.get("display_name"),
                define_type: r.get("define_type"),
                state: r.get("state"),
                content: r.get::<String, _>("content").into_bytes(),
                version: r.get("version"),
                create_time: r.get("create_time"),
                create_user: r.get("create_user"),
                update_time: r.get("update_time"),
                update_user: r.get("update_user"),
            }))
        })
    }

    fn save_define(&self, define: &mut ProcessDefine) -> JeeflowResult<()> {
        self.block_on(async {
            let result = sqlx::query(
                "INSERT INTO wf_process_define (name, display_name, define_type, state, content, version, create_user) VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(&define.name)
            .bind(&define.display_name)
            .bind(&define.define_type)
            .bind(define.state)
            .bind(String::from_utf8_lossy(&define.content).to_string())
            .bind(define.version)
            .bind(&define.create_user)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            define.id = result.last_insert_id() as i64;
            Ok(())
        })
    }

    fn update_define(&self, define: &ProcessDefine) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query(
                "UPDATE wf_process_define SET display_name=?, define_type=?, state=?, content=?, version=?, update_user=? WHERE id=?"
            )
            .bind(&define.display_name)
            .bind(&define.define_type)
            .bind(define.state)
            .bind(String::from_utf8_lossy(&define.content).to_string())
            .bind(define.version)
            .bind(&define.update_user)
            .bind(define.id)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn update_define_state(&self, define_id: i64, state: i32) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query("UPDATE wf_process_define SET state=? WHERE id=?")
                .bind(state)
                .bind(define_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn remove_define(&self, define_id: i64) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query("DELETE FROM wf_process_define WHERE id=?")
                .bind(define_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn find_instance_by_id(&self, instance_id: i64) -> JeeflowResult<Option<ProcessInstance>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT id, parent_id, process_define_id, state, parent_node_name, business_no, operator, expire_time, variable, create_time, create_user, update_time, update_user FROM wf_process_instance WHERE id = ?"
            )
            .bind(instance_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            Ok(row.map(|r| ProcessInstance {
                instance_id: r.get("id"),
                parent_id: r.get("parent_id"),
                define_id: r.get("process_define_id"),
                state: r.get("state"),
                parent_node_name: r.get("parent_node_name"),
                business_no: r.get("business_no"),
                operator: r.get("operator"),
                expire_time: r.get("expire_time"),
                variables: parse_flow_data(&r.get("variable")),
                tasks: vec![],
                create_time: r.get("create_time"),
                create_user: r.get("create_user"),
                update_time: r.get("update_time"),
                update_user: r.get("update_user"),
                define: None,
            }))
        })
    }

    fn save_instance(&self, instance: &mut ProcessInstance) -> JeeflowResult<()> {
        self.block_on(async {
            let var_json = flow_data_to_json(&instance.variables);
            let result = sqlx::query(
                "INSERT INTO wf_process_instance (parent_id, process_define_id, state, parent_node_name, business_no, operator, expire_time, variable, create_user) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(instance.parent_id)
            .bind(instance.define_id)
            .bind(instance.state)
            .bind(&instance.parent_node_name)
            .bind(&instance.business_no)
            .bind(&instance.operator)
            .bind(&instance.expire_time)
            .bind(&var_json)
            .bind(&instance.create_user)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            instance.instance_id = result.last_insert_id() as i64;
            Ok(())
        })
    }

    fn update_instance(&self, instance: &ProcessInstance) -> JeeflowResult<()> {
        self.block_on(async {
            let var_json = flow_data_to_json(&instance.variables);
            sqlx::query("UPDATE wf_process_instance SET state=?, variable=?, update_time=NOW() WHERE id=?")
                .bind(instance.state)
                .bind(&var_json)
                .bind(instance.instance_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn find_task_by_id(&self, task_id: i64) -> JeeflowResult<Option<ProcessTask>> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT id, process_instance_id, task_name, display_name, task_type, perform_type, state, actor_id, finish_time, expire_time, form_key, parent_task_id, variable, create_time, create_user, update_time, update_user FROM wf_process_task WHERE id = ?"
            )
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            Ok(row.map(|r| ProcessTask {
                task_id: r.get("id"),
                process_instance_id: r.get("process_instance_id"),
                task_name: r.get("task_name"),
                display_name: r.get("display_name"),
                task_type: r.get("task_type"),
                perform_type: r.get("perform_type"),
                task_state: r.get("state"),
                actor_id: r.get("actor_id"),
                actor_ids: vec![],
                finish_time: r.get("finish_time"),
                expire_time: r.get("expire_time"),
                form_key: r.get("form_key"),
                parent_task_id: r.get("parent_task_id"),
                variables: parse_flow_data(&r.get("variable")),
                create_time: r.get("create_time"),
                create_user: r.get("create_user"),
                update_time: r.get("update_time"),
                update_user: r.get("update_user"),
            }))
        })
    }

    fn save_task(&self, task: &mut ProcessTask) -> JeeflowResult<()> {
        self.block_on(async {
            let var_json = flow_data_to_json(&task.variables);
            let result = sqlx::query(
                "INSERT INTO wf_process_task (process_instance_id, task_name, display_name, task_type, perform_type, state, actor_id, expire_time, form_key, parent_task_id, variable, create_user) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )
            .bind(task.process_instance_id)
            .bind(&task.task_name)
            .bind(&task.display_name)
            .bind(task.task_type)
            .bind(task.perform_type)
            .bind(task.task_state)
            .bind(&task.actor_id)
            .bind(&task.expire_time)
            .bind(&task.form_key)
            .bind(task.parent_task_id)
            .bind(&var_json)
            .bind(&task.create_user)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            task.task_id = result.last_insert_id() as i64;
            Ok(())
        })
    }

    fn update_task(&self, task: &ProcessTask) -> JeeflowResult<()> {
        self.block_on(async {
            let var_json = flow_data_to_json(&task.variables);
            sqlx::query(
                "UPDATE wf_process_task SET state=?, actor_id=?, finish_time=?, variable=?, update_time=NOW() WHERE id=?"
            )
            .bind(task.task_state)
            .bind(&task.actor_id)
            .bind(&task.finish_time)
            .bind(&var_json)
            .bind(task.task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn find_doing_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, process_instance_id, task_name, display_name, task_type, perform_type, state, actor_id, finish_time, expire_time, form_key, parent_task_id, variable, create_time, create_user, update_time, update_user FROM wf_process_task WHERE process_instance_id = ? AND state = 10"
            )
            .bind(instance_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            let tasks: Vec<ProcessTask> = rows.into_iter().map(|r| ProcessTask {
                task_id: r.get("id"),
                process_instance_id: r.get("process_instance_id"),
                task_name: r.get("task_name"),
                display_name: r.get("display_name"),
                task_type: r.get("task_type"),
                perform_type: r.get("perform_type"),
                task_state: r.get("state"),
                actor_id: r.get("actor_id"),
                actor_ids: vec![],
                finish_time: r.get("finish_time"),
                expire_time: r.get("expire_time"),
                form_key: r.get("form_key"),
                parent_task_id: r.get("parent_task_id"),
                variables: jeeflow_core::json::FlowData::new(),
                create_time: r.get("create_time"),
                create_user: r.get("create_user"),
                update_time: r.get("update_time"),
                update_user: r.get("update_user"),
            }).collect();

            Ok(if task_names.is_empty() { tasks } else { tasks.into_iter().filter(|t| task_names.contains(&t.task_name)).collect() })
        })
    }

    fn find_done_tasks(&self, instance_id: i64, task_names: &[String]) -> JeeflowResult<Vec<ProcessTask>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, process_instance_id, task_name, display_name, task_type, perform_type, state, actor_id, finish_time, expire_time, form_key, parent_task_id, variable, create_time, create_user, update_time, update_user FROM wf_process_task WHERE process_instance_id = ? AND state = 20"
            )
            .bind(instance_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            let tasks: Vec<ProcessTask> = rows.into_iter().map(|r| ProcessTask {
                task_id: r.get("id"),
                process_instance_id: r.get("process_instance_id"),
                task_name: r.get("task_name"),
                display_name: r.get("display_name"),
                task_type: r.get("task_type"),
                perform_type: r.get("perform_type"),
                task_state: r.get("state"),
                actor_id: r.get("actor_id"),
                actor_ids: vec![],
                finish_time: r.get("finish_time"),
                expire_time: r.get("expire_time"),
                form_key: r.get("form_key"),
                parent_task_id: r.get("parent_task_id"),
                variables: jeeflow_core::json::FlowData::new(),
                create_time: r.get("create_time"),
                create_user: r.get("create_user"),
                update_time: r.get("update_time"),
                update_user: r.get("update_user"),
            }).collect();

            Ok(if task_names.is_empty() { tasks } else { tasks.into_iter().filter(|t| task_names.contains(&t.task_name)).collect() })
        })
    }

    fn find_history_tasks(&self, instance_id: i64) -> JeeflowResult<Vec<ProcessTask>> {
        self.block_on(async {
            let rows = sqlx::query(
                "SELECT id, process_instance_id, task_name, display_name, task_type, perform_type, state, actor_id, finish_time, expire_time, form_key, parent_task_id, variable, create_time, create_user, update_time, update_user FROM wf_process_task WHERE process_instance_id = ?"
            )
            .bind(instance_id)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;

            Ok(rows.into_iter().map(|r| ProcessTask {
                task_id: r.get("id"),
                process_instance_id: r.get("process_instance_id"),
                task_name: r.get("task_name"),
                display_name: r.get("display_name"),
                task_type: r.get("task_type"),
                perform_type: r.get("perform_type"),
                task_state: r.get("state"),
                actor_id: r.get("actor_id"),
                actor_ids: vec![],
                finish_time: r.get("finish_time"),
                expire_time: r.get("expire_time"),
                form_key: r.get("form_key"),
                parent_task_id: r.get("parent_task_id"),
                variables: jeeflow_core::json::FlowData::new(),
                create_time: r.get("create_time"),
                create_user: r.get("create_user"),
                update_time: r.get("update_time"),
                update_user: r.get("update_user"),
            }).collect())
        })
    }

    fn find_task_actors(&self, task_id: i64) -> JeeflowResult<Vec<String>> {
        self.block_on(async {
            let rows = sqlx::query("SELECT actor_id FROM wf_process_task_actor WHERE process_task_id = ?")
                .bind(task_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(rows.into_iter().map(|r| r.get::<String, _>("actor_id")).collect())
        })
    }

    fn add_task_actor(&self, task_id: i64, actors: &[String]) -> JeeflowResult<()> {
        self.block_on(async {
            for actor in actors {
                sqlx::query("INSERT INTO wf_process_task_actor (process_task_id, actor_id) VALUES (?, ?)")
                    .bind(task_id)
                    .bind(actor)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn remove_task_actor(&self, task_id: i64, actors: &[String]) -> JeeflowResult<()> {
        self.block_on(async {
            for actor in actors {
                sqlx::query("DELETE FROM wf_process_task_actor WHERE process_task_id = ? AND actor_id = ?")
                    .bind(task_id)
                    .bind(actor)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn create_cc_instance(&self, instance_id: i64, creator: &str, actor_ids: &[String]) -> JeeflowResult<()> {
        self.block_on(async {
            for actor in actor_ids {
                sqlx::query("INSERT INTO wf_cc_instance (process_instance_id, actor_id, state, create_user) VALUES (?, ?, 0, ?)")
                    .bind(instance_id)
                    .bind(actor)
                    .bind(creator)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            }
            Ok(())
        })
    }

    fn update_cc_status(&self, instance_id: i64, actor_id: &str) -> JeeflowResult<()> {
        self.block_on(async {
            sqlx::query("UPDATE wf_cc_instance SET state=1 WHERE process_instance_id=? AND actor_id=?")
                .bind(instance_id)
                .bind(actor_id)
                .execute(&self.pool)
                .await
                .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(())
        })
    }

    fn page_todo_tasks(&self, _query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> { Ok(PageResult::empty()) }
    fn page_done_tasks(&self, _query: &PageQuery) -> JeeflowResult<PageResult<TaskRow>> { Ok(PageResult::empty()) }
    fn page_instances(&self, _query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> { Ok(PageResult::empty()) }
    fn page_cc_instances(&self, _query: &PageQuery) -> JeeflowResult<PageResult<InstanceRow>> { Ok(PageResult::empty()) }
    fn page_defines(&self, _query: &PageQuery) -> JeeflowResult<PageResult<DefineRow>> { Ok(PageResult::empty()) }

    fn count_todo_tasks(&self, user_id: &str) -> JeeflowResult<i64> {
        self.block_on(async {
            let row = sqlx::query(
                "SELECT COUNT(DISTINCT t.id) as cnt FROM wf_process_task t INNER JOIN wf_process_task_actor ta ON t.id = ta.process_task_id WHERE ta.actor_id = ? AND t.state = 10"
            )
            .bind(user_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| JeeflowError::Internal(e.to_string()))?;
            Ok(row.get::<i64, _>("cnt"))
        })
    }
}

// ═══════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_mysql_not_empty() {
        let schema = schema_mysql();
        assert!(!schema.is_empty());
    }

    #[test]
    fn test_schema_contains_8_tables() {
        let schema = schema_mysql();
        let create_count = schema.matches("CREATE TABLE").count();
        assert_eq!(create_count, 8, "Schema should contain exactly 8 CREATE TABLE statements");
    }

    #[test]
    fn test_schema_table_names() {
        let schema = schema_mysql();
        let expected_tables = [
            "wf_process_define", "wf_process_instance", "wf_process_task",
            "wf_process_task_actor", "wf_cc_instance", "wf_process_design",
            "wf_process_design_his", "wf_process_surrogate",
        ];
        for table in &expected_tables {
            assert!(schema.contains(table), "Schema should contain table: {}", table);
        }
    }

    #[test]
    fn test_schema_ddl_syntax() {
        let schema = schema_mysql();
        let create_count = schema.matches("CREATE TABLE").count();
        let if_not_exists_count = schema.matches("IF NOT EXISTS").count();
        assert_eq!(create_count, if_not_exists_count, "All CREATE TABLE should use IF NOT EXISTS");
    }

    #[test]
    fn test_schema_primary_keys() {
        let schema = schema_mysql();
        let pk_count = schema.matches("PRIMARY KEY").count();
        assert_eq!(pk_count, 8, "Each table should have a PRIMARY KEY");
    }

    #[test]
    fn test_schema_indexes() {
        let schema = schema_mysql();
        assert!(schema.contains("idx_define_id"), "Should have idx_define_id");
        assert!(schema.contains("idx_instance_id"), "Should have idx_instance_id");
        assert!(schema.contains("idx_actor_id"), "Should have idx_actor_id");
        assert!(schema.contains("idx_operator"), "Should have idx_operator");
    }

    #[test]
    fn test_schema_engine() {
        let schema = schema_mysql();
        let engine_count = schema.matches("ENGINE=InnoDB").count();
        assert_eq!(engine_count, 8, "All tables should use InnoDB engine");
    }

    #[test]
    fn test_schema_charset() {
        let schema = schema_mysql();
        let charset_count = schema.matches("utf8mb4").count();
        assert!(charset_count >= 8, "All tables should use utf8mb4 charset");
    }

    #[test]
    fn test_flow_data_to_json_empty() {
        let fd = jeeflow_core::json::FlowData::new();
        assert_eq!(flow_data_to_json(&fd), "{}");
    }

    #[test]
    fn test_parse_flow_data_none() {
        let fd = parse_flow_data(&None);
        assert!(fd.is_empty());
    }

    #[test]
    fn test_parse_flow_data_empty_string() {
        let fd = parse_flow_data(&Some("".to_string()));
        assert!(fd.is_empty());
    }

    #[test]
    fn test_parse_flow_data_with_values() {
        let fd = parse_flow_data(&Some(r#"{"key1":"val1","key2":42}"#.to_string()));
        assert_eq!(fd.len(), 2);
    }

    #[test]
    fn test_flow_data_to_json_with_values() {
        let mut fd = jeeflow_core::json::FlowData::new();
        fd.insert("name".to_string(), jeeflow_core::json::JsonValue::Str("test".to_string()));
        let json = flow_data_to_json(&fd);
        assert!(json.contains("name"));
        assert!(json.contains("test"));
    }

    #[test]
    fn test_schema_define_table_columns() {
        let schema = schema_mysql();
        // Check key columns in wf_process_define
        assert!(schema.contains("id BIGINT"));
        assert!(schema.contains("name VARCHAR"));
        assert!(schema.contains("display_name VARCHAR"));
        assert!(schema.contains("content LONGTEXT"));
    }

    #[test]
    fn test_schema_instance_table_columns() {
        let schema = schema_mysql();
        assert!(schema.contains("process_define_id BIGINT"));
        assert!(schema.contains("state INT"));
        assert!(schema.contains("operator VARCHAR"));
    }

    #[test]
    fn test_schema_task_table_columns() {
        let schema = schema_mysql();
        assert!(schema.contains("task_name VARCHAR"));
        // The column is named "state" not "task_state"
        assert!(schema.contains("state INT"));
        assert!(schema.contains("perform_type INT"));
    }
}
