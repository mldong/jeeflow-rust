//! jeeflow-persist: DynamicTableWriter trait + PersistPostInterceptor.
//! Implements ARCHIVE/SYNC persist modes (spec/09).
//! Metadata-driven dynamic table writing with field permission filtering.

use jeeflow_core::json::{JsonValue, FlowData};
use jeeflow_core::spi::FlowInterceptor;
use jeeflow_core::engine::Execution;
use jeeflow_core::error::{JeeflowError, JeeflowResult};
use jeeflow_core::parser::{NodeModel, NodeType, ProcessModel};
use std::collections::HashMap;

// ═══════════════════════════════════════════════════════
// TableMeta / FieldMeta — metadata types
// ═══════════════════════════════════════════════════════

/// Field metadata for a dynamic table column.
#[derive(Debug, Clone)]
pub struct FieldMeta {
    /// Column name (e.g. "f_name")
    pub column_name: String,
    /// Database column type (e.g. "VARCHAR(200)")
    pub column_type: String,
    /// Display name
    pub display_name: String,
    /// Whether this field is nullable
    pub nullable: bool,
    /// Default value
    pub default_value: Option<String>,
    /// Field permission: 1=readonly, 2=editable, 3=hidden
    pub permission: i32,
}

impl FieldMeta {
    pub fn new(column_name: &str, column_type: &str, display_name: &str) -> Self {
        FieldMeta {
            column_name: column_name.to_string(),
            column_type: column_type.to_string(),
            display_name: display_name.to_string(),
            nullable: true,
            default_value: None,
            permission: 2, // editable by default
        }
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn with_permission(mut self, permission: i32) -> Self {
        self.permission = permission;
        self
    }

    pub fn with_default(mut self, default: &str) -> Self {
        self.default_value = Some(default.to_string());
        self
    }

    /// Is this field editable (permission=2)?
    pub fn is_editable(&self) -> bool {
        self.permission == 2
    }

    /// Is this field visible (permission != 3)?
    pub fn is_visible(&self) -> bool {
        self.permission != 3
    }
}

/// Table metadata for a dynamic table.
#[derive(Debug, Clone)]
pub struct TableMeta {
    /// Table name (must pass safety check)
    pub table_name: String,
    /// Display name
    pub display_name: String,
    /// Fields
    pub fields: Vec<FieldMeta>,
}

impl TableMeta {
    pub fn new(table_name: &str, display_name: &str) -> Self {
        TableMeta {
            table_name: table_name.to_string(),
            display_name: display_name.to_string(),
            fields: Vec::new(),
        }
    }

    pub fn with_field(mut self, field: FieldMeta) -> Self {
        self.fields.push(field);
        self
    }

    pub fn add_field(&mut self, field: FieldMeta) {
        self.fields.push(field);
    }

    /// Get field by column name.
    pub fn get_field(&self, column_name: &str) -> Option<&FieldMeta> {
        self.fields.iter().find(|f| f.column_name == column_name)
    }

    /// Get editable fields.
    pub fn editable_fields(&self) -> Vec<&FieldMeta> {
        self.fields.iter().filter(|f| f.is_editable()).collect()
    }

    /// Get visible fields.
    pub fn visible_fields(&self) -> Vec<&FieldMeta> {
        self.fields.iter().filter(|f| f.is_visible()).collect()
    }
}

// ═══════════════════════════════════════════════════════
// IDynamicMetaProvider — provides table metadata
// ═══════════════════════════════════════════════════════

/// Provider of dynamic table metadata.
pub trait IDynamicMetaProvider: Send + Sync {
    /// Get table metadata by table name.
    fn get_table_meta(&self, table_name: &str) -> JeeflowResult<Option<TableMeta>>;
    /// List all registered table names.
    fn list_table_names(&self) -> Vec<String>;
}

/// In-memory implementation of IDynamicMetaProvider.
pub struct InMemoryMetaProvider {
    tables: HashMap<String, TableMeta>,
}

impl InMemoryMetaProvider {
    pub fn new() -> Self {
        InMemoryMetaProvider { tables: HashMap::new() }
    }

    pub fn register(&mut self, meta: TableMeta) {
        self.tables.insert(meta.table_name.clone(), meta);
    }
}

impl Default for InMemoryMetaProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl IDynamicMetaProvider for InMemoryMetaProvider {
    fn get_table_meta(&self, table_name: &str) -> JeeflowResult<Option<TableMeta>> {
        Ok(self.tables.get(table_name).cloned())
    }

    fn list_table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }
}

// ═══════════════════════════════════════════════════════
// Table name safety
// ═══════════════════════════════════════════════════════

/// Reserved table name prefixes that are NOT allowed for dynamic tables.
const RESERVED_PREFIXES: &[&str] = &[
    "sys_", "wf_", "mysql.", "information_schema.", "pg_", "sqlite_",
];

/// Check if a table name is safe for dynamic writing.
/// Rejects:
/// - Names starting with reserved prefixes (sys_, wf_, mysql., etc.)
/// - Names containing SQL injection characters
/// - Empty names
/// - Names with spaces or special characters (only alphanumeric + underscore allowed)
pub fn is_table_name_safe(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }

    // Only allow alphanumeric + underscore (no dots, dashes, spaces, etc.)
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return false;
    }

    // Reject reserved prefixes
    let lower = name.to_lowercase();
    for prefix in RESERVED_PREFIXES {
        if lower.starts_with(prefix) {
            return false;
        }
    }

    true
}

// ═══════════════════════════════════════════════════════
// DynamicTableWriter trait
// ═══════════════════════════════════════════════════════

/// Dynamic table writer — writes process data to dynamic business tables.
pub trait DynamicTableWriter: Send + Sync {
    /// Insert a row into the dynamic table. Returns the row id.
    fn insert(&self, table_name: &str, data: &HashMap<String, JsonValue>) -> JeeflowResult<i64>;

    /// Update a row in the dynamic table by ID.
    fn update(&self, table_name: &str, id: i64, data: &HashMap<String, JsonValue>) -> JeeflowResult<()>;

    /// Check if a row exists by ID.
    fn exists(&self, table_name: &str, id: i64) -> JeeflowResult<bool>;

    /// Check if a row exists by key column (persist 幂等键 process_instance_id，对齐 Java/Go)。
    fn exists_by_key(&self, table_name: &str, key: &str, value: i64) -> JeeflowResult<bool>;

    /// Update a row by key column (对齐 Java/Go writer.update(tableName, data, key, value))。
    fn update_by_key(&self, table_name: &str, data: &HashMap<String, JsonValue>, key: &str, value: i64) -> JeeflowResult<()>;

    /// 探测表中实际存在的列（状态字段 {节点ID}_{状态码}/{节点ID} 列过滤，对齐 Java/Go filterColumns）。
    /// 默认实现：全部保留（内存 writer 无 schema 概念）。
    fn columns(&self, _table_name: &str, candidates: &[String]) -> JeeflowResult<Vec<String>> {
        Ok(candidates.to_vec())
    }

    /// Fill system fields (create_time, update_time, create_user, etc.)
    fn fill_system_fields(&self, data: &mut HashMap<String, JsonValue>, operator: &str, is_insert: bool) {
        let now = chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        if is_insert {
            data.insert("create_time".to_string(), JsonValue::Str(now.clone()));
            data.insert("create_user".to_string(), JsonValue::Str(operator.to_string()));
        }
        data.insert("update_time".to_string(), JsonValue::Str(now));
        data.insert("update_user".to_string(), JsonValue::Str(operator.to_string()));
    }
}

// ═══════════════════════════════════════════════════════
// MetaTableReader — reads back persisted data
// ═══════════════════════════════════════════════════════

/// Reads back data from dynamic tables.
pub trait MetaTableReader: Send + Sync {
    /// Read a row by ID from a dynamic table.
    fn read_by_id(&self, table_name: &str, id: i64) -> JeeflowResult<Option<HashMap<String, JsonValue>>>;
    /// Read all rows from a dynamic table.
    fn read_all(&self, table_name: &str) -> JeeflowResult<Vec<HashMap<String, JsonValue>>>;
    /// Count rows in a dynamic table.
    fn count(&self, table_name: &str) -> JeeflowResult<i64>;
}

// ═══════════════════════════════════════════════════════
// Persist mode
// ═══════════════════════════════════════════════════════

/// Persist mode for process data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistMode {
    /// ARCHIVE: Write form data to dynamic table, store reference ID in process variables.
    Archive,
    /// SYNC: Sync form data to dynamic table on every task completion.
    Sync,
}

impl PersistMode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "ARCHIVE" => Some(PersistMode::Archive),
            "SYNC" => Some(PersistMode::Sync),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            PersistMode::Archive => "ARCHIVE",
            PersistMode::Sync => "SYNC",
        }
    }
}

// ═══════════════════════════════════════════════════════
// PersistPostInterceptor — spec/09
// ═══════════════════════════════════════════════════════

/// 工作流业务数据入库适配拦截器（对齐 Java PersistPostInterceptor / Go persist.PostHandle）——
/// 按流程定义 `persistMode` 分派两种模式：
///
/// - **ARCHIVE（结束归档）**：仅结束节点 + 实例 FINISHED + submitType=AGREE(1) 时
///   将 f_ 表单全量 INSERT 业务表一次；幂等键 process_instance_id（先查后插）。
/// - **SYNC（同步演进）**：发起即 INSERT（start 节点，f_ 全量 + 状态字段）→
///   任务节点推进 UPDATE（f_ 按目标节点 properties.field.PERMISSION_* 过滤 +
///   tf_ 冗余 + 状态字段=DOING）→ 结束节点定稿 UPDATE（最终状态 FINISHED/REJECT）。
///
/// 字段提取对齐 Java/Go：从**实例变量**取 f_/tf_ 前缀键**去前缀**后按列名匹配
/// （业务表列 company_name 对应表单字段 f_company_name）；旧实现误用 execution.args
/// （带前缀键）导致 filter 恒空、静默不落库（L2-06/07 根因）。
pub struct PersistPostInterceptor {
    meta_provider: std::sync::Arc<dyn IDynamicMetaProvider>,
    table_writer: std::sync::Arc<dyn DynamicTableWriter>,
}

impl PersistPostInterceptor {
    pub fn new(
        meta_provider: std::sync::Arc<dyn IDynamicMetaProvider>,
        table_writer: std::sync::Arc<dyn DynamicTableWriter>,
    ) -> Self {
        PersistPostInterceptor { meta_provider, table_writer }
    }

    // ─── ARCHIVE（缺省：结束同意归档，对齐 Java interceptArchive） ───

    fn intercept_archive(&self, exec: &Execution, table: &str) -> JeeflowResult<()> {
        // 时机：仅结束节点 + 流程正常完成（FINISHED=20）且同意（submitType=1）
        let is_end = exec.current_node
            .as_ref()
            .map(|n| n.node_type == NodeType::End)
            .unwrap_or(false);
        if !is_end {
            return Ok(());
        }
        if exec.process_instance.state != 20 {
            return Ok(());
        }
        if exec.args.get_i64("submitType") != Some(1) {
            return Ok(());
        }
        // 幂等：以 process_instance_id 为键，先查后插
        if self.table_writer.exists_by_key(table, "process_instance_id", exec.process_instance.instance_id)? {
            return Ok(());
        }

        let mut data = self.extract_fields(&exec.process_instance.variables, None, false, true); // 只 f_ 全量
        self.fill_context(&mut data, exec);
        let meta = self.meta(table)?;
        let filtered = self.filter_editable(&meta, &data);
        if filtered.is_empty() {
            return Ok(());
        }
        let mut row_data = filtered;
        self.table_writer.fill_system_fields(&mut row_data, &exec.operator, true);
        self.table_writer.insert(table, &row_data)?;
        Ok(())
    }

    // ─── SYNC（同步演进：发起 INSERT → 任务节点 UPDATE → 结束定稿） ───

    fn intercept_sync(&self, exec: &Execution, table: &str) -> JeeflowResult<()> {
        let node = match &exec.current_node {
            Some(n) => n,
            None => return Ok(()),
        };

        let instance = &exec.process_instance;
        let is_task = matches!(node.node_type,
            NodeType::Task | NodeType::Custom);
        let exists = self.table_writer
            .exists_by_key(table, "process_instance_id", instance.instance_id)?;

        // 任务节点按目标节点 properties.field.PERMISSION_* 过滤业务字段；
        // 非任务节点（start/结束）不带出 f_——start 首次 INSERT 例外（全量）
        let field_perm = if is_task {
            self.resolve_field_permission(node)
        } else {
            None
        };
        let include_fields = !exists || is_task;
        let mut data = self.extract_fields(&instance.variables, field_perm.as_ref(), true, include_fields);

        // 状态字段：优先 {节点ID}_{状态码} 列，无则 {节点ID} 列（列探测过滤）。
        // 任务节点写 DOING(10)——execPost 在流转链之后触发，不能用实例状态；
        // 结束节点写实例最终状态（FINISHED/REJECT）；start 节点无状态列，跳过。
        let state_code = if is_task {
            Some(10)
        } else if node.node_type == NodeType::End {
            Some(instance.state)
        } else {
            None
        };
        if let Some(code) = state_code {
            // 列探测失败（如库连接异常）必须显性暴露，不静默吞（对齐 issues/60 原则）
            self.put_state_field(table, &mut data, &node.id, code)?;
        }

        self.fill_context(&mut data, exec);
        let meta = self.meta(table)?;
        let filtered = self.filter_editable(&meta, &data);
        if filtered.is_empty() {
            return Ok(());
        }
        let mut row_data = filtered;
        if exists {
            self.table_writer.fill_system_fields(&mut row_data, &exec.operator, false);
            self.table_writer
                .update_by_key(table, &row_data, "process_instance_id", instance.instance_id)
        } else {
            self.table_writer.fill_system_fields(&mut row_data, &exec.operator, true);
            self.table_writer.insert(table, &row_data)?;
            Ok(())
        }
    }

    // ─── 公共 ───────────────────────────────────────────────────────────

    /// 表名：relTableName 缺省回落流程 name（对齐 Java resolveTableName）。
    fn resolve_table(&self, model: &ProcessModel) -> Option<String> {
        let name = model.rel_table_name.as_deref().unwrap_or(&model.name);
        let trimmed = name.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    }

    fn meta(&self, table: &str) -> JeeflowResult<TableMeta> {
        self.meta_provider.get_table_meta(table)?
            .ok_or_else(|| JeeflowError::Business(format!("表元数据不存在: {}", table)))
    }

    /// 按列权限过滤出可写字段（对齐 Java writer 列权限过滤；表无列则不写）。
    fn filter_editable(&self, meta: &TableMeta, data: &HashMap<String, JsonValue>) -> HashMap<String, JsonValue> {
        let mut filtered = HashMap::new();
        for field in &meta.fields {
            if field.is_editable() {
                if let Some(val) = data.get(&field.column_name) {
                    filtered.insert(field.column_name.clone(), val.clone());
                }
            }
        }
        filtered
    }

    /// 提取字段（对齐 Java/Go extractFields）：f_ 去前缀（SYNC 任务节点按字段权限过滤），
    /// tf_ 去前缀冗余（有列则写，列过滤由 filter_editable 做）。
    fn extract_fields(
        &self,
        variables: &FlowData,
        field_perm: Option<&HashMap<String, JsonValue>>,
        include_task_fields: bool,
        include_form_fields: bool,
    ) -> HashMap<String, JsonValue> {
        let mut data = HashMap::new();
        for (key, value) in variables.iter() {
            if let Some(name) = key.strip_prefix("f_") {
                if !name.is_empty() && include_form_fields && self.is_editable(field_perm, name) {
                    data.insert(name.to_string(), value.clone());
                }
            } else if let Some(name) = key.strip_prefix("tf_") {
                if !name.is_empty() && include_task_fields {
                    data.insert(name.to_string(), value.clone());
                }
            }
        }
        data
    }

    /// 节点字段权限（properties.field 的 PERMISSION_x；缺省=全部可编辑）。
    fn resolve_field_permission(&self, node: &NodeModel) -> Option<HashMap<String, JsonValue>> {
        let field = node.properties.get("field")?;
        let obj = field.as_object()?;
        if obj.is_empty() {
            return None;
        }
        Some(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    /// 字段可编辑判定（对齐 Java isEditable）：无声明或 EDIT(2) 可更新；READ_ONLY(1)/HIDDEN(3) 不更新。
    /// 键格式兼容两种（issues/25）：PERMISSION_f_{全名}（前端约定，优先）/ PERMISSION_{去前缀名}。
    fn is_editable(
        &self,
        field_perm: Option<&HashMap<String, JsonValue>>,
        field_name: &str,
    ) -> bool {
        let perm = match field_perm {
            Some(p) if !p.is_empty() => p,
            _ => return true,
        };
        let val = perm.get(&format!("PERMISSION_f_{field_name}"))
            .or_else(|| perm.get(&format!("PERMISSION_{field_name}")));
        match val {
            None => true,
            Some(v) => v.as_i64().unwrap_or(0) == 2,
        }
    }

    /// 状态字段写入：优先 {节点ID}_{状态码} 列，无则 {节点ID} 列（列探测过滤）。
    fn put_state_field(&self, table: &str, data: &mut HashMap<String, JsonValue>, node_id: &str, state_code: i32)
        -> JeeflowResult<()> {
        if node_id.is_empty() {
            return Ok(());
        }
        let candidates = vec![
            format!("{node_id}_{state_code}"),
            node_id.to_string(),
        ];
        let kept = self.table_writer.columns(table, &candidates)?;
        if let Some(col) = kept.into_iter().next() {
            data.insert(col, JsonValue::Number(state_code as f64));
        }
        Ok(())
    }

    /// 流程上下文字段（蛇形列名约定，与 writer 系统字段一致；对齐 Java fillContext）。
    fn fill_context(&self, data: &mut HashMap<String, JsonValue>, exec: &Execution) {
        let instance = &exec.process_instance;
        data.entry("process_instance_id".to_string())
            .or_insert_with(|| JsonValue::Number(instance.instance_id as f64));
        data.entry("apply_user_id".to_string())
            .or_insert_with(|| JsonValue::Str(instance.operator.clone()));
        if let Some(dept) = instance.variables.get_str("u_deptId") {
            data.entry("apply_dept_id".to_string())
                .or_insert_with(|| JsonValue::Str(dept.to_string()));
        }
    }
}

impl FlowInterceptor for PersistPostInterceptor {
    fn intercept(&self, execution: &mut Execution) -> JeeflowResult<()> {
        // Determine persist mode from process model
        let persist_mode = execution.process_model.persist_mode.as_deref();
        if persist_mode.is_none() {
            return Ok(()); // No persist configured
        }
        let mode = match PersistMode::from_str(persist_mode.unwrap()) {
            Some(m) => m,
            None => return Ok(()), // 未知模式：静默跳过
        };
        let table = match self.resolve_table(&execution.process_model) {
            Some(t) => t,
            None => return Ok(()), // relTableName 与流程名均空
        };
        if !is_table_name_safe(&table) {
            // 配置错误必须显性暴露（与 Java/Go 一致），不静默吞
            return Err(JeeflowError::Business(format!("非法业务表名: {table}")));
        }

        match mode {
            PersistMode::Archive => self.intercept_archive(execution, &table),
            PersistMode::Sync => self.intercept_sync(execution, &table),
        }
    }

    fn order(&self) -> i32 {
        100 // Run after other interceptors
    }
}

// ═══════════════════════════════════════════════════════
// In-memory implementations for testing
// ═══════════════════════════════════════════════════════

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Mutex;

/// In-memory DynamicTableWriter for testing.
pub struct InMemoryTableWriter {
    id_counter: AtomicI64,
    tables: Mutex<HashMap<String, HashMap<i64, HashMap<String, JsonValue>>>>,
}

impl InMemoryTableWriter {
    pub fn new() -> Self {
        InMemoryTableWriter {
            id_counter: AtomicI64::new(1),
            tables: Mutex::new(HashMap::new()),
        }
    }

    /// Get all rows for a table (for testing).
    pub fn get_table_rows(&self, table_name: &str) -> Vec<HashMap<String, JsonValue>> {
        let tables = self.tables.lock().unwrap();
        tables.get(table_name)
            .map(|t| t.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Get a row by ID (for testing).
    pub fn get_row(&self, table_name: &str, id: i64) -> Option<HashMap<String, JsonValue>> {
        let tables = self.tables.lock().unwrap();
        tables.get(table_name)
            .and_then(|t| t.get(&id).cloned())
    }

    /// Get a row by key column (for testing, e.g. process_instance_id).
    pub fn get_row_by_key(&self, table_name: &str, key: &str, value: i64) -> Option<HashMap<String, JsonValue>> {
        let tables = self.tables.lock().unwrap();
        tables.get(table_name)
            .and_then(|t| t.values().find(|row| row.get(key).and_then(|v| v.as_i64()) == Some(value)).cloned())
    }
}

impl Default for InMemoryTableWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl DynamicTableWriter for InMemoryTableWriter {
    fn insert(&self, table_name: &str, data: &HashMap<String, JsonValue>) -> JeeflowResult<i64> {
        if !is_table_name_safe(table_name) {
            return Err(JeeflowError::Business(format!("非法表名: {}", table_name)));
        }
        let id = self.id_counter.fetch_add(1, Ordering::SeqCst);
        let mut row = data.clone();
        row.insert("id".to_string(), JsonValue::Number(id as f64));

        let mut tables = self.tables.lock().unwrap();
        let table = tables.entry(table_name.to_string()).or_insert_with(HashMap::new);
        table.insert(id, row);
        Ok(id)
    }

    fn update(&self, table_name: &str, id: i64, data: &HashMap<String, JsonValue>) -> JeeflowResult<()> {
        let mut tables = self.tables.lock().unwrap();
        if let Some(table) = tables.get_mut(table_name) {
            if let Some(row) = table.get_mut(&id) {
                for (k, v) in data {
                    row.insert(k.clone(), v.clone());
                }
                return Ok(());
            }
        }
        Err(JeeflowError::Business(format!("Row {} not found in {}", id, table_name)))
    }

    fn exists(&self, table_name: &str, id: i64) -> JeeflowResult<bool> {
        let tables = self.tables.lock().unwrap();
        Ok(tables.get(table_name)
            .map(|t| t.contains_key(&id))
            .unwrap_or(false))
    }

    fn exists_by_key(&self, table_name: &str, key: &str, value: i64) -> JeeflowResult<bool> {
        let tables = self.tables.lock().unwrap();
        Ok(tables.get(table_name)
            .map(|t| t.values().any(|row| row.get(key).and_then(|v| v.as_i64()) == Some(value)))
            .unwrap_or(false))
    }

    fn update_by_key(&self, table_name: &str, data: &HashMap<String, JsonValue>, key: &str, value: i64)
        -> JeeflowResult<()> {
        let mut tables = self.tables.lock().unwrap();
        if let Some(table) = tables.get_mut(table_name) {
            if let Some(row) = table.values_mut().find(|row| {
                row.get(key).and_then(|v| v.as_i64()) == Some(value)
            }) {
                for (k, v) in data {
                    row.insert(k.clone(), v.clone());
                }
                return Ok(());
            }
        }
        Err(JeeflowError::Business(format!(
            "Row with {}={} not found in {}", key, value, table_name
        )))
    }
}

/// In-memory MetaTableReader for testing.
pub struct InMemoryMetaTableReader {
    tables: Mutex<HashMap<String, Vec<HashMap<String, JsonValue>>>>,
}

impl InMemoryMetaTableReader {
    pub fn new() -> Self {
        InMemoryMetaTableReader { tables: Mutex::new(HashMap::new()) }
    }

    pub fn load(&self, table_name: &str, rows: Vec<HashMap<String, JsonValue>>) {
        let mut tables = self.tables.lock().unwrap();
        tables.insert(table_name.to_string(), rows);
    }
}

impl Default for InMemoryMetaTableReader {
    fn default() -> Self {
        Self::new()
    }
}

impl MetaTableReader for InMemoryMetaTableReader {
    fn read_by_id(&self, table_name: &str, id: i64) -> JeeflowResult<Option<HashMap<String, JsonValue>>> {
        let tables = self.tables.lock().unwrap();
        Ok(tables.get(table_name)
            .and_then(|rows| rows.iter().find(|r| {
                r.get("id").and_then(|v| v.as_i64()) == Some(id)
            }).cloned()))
    }

    fn read_all(&self, table_name: &str) -> JeeflowResult<Vec<HashMap<String, JsonValue>>> {
        let tables = self.tables.lock().unwrap();
        Ok(tables.get(table_name).cloned().unwrap_or_default())
    }

    fn count(&self, table_name: &str) -> JeeflowResult<i64> {
        let tables = self.tables.lock().unwrap();
        Ok(tables.get(table_name).map(|r| r.len() as i64).unwrap_or(0))
    }
}

// ═══════════════════════════════════════════════════════
// HandlerRegistry 注册助手（issues/60，对齐 Go RegisterMeta）
// ═══════════════════════════════════════════════════════

use jeeflow_core::metadata::{HandlerMeta, HandlerRegistry};

/// 将 PersistPostInterceptor 元数据注册进 HandlerRegistry（SPI 字典源）。
pub fn register_persist_meta(reg: &mut HandlerRegistry) {
    reg.register(HandlerMeta {
        handler_type: "FlowInterceptor".into(),
        class_name: "com.mldong.jeeflow.persist.interceptor.PersistPostInterceptor".into(),
        display_name: "业务数据自动入库".into(),
        order: 0,
        group: "post".into(),
    });
}

// ═══════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use jeeflow_core::model::{ProcessDefine, ProcessInstance};

    // ─── Table name safety tests ───

    #[test]
    fn test_table_name_safe_valid() {
        assert!(is_table_name_safe("oa_leave"));
        assert!(is_table_name_safe("biz_order"));
        assert!(is_table_name_safe("table123"));
        assert!(is_table_name_safe("my_table_name"));
    }

    #[test]
    fn test_table_name_safe_reject_sys_prefix() {
        assert!(!is_table_name_safe("sys_user"));
        assert!(!is_table_name_safe("sys_config"));
        assert!(!is_table_name_safe("SYS_USER"));
    }

    #[test]
    fn test_table_name_safe_reject_wf_prefix() {
        assert!(!is_table_name_safe("wf_process_define"));
        assert!(!is_table_name_safe("wf_instance"));
    }

    #[test]
    fn test_table_name_safe_reject_injection() {
        assert!(!is_table_name_safe("table; DROP TABLE users"));
        assert!(!is_table_name_safe("table' OR 1=1"));
        assert!(!is_table_name_safe("table\" OR \"1\"=\"1"));
        assert!(!is_table_name_safe("table--comment"));
        assert!(!is_table_name_safe("table.name"));
        assert!(!is_table_name_safe("table name"));
    }

    #[test]
    fn test_table_name_safe_reject_empty() {
        assert!(!is_table_name_safe(""));
    }

    #[test]
    fn test_table_name_safe_reject_mysql_prefix() {
        assert!(!is_table_name_safe("mysql.user"));
    }

    // ─── FieldMeta / TableMeta tests ───

    #[test]
    fn test_field_meta_permissions() {
        let f = FieldMeta::new("f_name", "VARCHAR(200)", "名称").with_permission(2);
        assert!(f.is_editable());
        assert!(f.is_visible());

        let f_ro = FieldMeta::new("f_id", "BIGINT", "ID").with_permission(1);
        assert!(!f_ro.is_editable());
        assert!(f_ro.is_visible());

        let f_hidden = FieldMeta::new("f_secret", "VARCHAR(100)", "秘密").with_permission(3);
        assert!(!f_hidden.is_editable());
        assert!(!f_hidden.is_visible());
    }

    #[test]
    fn test_table_meta_fields() {
        let meta = TableMeta::new("oa_leave", "请假表")
            .with_field(FieldMeta::new("f_name", "VARCHAR(100)", "姓名").with_permission(2))
            .with_field(FieldMeta::new("f_days", "INT", "天数").with_permission(2))
            .with_field(FieldMeta::new("f_status", "VARCHAR(20)", "状态").with_permission(1));

        assert_eq!(meta.fields.len(), 3);
        assert_eq!(meta.editable_fields().len(), 2);
        assert_eq!(meta.visible_fields().len(), 3);
        assert!(meta.get_field("f_name").is_some());
        assert!(meta.get_field("f_unknown").is_none());
    }

    // ─── 拦截器回归（对齐 Java/Go persist 语义；L2-06/07 根因回归） ───

    /// 构造最小 Execution：单 current_node + 指定 persistMode/relTableName/实例状态。
    fn make_exec(
        node_id: &str,
        node_type: NodeType,
        persist_mode: Option<&str>,
        table: Option<&str>,
        vars: FlowData,
        state: i32,
        submit_type: Option<i64>,
    ) -> Execution {
        let model = ProcessModel {
            name: "proc".to_string(),
            display_name: "proc".to_string(),
            model_type: "6".to_string(),
            expire_time: None,
            persist_mode: persist_mode.map(|s| s.to_string()),
            rel_table_name: table.map(|s| s.to_string()),
            nodes: vec![NodeModel {
                id: node_id.to_string(),
                node_type,
                display_name: node_id.to_string(),
                properties: HashMap::new(),
            }],
            edges: vec![],
        };
        let instance = ProcessInstance {
            instance_id: 9001,
            parent_id: None,
            define_id: 1,
            state,
            parent_node_name: None,
            business_no: None,
            operator: "user1".to_string(),
            expire_time: None,
            variables: vars,
            tasks: vec![],
            create_time: None,
            create_user: Some("user1".to_string()),
            update_time: None,
            update_user: None,
            define: None,
        };
        let define = ProcessDefine {
            id: 1, name: "proc".into(), display_name: "proc".into(),
            define_type: "approval".into(), state: 1, content: vec![],
            version: 1, create_time: None, create_user: None,
            update_time: None, update_user: None,
        };
        let mut exec = Execution::new(instance, model, define, "user1", {
            let mut args = FlowData::new();
            if let Some(st) = submit_type {
                args.insert_i64("submitType", st);
            }
            args
        });
        exec.current_node = Some(exec.process_model.nodes[0].clone());
        exec
    }

    fn set_node_field_perm(exec: &mut Execution, perm: Vec<(&str, i64)>) {
        if let Some(node) = exec.current_node.as_mut() {
            node.properties.insert(
                "field".to_string(),
                JsonValue::Object(
                    perm.into_iter()
                        .map(|(k, v)| (k.to_string(), JsonValue::Number(v as f64)))
                        .collect(),
                ),
            );
        }
    }

    fn merchant_meta() -> TableMeta {
        TableMeta::new("merchant_settlement", "结算表")
            .with_field(FieldMeta::new("company_name", "VARCHAR(255)", "公司"))
            .with_field(FieldMeta::new("contact_name", "VARCHAR(100)", "联系人"))
            .with_field(FieldMeta::new("contact_phone", "VARCHAR(20)", "电话"))
            .with_field(FieldMeta::new("comment", "VARCHAR(100)", "意见"))
            .with_field(FieldMeta::new("process_instance_id", "BIGINT", "实例ID"))
            .with_field(FieldMeta::new("apply_user_id", "BIGINT", "申请人"))
            .with_field(FieldMeta::new("apply_dept_id", "BIGINT", "部门"))
    }

    fn start_vars() -> FlowData {
        let mut v = FlowData::new();
        v.insert_str("f_company_name", "Acme");
        v.insert_str("f_contact_name", "Bob");
        v.insert_str("u_deptId", "D01");
        v
    }

    /// P1：SYNC 发起即 INSERT——f_ 去前缀按列写入 + 流程上下文 + 系统字段
    /// （旧实现读 execution.args 带前缀键，filter 恒空 → 静默不落库，L2-07 根因）。
    #[test]
    fn test_persist_sync_insert_on_start() {
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let mut provider = InMemoryMetaProvider::new();
        provider.register(merchant_meta());
        let interceptor = PersistPostInterceptor::new(std::sync::Arc::new(provider), writer.clone());

        let mut exec = make_exec("start", NodeType::Start, Some("SYNC"),
            Some("merchant_settlement"), start_vars(), 10, None);
        exec.process_instance.state = 10;
        interceptor.intercept(&mut exec).unwrap();

        let row = writer.get_row_by_key("merchant_settlement", "process_instance_id", 9001)
            .expect("SYNC 发起应 INSERT");
        assert_eq!(row.get("company_name").and_then(|v| v.as_str()), Some("Acme"));
        assert_eq!(row.get("contact_name").and_then(|v| v.as_str()), Some("Bob"));
        assert_eq!(row.get("process_instance_id").and_then(|v| v.as_i64()), Some(9001));
        assert_eq!(row.get("apply_user_id").and_then(|v| v.as_str()), Some("user1"));
        assert_eq!(row.get("apply_dept_id").and_then(|v| v.as_str()), Some("D01"));
        assert!(row.contains_key("create_time"));
        // f_ 前缀键与 u_ 上下文键不得落列
        assert!(!row.contains_key("f_company_name"));
        assert!(!row.contains_key("u_deptId"));
    }

    /// P2：SYNC 任务节点 UPDATE——PERMISSION_f_x=1 只读字段不落库、=2 可编辑更新、
    /// tf_ 去前缀冗余（对齐 Java isEditable / Go TestSyncModeFullCycle；L2-07 字段权限断言）。
    #[test]
    fn test_persist_sync_task_update_field_permission() {
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let mut provider = InMemoryMetaProvider::new();
        provider.register(merchant_meta());
        let interceptor = PersistPostInterceptor::new(std::sync::Arc::new(provider), writer.clone());

        // 发起 INSERT
        let mut start_exec = make_exec("start", NodeType::Start, Some("SYNC"),
            Some("merchant_settlement"), start_vars(), 10, None);
        interceptor.intercept(&mut start_exec).unwrap();

        // approve 任务节点推进：company_name 只读（改不动）、contact_name 可编辑、tf_comment 冗余
        let mut vars = start_vars();
        vars.insert_str("f_company_name", "HACK");   // 只读 → 不更新
        vars.insert_str("f_contact_name", "NewBob"); // 可编辑 → 更新
        vars.insert_str("tf_comment", "同意");        // tf_ 去前缀
        let mut task_exec = make_exec("approve", NodeType::Task, Some("SYNC"),
            Some("merchant_settlement"), vars, 10, Some(1));
        task_exec.operator = "user2".into(); // 不同操作人，验证 create 组不被 update 重写
        set_node_field_perm(&mut task_exec,
            vec![("PERMISSION_f_company_name", 1), ("PERMISSION_f_contact_name", 2)]);
        interceptor.intercept(&mut task_exec).unwrap();

        let row = writer.get_row_by_key("merchant_settlement", "process_instance_id", 9001).unwrap();
        assert_eq!(row.get("company_name").and_then(|v| v.as_str()), Some("Acme"), "只读字段不得被任务节点更新");
        assert_eq!(row.get("contact_name").and_then(|v| v.as_str()), Some("NewBob"));
        assert_eq!(row.get("comment").and_then(|v| v.as_str()), Some("同意"));
        assert_eq!(row.get("create_user").and_then(|v| v.as_str()), Some("user1"), "update 只填 update 组");
        assert_eq!(row.get("update_user").and_then(|v| v.as_str()), Some("user2"));
    }

    /// P3：ARCHIVE 仅「结束节点 + FINISHED(20) + submitType=1(同意)」落库一次；
    /// 发起/办理不写（L2-06 start_empty 断言）；驳回不写；幂等先查后插。
    #[test]
    fn test_persist_archive_only_end_finished_agree() {
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let mut provider = InMemoryMetaProvider::new();
        provider.register(merchant_meta());
        let interceptor = PersistPostInterceptor::new(std::sync::Arc::new(provider), writer.clone());

        // 发起（start 节点）→ 不落库
        let mut start_exec = make_exec("start", NodeType::Start, Some("ARCHIVE"),
            Some("merchant_settlement"), start_vars(), 10, Some(0));
        interceptor.intercept(&mut start_exec).unwrap();
        assert!(writer.get_row_by_key("merchant_settlement", "process_instance_id", 9001).is_none());

        // 办理（task 节点，进行中）→ 不落库
        let mut task_exec = make_exec("approve", NodeType::Task, Some("ARCHIVE"),
            Some("merchant_settlement"), start_vars(), 10, Some(1));
        interceptor.intercept(&mut task_exec).unwrap();
        assert!(writer.get_row_by_key("merchant_settlement", "process_instance_id", 9001).is_none());

        // 结束 + 进行中状态 → 不落库
        let mut end_doing = make_exec("end", NodeType::End, Some("ARCHIVE"),
            Some("merchant_settlement"), start_vars(), 10, Some(1));
        interceptor.intercept(&mut end_doing).unwrap();
        assert!(writer.get_row_by_key("merchant_settlement", "process_instance_id", 9001).is_none());

        // 结束 + FINISHED + 同意 → 落库
        let mut end_agree = make_exec("end", NodeType::End, Some("ARCHIVE"),
            Some("merchant_settlement"), start_vars(), 20, Some(1));
        interceptor.intercept(&mut end_agree).unwrap();
        let row = writer.get_row_by_key("merchant_settlement", "process_instance_id", 9001)
            .expect("ARCHIVE 结束同意应 INSERT");
        assert_eq!(row.get("company_name").and_then(|v| v.as_str()), Some("Acme"));
        assert_eq!(row.get("process_instance_id").and_then(|v| v.as_i64()), Some(9001));

        // 幂等：重复触发（同链/跨请求）不得重复插入
        let mut end_again = make_exec("end", NodeType::End, Some("ARCHIVE"),
            Some("merchant_settlement"), start_vars(), 20, Some(1));
        interceptor.intercept(&mut end_again).unwrap();
        assert_eq!(writer.get_table_rows("merchant_settlement").len(), 1);
    }

    /// P4：ARCHIVE 驳回（state=45，submitType=2）不落库。
    #[test]
    fn test_persist_archive_reject_no_insert() {
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let mut provider = InMemoryMetaProvider::new();
        provider.register(merchant_meta());
        let interceptor = PersistPostInterceptor::new(std::sync::Arc::new(provider), writer.clone());

        let mut exec = make_exec("end", NodeType::End, Some("ARCHIVE"),
            Some("merchant_settlement"), start_vars(), 45, Some(2));
        interceptor.intercept(&mut exec).unwrap();
        assert_eq!(writer.get_table_rows("merchant_settlement").len(), 0);
    }

    /// P5：未配置 persistMode → 无动作（不报错、不落库）——L3 演示流无 persist 配置的安全兜底。
    #[test]
    fn test_persist_no_mode_noop() {
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let provider = std::sync::Arc::new(InMemoryMetaProvider::new());
        let interceptor = PersistPostInterceptor::new(provider, writer.clone());

        let mut exec = make_exec("end", NodeType::End, None, None, start_vars(), 20, Some(1));
        interceptor.intercept(&mut exec).unwrap();
        assert!(writer.get_table_rows("merchant_settlement").is_empty());
    }

    /// P6：relTableName 非法 → 显性报错（配置错误不静默吞，对齐 Java/Go）。
    #[test]
    fn test_persist_unsafe_table_errors() {
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let provider = std::sync::Arc::new(InMemoryMetaProvider::new());
        let interceptor = PersistPostInterceptor::new(provider, writer);

        let mut exec = make_exec("start", NodeType::Start, Some("SYNC"),
            Some("sys_user"), start_vars(), 10, None);
        assert!(intercept_err(interceptor.intercept(&mut exec)));
    }

    fn intercept_err(r: JeeflowResult<()>) -> bool {
        matches!(r, Err(JeeflowError::Business(_)))
    }

    // ─── PersistMode tests ───

    #[test]
    fn test_persist_mode_from_str() {
        assert_eq!(PersistMode::from_str("ARCHIVE"), Some(PersistMode::Archive));
        assert_eq!(PersistMode::from_str("archive"), Some(PersistMode::Archive));
        assert_eq!(PersistMode::from_str("SYNC"), Some(PersistMode::Sync));
        assert_eq!(PersistMode::from_str("sync"), Some(PersistMode::Sync));
        assert_eq!(PersistMode::from_str("unknown"), None);
    }

    #[test]
    fn test_persist_mode_as_str() {
        assert_eq!(PersistMode::Archive.as_str(), "ARCHIVE");
        assert_eq!(PersistMode::Sync.as_str(), "SYNC");
    }

    // ─── InMemoryTableWriter tests ───

    #[test]
    fn test_in_memory_writer_insert_and_exists() {
        let writer = InMemoryTableWriter::new();
        let mut data = HashMap::new();
        data.insert("name".to_string(), JsonValue::Str("test".to_string()));

        let id = writer.insert("test_table", &data).unwrap();
        assert!(id > 0);
        assert!(writer.exists("test_table", id).unwrap());
        assert!(!writer.exists("test_table", 999).unwrap());
        assert!(!writer.exists("nonexistent", id).unwrap());
    }

    #[test]
    fn test_in_memory_writer_update() {
        let writer = InMemoryTableWriter::new();
        let mut data = HashMap::new();
        data.insert("name".to_string(), JsonValue::Str("original".to_string()));

        let id = writer.insert("test_table", &data).unwrap();

        let mut update = HashMap::new();
        update.insert("name".to_string(), JsonValue::Str("updated".to_string()));
        writer.update("test_table", id, &update).unwrap();

        let row = writer.get_row("test_table", id).unwrap();
        assert_eq!(row.get("name").and_then(|v| v.as_str()), Some("updated"));
    }

    #[test]
    fn test_in_memory_writer_reject_unsafe_table() {
        let writer = InMemoryTableWriter::new();
        let data = HashMap::new();
        let result = writer.insert("sys_user", &data);
        assert!(result.is_err());
    }

    // ─── InMemoryMetaTableReader tests ───

    #[test]
    fn test_meta_table_reader() {
        let reader = InMemoryMetaTableReader::new();
        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), JsonValue::Number(1.0));
        row1.insert("name".to_string(), JsonValue::Str("test1".to_string()));

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), JsonValue::Number(2.0));
        row2.insert("name".to_string(), JsonValue::Str("test2".to_string()));

        reader.load("test_table", vec![row1, row2]);

        assert_eq!(reader.count("test_table").unwrap(), 2);
        assert_eq!(reader.count("nonexistent").unwrap(), 0);

        let found = reader.read_by_id("test_table", 1).unwrap().unwrap();
        assert_eq!(found.get("name").and_then(|v| v.as_str()), Some("test1"));

        let not_found = reader.read_by_id("test_table", 999).unwrap();
        assert!(not_found.is_none());

        let all = reader.read_all("test_table").unwrap();
        assert_eq!(all.len(), 2);
    }

    // ─── System fields tests ───

    #[test]
    fn test_fill_system_fields_insert() {
        let writer = InMemoryTableWriter::new();
        let mut data = HashMap::new();
        data.insert("name".to_string(), JsonValue::Str("test".to_string()));

        writer.fill_system_fields(&mut data, "user1", true);

        assert!(data.contains_key("create_time"));
        assert!(data.contains_key("create_user"));
        assert!(data.contains_key("update_time"));
        assert!(data.contains_key("update_user"));
        assert_eq!(data.get("create_user").and_then(|v| v.as_str()), Some("user1"));
    }

    #[test]
    fn test_fill_system_fields_update() {
        let writer = InMemoryTableWriter::new();
        let mut data = HashMap::new();
        data.insert("name".to_string(), JsonValue::Str("test".to_string()));

        writer.fill_system_fields(&mut data, "user2", false);

        assert!(!data.contains_key("create_time")); // not filled on update
        assert!(!data.contains_key("create_user"));
        assert!(data.contains_key("update_time"));
        assert!(data.contains_key("update_user"));
        assert_eq!(data.get("update_user").and_then(|v| v.as_str()), Some("user2"));
    }

    // ─── IDynamicMetaProvider tests ───

    #[test]
    fn test_in_memory_meta_provider() {
        let mut provider = InMemoryMetaProvider::new();
        provider.register(TableMeta::new("oa_leave", "请假表"));
        provider.register(TableMeta::new("oa_expense", "报销表"));

        let names = provider.list_table_names();
        assert_eq!(names.len(), 2);

        let meta = provider.get_table_meta("oa_leave").unwrap();
        assert!(meta.is_some());
        assert_eq!(meta.unwrap().display_name, "请假表");

        let none = provider.get_table_meta("nonexistent").unwrap();
        assert!(none.is_none());
    }

    // ─── Additional edge case tests ───

    #[test]
    fn test_field_meta_builder() {
        let f = FieldMeta::new("col1", "VARCHAR(100)", "Column 1")
            .with_nullable(false)
            .with_permission(1)
            .with_default("default");
        assert_eq!(f.column_name, "col1");
        assert_eq!(f.permission, 1);
        assert!(!f.nullable);
        assert_eq!(f.default_value, Some("default".to_string()));
    }

    #[test]
    fn test_table_meta_add_field() {
        let mut meta = TableMeta::new("test_table", "Test");
        meta.add_field(FieldMeta::new("f1", "INT", "Field 1"));
        meta.add_field(FieldMeta::new("f2", "VARCHAR(50)", "Field 2"));
        assert_eq!(meta.fields.len(), 2);
        assert_eq!(meta.editable_fields().len(), 2);
    }

    #[test]
    fn test_table_meta_editable_fields_excludes_readonly() {
        let mut meta = TableMeta::new("test_table", "Test");
        meta.add_field(FieldMeta::new("f1", "INT", "Field 1").with_permission(1)); // readonly
        meta.add_field(FieldMeta::new("f2", "VARCHAR(50)", "Field 2").with_permission(2)); // editable
        meta.add_field(FieldMeta::new("f3", "VARCHAR(50)", "Field 3").with_permission(3)); // hidden
        assert_eq!(meta.editable_fields().len(), 1);
        assert_eq!(meta.editable_fields()[0].column_name, "f2");
    }

    #[test]
    fn test_table_name_safe_reject_semicolon() {
        assert!(!is_table_name_safe("table;DROP TABLE"));
    }

    #[test]
    fn test_table_name_safe_reject_dash() {
        assert!(!is_table_name_safe("table-name"));
    }

    #[test]
    fn test_table_name_safe_reject_space() {
        assert!(!is_table_name_safe("table name"));
    }

    #[test]
    fn test_archive_mode_requires_meta_provider() {
        let provider = std::sync::Arc::new(InMemoryMetaProvider::new());
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let interceptor = PersistPostInterceptor::new(provider, writer);
        // Just verify it can be created
        let _ = interceptor;
    }

    #[test]
    fn test_sync_mode_requires_meta_provider() {
        let provider = std::sync::Arc::new(InMemoryMetaProvider::new());
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let interceptor = PersistPostInterceptor::new(provider, writer);
        let _ = interceptor;
    }

    #[test]
    fn test_in_memory_writer_exists_check() {
        let writer = InMemoryTableWriter::new();
        assert!(!writer.exists("test_table", 1).unwrap());

        let mut data = HashMap::new();
        data.insert("id".to_string(), JsonValue::Number(1.0));
        writer.insert("test_table", &data).unwrap();

        assert!(writer.exists("test_table", 1).unwrap());
        assert!(!writer.exists("test_table", 2).unwrap());
    }

    #[test]
    fn test_meta_table_reader_read_by_id() {
        let reader = InMemoryMetaTableReader::new();
        let mut data = HashMap::new();
        data.insert("id".to_string(), JsonValue::Number(42.0));
        data.insert("name".to_string(), JsonValue::Str("test".to_string()));
        reader.load("test_table", vec![data]);

        let row = reader.read_by_id("test_table", 42).unwrap();
        assert!(row.is_some());
        assert_eq!(row.unwrap().get("name").and_then(|v| v.as_str()), Some("test"));

        let none = reader.read_by_id("test_table", 999).unwrap();
        assert!(none.is_none());
    }
}
