//! jeeflow-persist: DynamicTableWriter trait + PersistPostInterceptor.
//! Implements ARCHIVE/SYNC persist modes (spec/09).
//! Metadata-driven dynamic table writing with field permission filtering.

use jeeflow_core::json::{JsonValue, FlowData};
use jeeflow_core::spi::FlowInterceptor;
use jeeflow_core::engine::Execution;
use jeeflow_core::error::{JeeflowError, JeeflowResult};
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
    /// Filter columns based on field permissions.
    /// Returns only the columns that should be written (editable fields).
    fn filter_columns(&self, meta: &TableMeta, data: &FlowData) -> HashMap<String, JsonValue> {
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

    /// Insert a row into the dynamic table.
    fn insert(&self, table_name: &str, data: &HashMap<String, JsonValue>) -> JeeflowResult<i64>;

    /// Update a row in the dynamic table by ID.
    fn update(&self, table_name: &str, id: i64, data: &HashMap<String, JsonValue>) -> JeeflowResult<()>;

    /// Check if a row exists by ID.
    fn exists(&self, table_name: &str, id: i64) -> JeeflowResult<bool>;

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

/// Post-interceptor that persists form data to dynamic tables.
/// ARCHIVE mode: writes once at process start.
/// SYNC mode: writes on every task completion.
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

    /// Execute persist logic for ARCHIVE mode.
    pub fn persist_archive(&self, table_name: &str, data: &FlowData, operator: &str) -> JeeflowResult<i64> {
        if !is_table_name_safe(table_name) {
            return Err(JeeflowError::Business(format!("非法表名: {}", table_name)));
        }

        let meta = self.meta_provider.get_table_meta(table_name)?
            .ok_or_else(|| JeeflowError::Business(format!("表元数据不存在: {}", table_name)))?;

        // Filter columns by permission
        let filtered = self.table_writer.filter_columns(&meta, data);

        if filtered.is_empty() {
            return Ok(0); // Nothing to persist
        }

        // Fill system fields
        let mut row_data = filtered;
        self.table_writer.fill_system_fields(&mut row_data, operator, true);

        // Insert
        self.table_writer.insert(table_name, &row_data)
    }

    /// Execute persist logic for SYNC mode.
    pub fn persist_sync(&self, table_name: &str, id: i64, data: &FlowData, operator: &str) -> JeeflowResult<()> {
        if !is_table_name_safe(table_name) {
            return Err(JeeflowError::Business(format!("非法表名: {}", table_name)));
        }

        let meta = self.meta_provider.get_table_meta(table_name)?
            .ok_or_else(|| JeeflowError::Business(format!("表元数据不存在: {}", table_name)))?;

        // Filter columns by permission
        let filtered = self.table_writer.filter_columns(&meta, data);

        if filtered.is_empty() {
            return Ok(()); // Nothing to persist
        }

        // Fill system fields
        let mut row_data = filtered;
        self.table_writer.fill_system_fields(&mut row_data, operator, false);

        // Check if exists, then insert or update
        if self.table_writer.exists(table_name, id)? {
            self.table_writer.update(table_name, id, &row_data)
        } else {
            self.table_writer.insert(table_name, &row_data)?;
            Ok(())
        }
    }
}

impl FlowInterceptor for PersistPostInterceptor {
    fn intercept(&self, execution: &mut Execution) -> JeeflowResult<()> {
        // Determine persist mode from process model
        let persist_mode = execution.process_model.persist_mode.as_deref();
        let table_name = execution.process_model.rel_table_name.as_deref();

        if persist_mode.is_none() || table_name.is_none() {
            return Ok(()); // No persist configured
        }

        let mode = PersistMode::from_str(persist_mode.unwrap());
        let table = table_name.unwrap();

        match mode {
            Some(PersistMode::Archive) => {
                // ARCHIVE: only persist on first task (apply node)
                let is_first = execution.process_task.is_none();
                if is_first {
                    let id = self.persist_archive(table, &execution.args, &execution.operator)?;
                    if id > 0 {
                        execution.args.insert_i64("_persist_id", id);
                    }
                }
            }
            Some(PersistMode::Sync) => {
                // SYNC: persist on every task completion
                let persist_id = execution.args.get_i64("_persist_id").unwrap_or(0);
                self.persist_sync(table, persist_id, &execution.args, &execution.operator)?;
            }
            None => {} // No persist mode
        }

        Ok(())
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
// Tests
// ═══════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

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

    // ─── Column filtering tests ───

    #[test]
    fn test_filter_columns() {
        let writer = InMemoryTableWriter::new();
        let meta = TableMeta::new("oa_leave", "请假表")
            .with_field(FieldMeta::new("f_name", "VARCHAR(100)", "姓名").with_permission(2))
            .with_field(FieldMeta::new("f_days", "INT", "天数").with_permission(2))
            .with_field(FieldMeta::new("f_status", "VARCHAR(20)", "状态").with_permission(1)); // readonly

        let mut data = FlowData::new();
        data.insert_str("f_name", "张三");
        data.insert_i64("f_days", 3);
        data.insert_str("f_status", "pending"); // should be filtered out (readonly)
        data.insert_str("f_extra", "extra"); // should be filtered out (not in meta)

        let filtered = writer.filter_columns(&meta, &data);
        assert_eq!(filtered.len(), 2); // only f_name and f_days (editable)
        assert!(filtered.contains_key("f_name"));
        assert!(filtered.contains_key("f_days"));
        assert!(!filtered.contains_key("f_status")); // readonly
        assert!(!filtered.contains_key("f_extra")); // not in meta
    }

    // ─── ARCHIVE mode tests ───

    #[test]
    fn test_archive_mode_insert() {
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let mut provider = InMemoryMetaProvider::new();
        provider.register(
            TableMeta::new("oa_leave", "请假表")
                .with_field(FieldMeta::new("f_name", "VARCHAR(100)", "姓名"))
                .with_field(FieldMeta::new("f_days", "INT", "天数"))
        );
        let meta_provider = std::sync::Arc::new(provider);

        let interceptor = PersistPostInterceptor::new(meta_provider, writer.clone());

        let mut data = FlowData::new();
        data.insert_str("f_name", "张三");
        data.insert_i64("f_days", 5);

        let id = interceptor.persist_archive("oa_leave", &data, "user1").unwrap();
        assert!(id > 0);

        // Verify the row was inserted
        let row = writer.get_row("oa_leave", id).unwrap();
        assert_eq!(row.get("f_name").and_then(|v| v.as_str()), Some("张三"));
        assert!(row.contains_key("create_time")); // system field filled
        assert!(row.contains_key("create_user"));
    }

    #[test]
    fn test_archive_mode_reject_unsafe_table() {
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let provider = InMemoryMetaProvider::new();
        let meta_provider = std::sync::Arc::new(provider);
        let interceptor = PersistPostInterceptor::new(meta_provider, writer);

        let data = FlowData::new();
        let result = interceptor.persist_archive("sys_user", &data, "user1");
        assert!(result.is_err());
        assert!(result.unwrap_err().message().contains("非法表名"));
    }

    // ─── SYNC mode tests ───

    #[test]
    fn test_sync_mode_update() {
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let mut provider = InMemoryMetaProvider::new();
        provider.register(
            TableMeta::new("oa_leave", "请假表")
                .with_field(FieldMeta::new("f_name", "VARCHAR(100)", "姓名"))
                .with_field(FieldMeta::new("f_days", "INT", "天数"))
        );
        let meta_provider = std::sync::Arc::new(provider);

        let interceptor = PersistPostInterceptor::new(meta_provider, writer.clone());

        // First insert
        let mut data = FlowData::new();
        data.insert_str("f_name", "张三");
        data.insert_i64("f_days", 3);
        let id = interceptor.persist_archive("oa_leave", &data, "user1").unwrap();

        // Then sync update
        let mut update_data = FlowData::new();
        update_data.insert_i64("f_days", 5);
        interceptor.persist_sync("oa_leave", id, &update_data, "user2").unwrap();

        // Verify updated
        let row = writer.get_row("oa_leave", id).unwrap();
        assert_eq!(row.get("f_days").and_then(|v| v.as_i64()), Some(5));
        assert!(row.contains_key("update_time")); // system field filled
    }

    #[test]
    fn test_sync_mode_field_permissions() {
        let writer = std::sync::Arc::new(InMemoryTableWriter::new());
        let mut provider = InMemoryMetaProvider::new();
        provider.register(
            TableMeta::new("oa_leave", "请假表")
                .with_field(FieldMeta::new("f_name", "VARCHAR(100)", "姓名").with_permission(1)) // readonly
                .with_field(FieldMeta::new("f_days", "INT", "天数").with_permission(2)) // editable
                .with_field(FieldMeta::new("f_secret", "VARCHAR(50)", "秘密").with_permission(3)) // hidden
        );
        let meta_provider = std::sync::Arc::new(provider);

        let interceptor = PersistPostInterceptor::new(meta_provider, writer.clone());

        let mut data = FlowData::new();
        data.insert_str("f_name", "张三"); // readonly — should not be written
        data.insert_i64("f_days", 3); // editable — should be written
        data.insert_str("f_secret", "hidden"); // hidden — should not be written

        let id = interceptor.persist_archive("oa_leave", &data, "user1").unwrap();

        let row = writer.get_row("oa_leave", id).unwrap();
        assert!(!row.contains_key("f_name")); // readonly filtered
        assert!(row.contains_key("f_days")); // editable persisted
        assert!(!row.contains_key("f_secret")); // hidden filtered
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
