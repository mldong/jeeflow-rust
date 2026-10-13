# 业务数据入库（persist）

`jeeflow-persist` 提供**元数据驱动的业务数据动态入库**：流程发起 / 提交时把表单字段按
表元数据写入业务表（与五语言同语义——ARCHIVE 归档表 / SYNC 同步表两种 persist 模式）。

## 核心抽象

```rust
pub struct FieldMeta { column_name, column_type, display_name, nullable, permission, default }
pub struct TableMeta  { table_name, display_name, fields: Vec<FieldMeta> }

pub trait IDynamicMetaProvider: Send + Sync {
    fn table_meta(&self, table_name: &str) -> Option<TableMeta>;
}
pub struct InMemoryMetaProvider { /* register(TableMeta) / 查表 */ }

pub trait DynamicTableWriter: Send + Sync {
    fn write(&self, table_name: &str, row: &FlowData, id_gen: &dyn Fn() -> i64) -> Result<i64>;
}
```

- **`FieldMeta::permission`** 控制字段可见 / 可编辑（`is_visible` / `is_editable`），对齐
  mldong 框架字段权限码（L3 S9 金额字段审批只读即此机制）。
- **`is_table_name_safe`** 白名单校验表名（防 SQL 注入——动态表名进 DML，必须先过此校验）。
- 写入行 id 由注入的 `id_gen` 生成（雪花，与流程 id 同 ID 空间）。

## 两种 persist 模式（对齐五语言 L2-06 / L2-07）

| 模式 | 行为 | 验证点 |
|------|------|--------|
| **ARCHIVE**（归档） | 流程**结束**时整行落业务表（一次写全量字段） | `start_empty=True`（发起时表无行）→ 结束后行存在、字段齐 |
| **SYNC**（同步） | 发起 + 每次提交**增量同步**字段到业务表 | 发起即有行（`start_row=True`），后续提交覆盖对应字段 |

mldong-salvo 集成仓的 `src/modules/wf/core/wf_persist.rs` 是这两模式在 Salvo 栈的薄映射实现
（读 `wf_` 前缀字典路由决定表名 / 字段，写 `DynamicTableWriter`）。

## 元数据来源

- `InMemoryMetaProvider`：启动时 `register(TableMeta)`（演示 / 测试）。
- 生产集成：mldong 框架的表结构元数据服务（`/dev/schema/getByTableName`）经适配层桥接为
  `IDynamicMetaProvider`——见 [mldong-salvo 集成](./salvo.md)。

## 列匹配档位（spec/09「列匹配（1.6.4）」）

- **缺省宽松档**：表单键与表列按「转小写 + 去下划线」归一后相等即命中 ⇒ 驼峰表单键
  （`companyName`）能落到蛇形表列（`company_name`），**写入用表列原名**。尺子＝
  `normalize_column` / `column_names_equal` / `find_data_key`（与 java `findDataKey` 同一枚）。
- **严格档**：`PersistPostInterceptor::new(provider, writer).with_strict_columns(true)` ⇒
  只认忽略大小写的逐字相等（对应 java `setStrictColumnMatch(true)`）。
- 两个键归一到同一列时，胜者规则＝**逐字相等优先，其余按字典序首个命中**（rust 的 `HashMap`
  没有插入序，java／MoonBit 的「遍历即停」在这里不可复现，故本栈钉一条确定性的）。
- 未命中的两条静默面现在都留日志（`[jeeflow] WARN …`）：写了 `persistMode` 但不认识
  （`"ARCHVIE"` 这类拼错；首尾空白会先 trim，`"ARCHIVE "` 属正常档）、字段权限过滤／列匹配后
  **空集**（整单不落）。两者都不打断流程流转。
