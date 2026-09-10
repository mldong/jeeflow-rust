# jeeflow-rust 文档

> jeeflow 引擎的 **Rust 实现**——对齐 Java 参考实现的行为语义（五语言联邦第六个成员）。本文档面向 Rust 开发者，内容也聚合到[文档站语言指南](../../)。

## 引擎定位

- **crate 拆分**：`jeeflow-core`（引擎核心，零第三方依赖）/ `jeeflow-repository-sqlx`（MySQL 仓储 + schema）/ `jeeflow-persist`（业务数据动态入库）/ `jeeflow-facade`（42-action 统一门面，对齐 mldong 框架接口）
- **发布通道**：crates.io（tag `v*.*.*` 触发 CI 按拓扑序发布四 crate）
- **当前版本**：1.0.5（2026-08-25 crates.io 首发）

## SDK 集成

| 文档 | 内容 |
|------|------|
| [快速开始（SDK 集成）](./getting-started.md) | crates.io 安装、最小示例 |
| [引擎 API](./engine-api.md) | 引擎接口与核心方法、状态码 |
| [流程定义格式](./flow-definition.md) | LogicFlow JSON 结构、节点类型 |
| [SPI 扩展指南](./spi-guide.md) | `ProcessRepository` / `UserProvider` 等 Rust trait |
| [业务数据入库（persist）](./persist.md) | `DynamicTableWriter` / `IDynamicMetaProvider` |

## 框架集成与演示

| 文档 | 内容 |
|------|------|
| [mldong-salvo 集成](./salvo.md) | mldong 框架 Rust 栈（Salvo + SeaORM）薄映射接入，40+ action 全覆盖 |
| [演示站（Demo）](./demo.md) | 启动 jeeflow-demo-salvo（:8091）、快速验证 |
