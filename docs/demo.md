# 演示站（jeeflow-demo-salvo）

Rust 引擎的 Salvo 演示站，与 Java（:8080）/ Go（:8081）/ Python（:8100）/ Node（:8082）/
PHP 演示站同款形态：**统一前端 jeeflow-ui + 42 action `/wf/**` 转发**。

## 本地运行

```bash
git clone https://github.com/mldong/jeeflow-rust
cd jeeflow-rust
cargo run -p jeeflow-demo-salvo
# 打开 http://localhost:8091
```

## 路由

| 路由 | 方法 | 说明 |
|------|------|------|
| `/healthz` | GET | 健康检查 `{status: UP, service: jeeflow-demo-salvo}` |
| `/api/stats` | GET | `todoCount` / `instanceCount` |
| `/api/reset` | POST | 重置全部数据 + 重载共享流程 |
| `/wf/{action}` | POST | 42 action 统一入口（`JeeflowFacade::flow`） |

内存仓储（`MemoryRepository`），8 个具名用户（与五语言 demo 同款：张三 / 孙倩 / 周明 /
吴婷 等），登录态由 demo 侧模拟（无 mldong 框架 RBAC）。

## 线上演示

- **开源演示站** [jeeflow-demo.mldong.com](https://jeeflow-demo.mldong.com) 右上角可切换
  Rust 后端（六语言后端常驻：Java / Go / Python / Node.js / PHP / Rust）
- tag `v*.*.*` 触发 `demo-deploy.yml`：构建 `jeeflow-rust-demo` 镜像 → 部署演示站，
  与引擎发版（`release.yml` 推 crates.io）分离

## 快速验证

```bash
curl http://127.0.0.1:8091/healthz
curl -X POST http://127.0.0.1:8091/wf/userLogin -H 'Content-Type: application/json' \
  -d '{"userName":"superAdmin","password":"123456"}'
```

## 生产部署

生产集成走 [mldong-salvo 集成](./salvo.md)（框架级：RBAC / 持久化 / vben5 前端），
demo 站仅用于引擎能力演示与联调。
