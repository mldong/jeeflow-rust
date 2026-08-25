# mldong-salvo 集成

[mldong-salvo](https://gitee.com/mldong/mldong)（Salvo 0.76 + SeaORM 1.1，Rust 快速开发框架）的
jeeflow 薄映射集成，是 jeeflow **第六个框架集成栈**（前五：boot2/boot3/boot4 × Java、goframe、
nestjs、fastapi、laravel——八栈含 Rust 引擎自身 demo）。

## 集成仓

`mldong-salvo-jeeflow` @ `feature/jeeflow`，**基础仓 + 1 commit** 模式：

- 基础仓 `mldong-salvo` @ master：框架层 jeeflow 前置（vben5 引导接口 `/menu/all` +
  `/badgeConfig`、框架枚举字典层 11 个 `sys_*` 枚举、`/sys/user/select` LabelValueVO、
  雪花 EPOCH 对齐 1288834974657）
- 集成 commit：`src/modules/wf/` 全量薄映射层 + `/wf/` 免权限白名单 + `wf_*` 字典路由 +
  crates.io 引擎依赖（`jeeflow-* = "1.0.5"`）+ Docker 部署（Docker 链已下沉基础仓，两仓 blob 一致）

自检：`git rev-list --count master..feature/jeeflow` = 1。

## 薄映射层结构

```
src/modules/wf/
├── controller/wf_controller.rs     # 唯一 /wf/** 入口：action 分发转发 JeeflowFacade
└── core/
    ├── wf_factory.rs               # JeeflowFacade 单例装配（ServiceContext + 全部 SPI）
    ├── wf_db.rs                    # sqlx MySqlPool（SeaORM 共享连接池）
    ├── wf_user_provider.rs         # IUserProvider → UserProvider（sys_user 表）
    ├── wf_user_search_provider.rs  # 用户搜索（候选/转办/加签下拉）
    ├── wf_org_user_provider.rs     # 组织取人（部门/岗位/角色成员 → OrgUserProvider）
    ├── wf_permission.rs            # 权限码校验（wf:{action 的 / → :} 动态映射）
    ├── wf_persist.rs               # ARCHIVE/SYNC 业务数据入库薄映射
    ├── wf_dict_service.rs          # wf_* 流程字典（EnumDictRegistry + HandlerRegistry 清单）
    └── wf_db / wf_factory …        # 装配 + 生命周期
```

**42 个 action 全覆盖**：controller 层不做业务，全部转发 `JeeflowFacade::flow(action, args)`
（与 boot4 集成同款"单 controller 转发"形态，boot2/boot3 的多 controller 是历史结构）。

## 权限码规律

`wf:{action 路径把 / 换 :}`——如 `processDesign/save` → `wf:processDesign:save`（
`wf_permission.rs::permission_codes` 动态映射；另有 OR 规则表 / NO_PERM_ACTIONS 登录即可表）。
超管（superAdmin）放行；普通用户按 mldong 框架 RBAC 校验（NO_PERM_ACTIONS 内只读放行、写操作按角色授权）。
L2-08 契约用例验证 save 拒 / detail 放行。

## 免权限白名单（NO_PERM_PATHS）

login-only 下拉 / 字典 / 菜单接口对齐 goframe `IgnoreAuthList`：
`/menu/all`、`/badgeConfig`、`/sys/dict/getByDictType`、`/sys/{user,role,dept,post,dict,dictItem,config}/select`、
`/sys/menu/appList`（基础仓 consts）+ `/wf/**` 中仅登录态接口（集成层）。
`/wf/` 业务接口**全部**需 token（L0-03 契约：未登录调 wf 返回 99990403）。

## vben5 前端兼容

前端 `mldong-vben5` @ `feature/wf`（与其余七栈**共用同一前端**，零改动）：

- `GET /menu/all` 用户路由菜单树（app_code=platform，type IN (1,2)，树构建含兄弟环修复）
- `/sys/user/select` 返回 goframe LabelValueVO `{label, value, ext}`（id 字符串防 JS 精度丢失）
- 框架枚举字典（yes_no / sex / sys_menu_type 等 11 个无 sys_dict 行的枚举，wire 格式
  `{label, value}` 对齐 goframe）——vben ApiComponent / 流程设计器下拉的数据源

## 部署与验收

- 一键部署：`mldong-website/public/deploy/mldong-salvo-jeeflow/`（:28080 前端 / :28100 API /
  :28406 MySQL / :28579 Redis；compose 后端 image 参数化 `${BACKEND_IMAGE:-ACR 生产 tag}`，
  需 `seccomp:unconfined`——160 老 runc 拦新 glibc 系统调用）
- 镜像构建：160 容器内 cargo 编译（base `mldong/rust:1.97.1`，引擎走 crates.io，
  镜像与发布产物严格一致），通道 B `bash scripts/build-jeeflow-image.sh salvo`
- 验收（2026-08-25，镜像 #12）：L0–L2 契约 19/19 + L3 端到端 14 过 / 1 固定 skip（S12）/ 0 败
