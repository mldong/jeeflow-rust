//! 本仓 flows/ 流程定义目录解析（维护者与用户统一入口）。
//!
//! 唯一编辑源是 jeeflow-java 仓的 test/resources/flows/。本仓（jeeflow-rust）flows/ 是其副本，
//! 入库 commit（单语言用户下载即用，不依赖隔壁 Java 仓）。
//!
//! `dir()` 的语义：
//!   1. 环境变量 JEEFLOW_FLOWS_DIR 显式覆盖（Docker/CI）
//!   2. 否则以 workspace 根（jeeflow-core 的上级目录）为基准，返回 `<root>/flows`
//!   3. 若根的兄弟目录里有 Java 源（维护者机器）→ 精确镜像进本仓 flows/
//!      （拷贝所有 .json + 删除本仓多出的孤儿 .json，防 id 按文件名排序错位）
//!   4. 始终返回本仓 flows/ 路径 —— 所有读取点只读这里，Java 仓不再被直接读取
//!
//! 镜像用 `Once` 守护整进程只执行一次：测试并行跑时多个线程共享这一把锁串行完成镜像，
//! 其余线程直接读已就位的本仓 flows/，避免"一边写一边读"撞文件锁。
//!
//! 编译门控：`#[cfg(any(test, feature = "dev-flows"))]`。engine 测试走 cfg(test)；
//! demo-salvo 通过 `features = ["dev-flows"]` 使用；发布 crate 两者皆关，本模块编译不到。

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Once;

/// 镜像整进程只执行一次（并行测试共享，避免读写并发撞文件锁）。
static MIRROR_ONCE: Once = Once::new();

/// 返回本仓 flows/ 绝对路径；维护者机器上会先把 Java 源精确镜像进来。
/// 解析顺序：JEEFLOW_FLOWS_DIR → workspace 根/flows → 容器约定 /app/flows。
pub fn dir() -> PathBuf {
    if let Ok(d) = std::env::var("JEFFLOW_FLOWS_DIR") {
        if !d.is_empty() {
            return PathBuf::from(d);
        }
    }
    let root = root();
    MIRROR_ONCE.call_once(|| mirror(&root)); // 有 Java 源则镜像进本地 flows/，无则 no-op
    let local = root.join("flows");
    if has_json(&local) {
        return local;
    }
    // Docker 镜像约定挂载/打包路径
    const DOCKER: &str = "/app/flows";
    let docker = PathBuf::from(DOCKER);
    if has_json(&docker) {
        return docker;
    }
    local
}

fn has_json(dir: &PathBuf) -> bool {
    fs::read_dir(dir)
        .map(|rd| rd.flatten().any(|e| e.file_name().to_string_lossy().ends_with(".json")))
        .unwrap_or(false)
}

/// workspace 根 = jeeflow-core crate 的上级目录（flows/ 在仓根）。
fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_default()
}

/// 若 Java 源存在则精确镜像到本仓 flows/（拷所有 + 删孤儿），不存在则原样返回。
/// 幂等：内容相同的文件跳过写，git 工作区保持干净。
fn mirror(root: &PathBuf) {
    let src = root
        .join("../jeeflow-java/jeeflow-core/src/test/resources/flows")
        .canonicalize();
    let src = match src {
        Ok(p) if p.is_dir() => p,
        _ => return, // 用户单仓 / 容器：无 Java 源，跳过镜像
    };
    let dst = root.join("flows");
    let _ = fs::create_dir_all(&dst);
    let mut src_names = HashSet::new();
    if let Ok(rd) = fs::read_dir(&src) {
        for e in rd.flatten() {
            let name = e.file_name();
            let s = name.to_string_lossy().to_string();
            if !s.ends_with(".json") {
                continue;
            }
            let to = dst.join(&s);
            // 内容相同则跳过写（幂等 + 避免无谓 mtime 变更）
            let same = fs::read(e.path()).is_ok_and(|a| fs::read(&to).is_ok_and(|b| a == b));
            if !same {
                let _ = fs::copy(e.path(), &to);
            }
            src_names.insert(s);
        }
    }
    // 孤儿清理：本仓有、Java 源已无的 .json（防 id 错位）
    if let Ok(rd) = fs::read_dir(&dst) {
        for e in rd.flatten() {
            let name = e.file_name();
            let s = name.to_string_lossy().to_string();
            if s.ends_with(".json") && !src_names.contains(&s) {
                let _ = fs::remove_file(e.path());
            }
        }
    }
}
