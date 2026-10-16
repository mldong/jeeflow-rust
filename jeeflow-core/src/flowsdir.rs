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
///
/// 失败一律响亮 panic（audit8 R81）：本模块此前把 `create_dir_all`／`fs::copy`／`remove_file`
/// 的 `Result` 全用 `let _ =` 丢掉 ⇒ 镜像没成也当成功，测试拿到旧件／缺件继续跑（假绿）。
/// 写腿同时改原子形（`<目标>.tmp.<pid>` + rename），理由与 node／python／php 三栈同一档。
fn mirror(root: &PathBuf) {
    let src = root
        .join("../jeeflow-java/jeeflow-core/src/test/resources/flows")
        .canonicalize();
    let src = match src {
        Ok(p) if p.is_dir() => p,
        _ => return, // 用户单仓 / 容器：无 Java 源，跳过镜像
    };
    let dst = root.join("flows");
    if let Err(e) = fs::create_dir_all(&dst) {
        panic!("flowsdir: 建不出镜像目录 {}：{}", dst.display(), e);
    }
    let mut src_names = HashSet::new();
    let rd = match fs::read_dir(&src) {
        Ok(rd) => rd,
        Err(e) => panic!("flowsdir: 读不到 Java 源目录 {}：{}", src.display(), e),
    };
    for e in rd.flatten() {
        let name = e.file_name();
        let s = name.to_string_lossy().to_string();
        if !s.ends_with(".json") {
            continue;
        }
        let data = read_retry(&e.path());
        write_atomic(&dst.join(&s), &data);
        src_names.insert(s);
    }
    // 孤儿清理：本仓有、Java 源已无的 .json（防 id 错位）。临时件名不以 .json 结尾，不在这条腿上。
    if let Ok(rd) = fs::read_dir(&dst) {
        for e in rd.flatten() {
            let name = e.file_name();
            let s = name.to_string_lossy().to_string();
            if s.ends_with(".json") && !src_names.contains(&s) {
                if let Err(err) = fs::remove_file(e.path()) {
                    panic!("flowsdir: 删不掉孤儿镜像件 {}：{}", e.path().display(), err);
                }
            }
        }
    }
}

/// 写腿重试预算（挡到用尽就响亮失败）。10ms × 300 = 3s。
const RETRY: usize = 300;
const RETRY_MS: u64 = 10;

/// 稳态零写 + 原子替换：内容逐字节相同就不动盘；真要写就写 `<目标>.tmp.<pid>` 再 rename，
/// 读者只会看到旧件或新件。Windows 上读者持句柄时 rename 会被挡 ⇒ 有界重试，用尽则
/// 清掉自己的临时件后 panic（不静默丢夹具）。
fn write_atomic(to: &PathBuf, data: &[u8]) {
    if fs::read(to).is_ok_and(|b| b == data) {
        return;
    }
    let tmp = PathBuf::from(format!("{}.tmp.{}", to.display(), std::process::id()));
    for attempt in 0..=RETRY {
        if let Err(e) = fs::write(&tmp, data) {
            let _ = fs::remove_file(&tmp);
            panic!("flowsdir: 临时件写入失败 {}：{}", tmp.display(), e);
        }
        match fs::rename(&tmp, to) {
            Ok(()) => return,
            Err(e) => {
                if attempt == RETRY {
                    let _ = fs::remove_file(&tmp);
                    panic!(
                        "flowsdir: 镜像 {} 失败（重试 {} 次）：{}",
                        to.display(),
                        RETRY,
                        e
                    );
                }
                std::thread::sleep(std::time::Duration::from_millis(RETRY_MS));
            }
        }
    }
}

/// 读源件：撞共享冲突（别的进程正在换这份文件）时短重试，用尽仍读不到才响亮失败。
fn read_retry(path: &PathBuf) -> Vec<u8> {
    for attempt in 0..=RETRY {
        match fs::read(path) {
            Ok(d) => return d,
            Err(e) => {
                if attempt == RETRY {
                    panic!("flowsdir: 读不到源件 {}：{}", path.display(), e);
                }
                std::thread::sleep(std::time::Duration::from_millis(RETRY_MS));
            }
        }
    }
    unreachable!()
}

#[cfg(test)]
mod flowsdir_r81_tests {
    //! audit8 R81（issues/187 的 rust 同形档）——镜像写腿的原子形与"失败不许静默"。
    //!
    //! 旧形状＝`fs::copy` 就地非原子写 ＋ `let _ =` 把 `create_dir_all`／`copy`／`remove_file`
    //! 的 `Result` 全丢掉 ⇒ 镜像没成也当成功（测试拿到旧件／缺件继续跑＝假绿）。
    //! 本栈已有 `Once`（整进程一次）＋内容相同跳过 ⇒ 缺的只有"原子形"与"响亮失败"两件事，
    //! 故不立 node／python 那种多进程 storm 档（`Once` 已经消掉了同进程内的并发窗口，
    //! 案面「范围」列那条"撞不上就按低档记账、别照抄别栈严重度"在此兑现）。
    //!
    //! 两档各打一件事：
    //! ①镜像语义＝全量复制＋删孤儿＋**稳态零写**（把目标 mtime 钉到过去，再镜像两次必须纹丝不动）
    //!   ＋源变更真落盘＋无 Java 源静默；
    //! ②结构档＝目标名被目录占位 ⇒ `rename` 必失败 ⇒ 必须响亮 panic，且抛出前清掉自己的临时件。
    //!   这一档同时是坏形状探测器：旧 `let _ = fs::copy(...)` 在同一场景一声不响。

    use super::*;
    use std::fs::File;
    use std::io::Write;
    use std::time::{Duration, UNIX_EPOCH};

    fn body(ch: char, n: usize) -> String {
        format!("{{\"id\":1,\"pad\":\"{}\"}}", ch.to_string().repeat(n))
    }

    fn scratch(tag: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "r81-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.subsec_nanos())
                .unwrap_or(0)
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(p.join("proj/flows")).unwrap();
        fs::create_dir_all(java_flows_of(&p)).unwrap();
        p
    }

    /// mirror() 认的布局：`<root>/../jeeflow-java/.../flows` ⇒ 假源放 <tmp>/jeeflow-java/...，仓放 <tmp>/proj。
    fn java_flows_of(tmp: &PathBuf) -> PathBuf {
        tmp.join("jeeflow-java/jeeflow-core/src/test/resources/flows")
    }

    fn put(path: &PathBuf, text: &str) {
        let mut f = File::create(path).unwrap();
        f.write_all(text.as_bytes()).unwrap();
    }

    fn tmps(dir: &PathBuf) -> Vec<String> {
        fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|f| f.contains(".tmp."))
            .collect()
    }

    #[test]
    fn r81_镜像语义含稳态零写() {
        let tmp = scratch("sem");
        let java = java_flows_of(&tmp);
        let proj = tmp.join("proj");
        let dst = proj.join("flows");
        put(&java.join("01-a.json"), &body('a', 64));
        put(&dst.join("orphan.json"), "{\"id\":99}");

        mirror(&proj);
        assert_eq!(
            fs::read_to_string(dst.join("01-a.json")).unwrap(),
            body('a', 64),
            "Java 源应整份镜像"
        );
        assert!(!dst.join("orphan.json").exists(), "孤儿 .json 应被删");

        // 稳态零写：把 mtime 钉到过去再镜像两次——真被重写就一定不等于它（rename 会把临时件的新 mtime 带进来）
        let target = dst.join("01-a.json");
        File::options().write(true).open(&target)
            .unwrap()
            .set_modified(UNIX_EPOCH + Duration::from_secs(1_000_000_000))
            .unwrap();
        let pinned = fs::metadata(&target).unwrap().modified().unwrap();
        mirror(&proj);
        mirror(&proj);
        assert_eq!(
            fs::metadata(&target).unwrap().modified().unwrap(),
            pinned,
            "内容相同不得重写"
        );
        assert_eq!(tmps(&dst), Vec::<String>::new(), "不该留下临时件");

        // 源变更要落盘
        put(&java.join("01-a.json"), &body('b', 64));
        mirror(&proj);
        assert_eq!(fs::read_to_string(&target).unwrap(), body('b', 64), "源变更应镜像落盘");

        // 无 Java 源（用户单仓／容器）：静默跳过，不写不炸
        let bare = tmp.join("deep/bare");
        fs::create_dir_all(bare.join("flows")).unwrap();
        put(&bare.join("flows/keep.json"), "{\"id\":7}");
        mirror(&bare);
        assert_eq!(fs::read_to_string(bare.join("flows/keep.json")).unwrap(), "{\"id\":7}");

        fs::remove_dir_all(&tmp).ok();
    }

    #[test]
    fn r81_镜像失败必须响亮抛出不留临时件() {
        let tmp = scratch("fail");
        let java = java_flows_of(&tmp);
        let proj = tmp.join("proj");
        let dst = proj.join("flows");
        put(&java.join("01-a.json"), &body('c', 64));
        // 目标名被目录占位 ⇒ rename 必失败（旧形状 `let _ = fs::copy` 在这里一声不响）
        fs::create_dir_all(dst.join("01-a.json")).unwrap();

        let panicked = std::panic::catch_unwind(|| mirror(&proj)).is_err();
        assert!(panicked, "镜像失败必须响亮抛出，不能静默丢夹具");
        assert_eq!(tmps(&dst), Vec::<String>::new(), "抛出前应清掉自己的临时件");

        fs::remove_dir_all(&tmp).ok();
    }
}
