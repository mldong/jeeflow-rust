//! Error types for jeeflow engine core.
//! Zero dependencies — uses custom enum, no thiserror/anyhow.

use std::fmt;

/// Engine error codes.
/// Business failure = 99999999 (spec/06 §2.1).
pub const ERR_BUSINESS: i64 = 99999999;

/// 内部错误对外只说这一句（issues/137 §3-1 · spec/06 §2.12，八栈同一串**逐字**，不许改措辞）。
/// 对应 java `JeeflowFacade.INTERNAL_FAILURE_MSG`；原文只进日志与错误链（[`JeeflowError::detail`]）。
pub const INTERNAL_FAILURE_MSG: &str = "流程处理失败";

#[derive(Debug, Clone)]
pub enum JeeflowError {
    /// Business logic failure (code=99999999)
    Business(String),
    /// Unknown action
    UnknownAction(String),
    /// Process definition not found
    DefineNotFound(i64),
    /// Process instance not found
    InstanceNotFound(i64),
    /// Task not found
    TaskNotFound(i64),
    /// Permission denied (operator not allowed)
    PermissionDenied(String),
    /// Invalid state transition
    InvalidState(String),
    /// Invalid submit type
    InvalidSubmitType(i64),
    /// Parse error (JSON / model).
    ///
    /// ⚠️ payload 就是**对外 msg 本体**，`message()` 逐字透出、不再套前缀（issues/139）：
    /// 解析类失败的 msg 一律是基准（jeeflow-java `ModelParser`）钉死的中文原文，
    /// 底层实现（JSON 解析器 / 驱动 / serde）的原文**严禁**拼进来——它只走
    /// [`std::error::Error::source`]，见 [`JeeflowError::ParseErrorWithCause`]。
    ParseError(String),
    /// 解析失败 ＋ 底层原文（issues/139）。
    ///
    /// 逐字对齐 java 的 `throw new RuntimeException("读取流程定义 JSON 失败", e)`：
    /// 第一个字段＝对外 msg（`message()` 只回它），第二个字段＝底层异常，只挂在
    /// [`std::error::Error::source`] 上（本仓 core 零依赖，用 std 的 `Box<dyn Error>` 复刻
    /// java 的 getCause 分工；`JeeflowError` 还得保持 `Clone`，故底层文本包成 [`ParseSource`]）。
    ParseErrorWithCause(String, Box<ParseSource>),
    /// Internal error
    ///
    /// ⚠️ payload 是**内部实现细节原文**（sqlx 驱动／运行时／集成方 provider 抛上来的文本），
    /// 属 issues/137 §3-1（spec/06 §2.12）判别式里的"外来文案"档：对外 msg 一律只给固定文案
    /// [`INTERNAL_FAILURE_MSG`]，原文只进日志与错误链（经 [`JeeflowError::detail`] 取回），
    /// **不得**拼进任何对外字段。
    Internal(String),
}

/// 底层实现抛出的原文载体（issues/139）：进错误链，不进对外 msg。
#[derive(Debug, Clone)]
pub struct ParseSource(pub String);

impl fmt::Display for ParseSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for ParseSource {}

impl JeeflowError {
    pub fn code(&self) -> i64 {
        ERR_BUSINESS
    }

    /// issues/139：解析类失败的唯一构造口——对外 msg 用基准逐字原文，底层原文只上错误链。
    ///
    /// `msg` 会**原样**成为门面出口的 `msg`（见 [`Self::message`]），调用方负责给它基准文案。
    pub fn parse_failure(msg: impl Into<String>, cause: impl Into<String>) -> Self {
        JeeflowError::ParseErrorWithCause(msg.into(), Box::new(ParseSource(cause.into())))
    }

    /// 对外 msg 本体（门面出口 `error_response(&e.message())` 逐字送出）。
    ///
    /// issues/137 §3-1（spec/06 §2.12）：`Internal` 档经 [`is_foreign_detail`] 收敛为固定文案
    /// [`INTERNAL_FAILURE_MSG`]——旧形状 `format!("内部错误: {}", msg)` 把驱动／运行时／provider
    /// 的内部原文带前缀外透，那一条腿整体挪到 [`Self::detail`]（日志侧），不再进出口。
    /// 其余变体都是引擎自己写的契约文案（八栈＋十三壳＋前端 toast 按原文逐字对齐），照旧逐字透出。
    pub fn message(&self) -> String {
        if is_foreign_detail(self) {
            return INTERNAL_FAILURE_MSG.to_string();
        }
        match self {
            JeeflowError::Business(msg) => msg.clone(),
            JeeflowError::UnknownAction(a) => format!("未知 action: {}", a),
            JeeflowError::DefineNotFound(id) => format!("流程定义不存在: {}", id),
            JeeflowError::InstanceNotFound(id) => format!("流程实例不存在: {}", id),
            JeeflowError::TaskNotFound(id) => format!("任务不存在: {}", id),
            JeeflowError::PermissionDenied(msg) => format!("权限不足: {}", msg),
            JeeflowError::InvalidState(msg) => format!("非法状态转换: {}", msg),
            JeeflowError::InvalidSubmitType(st) => format!("非法 submitType: {}", st),
            // issues/139：payload 即对外 msg，逐字透出（旧形状套 "解析错误: " 前缀，
            // 与本仓 Business 那一档同规则，也让"固定文案"断言对不上基准）。
            JeeflowError::ParseError(msg) => msg.clone(),
            JeeflowError::ParseErrorWithCause(msg, _) => msg.clone(),
            // 判别式已在函数头收敛这一档；此臂只为 match 穷尽，与固定文案同答案。
            JeeflowError::Internal(_) => INTERNAL_FAILURE_MSG.to_string(),
        }
    }

    /// 日志／错误链侧的全文（issues/137 §3-1「原文只进日志与错误对象」那一半）。
    ///
    /// - `Internal`：带原文（旧出口形状 `内部错误: {}` 原样挪到这里，排障信息不丢）；
    /// - `ParseErrorWithCause`：契约文案 ＋ 底层原文（`source()` 那一份的文本形态）；
    /// - 其余：与 [`Self::message`] 相同（本来就是引擎写的契约文案）。
    ///
    /// **不得**拼进对外 msg 或任何其它对外字段。
    pub fn detail(&self) -> String {
        match self {
            JeeflowError::Internal(msg) => format!("内部错误: {}", msg),
            JeeflowError::ParseErrorWithCause(msg, cause) => format!("{}（cause: {}）", msg, cause.0),
            other => other.message(),
        }
    }
}

/// issues/137 §3-1「谁写的这段文案」判别式（**纯函数**，对应 java
/// `JeeflowFacade.isForeignDetail`；文案判据与副作用〔日志〕各自可测）。
///
/// 返回 `true` ⇒ 属内部实现细节 ⇒ 出口只给固定文案 [`INTERNAL_FAILURE_MSG`]，
/// 原文只进日志与错误链（[`JeeflowError::detail`]）。
///
/// 本栈**不需要** java 那五条运行时异常类型族启发式：`JeeflowError` 的变体本身就承载了
/// "谁写的这段文案"——`Internal(_)` 是唯一的内部档（sqlx 驱动原文、std 解析原文、集成方
/// provider 原文全经这一变体进来），其余每个变体的 message 都是引擎自己写的契约文案
/// （`Business` 逐字透出；`ParseErrorWithCause` 的第一个字段是基准逐字文案、底层原文只挂
/// `source()`，issues/139 已收）。把判据收窄成"一律固定文案"会静默改写契约面，
/// 与 spec/06 §2.12「不能简单收窄」是同一条红线。
pub fn is_foreign_detail(err: &JeeflowError) -> bool {
    matches!(err, JeeflowError::Internal(_))
}

/// Display ＝ 对外 msg 视图（与 [`JeeflowError::message`] 同口径，issues/137 §3-1：
/// `Internal` 档只出固定文案）。日志/排障要原文请走 [`JeeflowError::detail`]。
impl fmt::Display for JeeflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message())
    }
}

impl std::error::Error for JeeflowError {
    /// issues/139：底层实现（JSON 解析器等）的原文只走这一条链，绝不进 [`Self::message`]。
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            JeeflowError::ParseErrorWithCause(_, cause) => Some(cause.as_ref()),
            _ => None,
        }
    }
}

/// 流程定义 JSON 解析失败的对外固定文案（逐字＝jeeflow-java `ModelParser`
/// `throw new RuntimeException("读取流程定义 JSON 失败", e)`，issues/139）。
pub const MSG_READ_PROCESS_DEFINE_JSON_FAILED: &str = "读取流程定义 JSON 失败";

pub type JeeflowResult<T> = Result<T, JeeflowError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_always_business() {
        assert_eq!(JeeflowError::Business("test".into()).code(), ERR_BUSINESS);
        assert_eq!(JeeflowError::UnknownAction("x".into()).code(), ERR_BUSINESS);
        assert_eq!(JeeflowError::DefineNotFound(1).code(), ERR_BUSINESS);
        assert_eq!(JeeflowError::TaskNotFound(1).code(), ERR_BUSINESS);
        assert_eq!(JeeflowError::PermissionDenied("x".into()).code(), ERR_BUSINESS);
        assert_eq!(JeeflowError::Internal("x".into()).code(), ERR_BUSINESS);
    }

    #[test]
    fn test_error_messages() {
        assert!(JeeflowError::Business("msg".into()).message().contains("msg"));
        assert!(JeeflowError::UnknownAction("test/act".into()).message().contains("test/act"));
        assert!(JeeflowError::DefineNotFound(42).message().contains("42"));
        assert!(JeeflowError::TaskNotFound(99).message().contains("99"));
    }

    #[test]
    fn test_error_display() {
        let err = JeeflowError::Business("test error".into());
        let s = format!("{}", err);
        assert_eq!(s, "test error");
    }

    #[test]
    fn test_error_business_code_constant() {
        assert_eq!(ERR_BUSINESS, 99999999);
    }

    #[test]
    fn test_invalid_submit_type_message() {
        let err = JeeflowError::InvalidSubmitType(99);
        assert!(err.message().contains("99"));
    }

    #[test]
    fn test_parse_error_message() {
        let err = JeeflowError::ParseError("bad json".into());
        assert!(err.message().contains("bad json"));
    }

    // ═══ issues/137 §3-1 · Internal 档出口固定文案 ＋ 判别式纯函数 ═══

    /// 固定文案钉死在字面量上（八栈同一串逐字，改措辞即违约）。
    #[test]
    fn test_i137_internal_failure_msg_is_verbatim_contract_text() {
        assert_eq!(INTERNAL_FAILURE_MSG, "流程处理失败");
    }

    /// 判别式矩阵：`Internal` 是唯一内部档；其余变体全是引擎自己写的契约文案，
    /// 一律不得被判成内部（判宽＝静默收窄契约面，spec/06 §2.12 红线）。
    #[test]
    fn test_i137_is_foreign_detail_matrix() {
        assert!(is_foreign_detail(&JeeflowError::Internal("x".into())));
        let contract_variants = [
            JeeflowError::Business("operator 必填".into()),
            JeeflowError::UnknownAction("a/b".into()),
            JeeflowError::DefineNotFound(1),
            JeeflowError::InstanceNotFound(1),
            JeeflowError::TaskNotFound(1),
            JeeflowError::PermissionDenied("x".into()),
            JeeflowError::InvalidState("x".into()),
            JeeflowError::InvalidSubmitType(9),
            JeeflowError::ParseError(MSG_READ_PROCESS_DEFINE_JSON_FAILED.into()),
            JeeflowError::parse_failure(MSG_READ_PROCESS_DEFINE_JSON_FAILED, "JSON parse error: …"),
        ];
        for e in contract_variants {
            assert!(!is_foreign_detail(&e), "契约档不得被判成内部：{:?}", e);
        }
    }

    /// 泄漏原文的各形状 ⇒ `message()`（＝门面出口 `error_response(&e.message())` 的输入）
    /// 必须**逐字**等于固定文案；Display 与 message() 同一口径。
    #[test]
    fn test_i137_internal_message_shapes_all_collapse_to_fixed_text() {
        let parse_int = "x".parse::<i64>().unwrap_err().to_string(); // std ParseIntError 真原文
        let shapes: Vec<String> = vec![
            "expected value at line 1 column 1".to_string(), // serde/JSON 解析器原文族
            "error communicating with server: Connection refused (os error 61)".to_string(), // sqlx 驱动族
            parse_int,                                       // invalid digit found in string
            "内部驱动细节 12345".to_string(),                  // 裸包装（message==cause 原文）族
            "third-party provider exploded: token=abc123".to_string(), // 集成方 provider 族
        ];
        for s in shapes {
            let err = JeeflowError::Internal(s.clone());
            assert_eq!(err.message(), INTERNAL_FAILURE_MSG, "出口固定文案：{}", s);
            assert_eq!(err.to_string(), INTERNAL_FAILURE_MSG, "Display 与 message() 同一口径：{}", s);
            assert!(!err.message().contains(&s), "原文不得进 msg：{}", s);
            assert!(!err.message().contains("内部错误"), "旧前缀不得复活：{}", s);
        }
    }

    /// 原文没丢：日志/错误链一侧（`detail()`）仍能取回全文——只断言 msg 的话，
    /// "把原文整个丢掉"的假修也能绿，这一格就是挡那个的。
    #[test]
    fn test_i137_internal_original_text_survives_in_detail() {
        let err = JeeflowError::Internal("mysql query: Unknown column 'x' in 'field list'".to_string());
        assert_eq!(err.message(), INTERNAL_FAILURE_MSG);
        let detail = err.detail();
        assert!(detail.contains("mysql query: Unknown column 'x'"),
            "原文必须能从日志侧取回：{}", detail);
        assert!(detail.starts_with("内部错误: "), "日志侧沿用旧形状前缀，排障口径不变：{}", detail);

        // ParseErrorWithCause：msg 是基准逐字文案，detail 附带底层原文（source() 同一份文本）
        let p = JeeflowError::parse_failure(MSG_READ_PROCESS_DEFINE_JSON_FAILED, "JSON parse error: Unexpected character");
        assert_eq!(p.message(), MSG_READ_PROCESS_DEFINE_JSON_FAILED);
        assert!(p.detail().contains("JSON parse error: Unexpected character"), "{}", p.detail());
        assert!(std::error::Error::source(&p).is_some(), "错误链那一份不动（issues/139）");
    }

    /// 判据收窄成"一律固定文案"时这一组必须红：引擎自己写的中文契约文案仍逐字透出。
    #[test]
    fn test_i137_contract_texts_still_verbatim() {
        for s in [
            "operator 必填",
            "任务不存在",
            "任务非进行中，不可摘除参与人",
            "至少需保留一名参与人",
            "读取流程定义 JSON 失败",
            "流程实例非进行中，无法撤回",
            "无权限撤回该流程实例",
            "无权限转办该任务",
            "原办理人不是该任务参与人",
            "目标人已是该任务参与人",
            "任务非进行中，不可转办",
            "上一步任务ID为空，无法驳回至上一步处理",
            "processTaskId/actorIds 缺失",
        ] {
            assert_eq!(JeeflowError::Business(s.to_string()).message(), s, "Business 逐字透出");
        }
        // 非 Business 的契约变体同样是引擎写的文案，不被判别式波及
        assert_eq!(JeeflowError::TaskNotFound(424242).message(), "任务不存在: 424242");
        assert_eq!(JeeflowError::UnknownAction("x/y".into()).message(), "未知 action: x/y");
        assert_eq!(
            JeeflowError::parse_failure(MSG_READ_PROCESS_DEFINE_JSON_FAILED, "raw").message(),
            "读取流程定义 JSON 失败"
        );
    }
}
