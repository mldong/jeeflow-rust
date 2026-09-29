//! Error types for jeeflow engine core.
//! Zero dependencies — uses custom enum, no thiserror/anyhow.

use std::fmt;

/// Engine error codes.
/// Business failure = 99999999 (spec/06 §2.1).
pub const ERR_BUSINESS: i64 = 99999999;

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

    pub fn message(&self) -> String {
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
            JeeflowError::Internal(msg) => format!("内部错误: {}", msg),
        }
    }
}

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
}
