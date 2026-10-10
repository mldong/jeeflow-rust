//! 引擎内置默认表达式求值器（issues/158）。
//!
//! **为什么存在**：rust 的运行时一直有内置求值（旧 `engine.rs::simple_eval`），宿主不注册
//! `IExpressionEvaluator` 时决策支照样能定方向；而门面的 `eval_decision_expr` 在 SPI 为 `None`
//! 时整档判 false。同一个实例于是"运行时走了那条支、门面说没走"——活栈读数＝salvo 生产镜像
//! 在跨栈门禁 `L2-39` 上 47/1，唯一红就是档③ 缺 `e_dec_yes`。csharp 同夹具 48/0 PASS，
//! 差别只在它把内置默认做成了 SPI（`DefaultExpressionEvaluator` ＋ `ExpressionEvaluatorOrDefault`）
//! 让**两条腿共用一个出口**。本模块即 rust 侧的同形补齐。
//!
//! **语义基准**＝jeeflow-csharp `DefaultExpressionEvaluator`（其自身以 PHP `WfExpressionEvaluator`
//! 为最小基准不超集）：变量代入（`${var}` / `#var` / 裸名）＋ 比较运算 ＋ 布尔字面量。
//! 不含 `&&` / `||`——rust 旧内置没有、csharp 基准也没有，不在此扩集。
//!
//! **相对旧 `simple_eval` 的两处刻意收紧**（旧形状是蒙方向，不是求值）：
//! 1. 裸变量名**代入**：旧实现只认 `${var}`/`#var`，`amount > 1000` 里的 `amount` 原样留着，
//!    靠 `"amount".cmp("1000")`＝Greater 得出 true——同一条比较对 `amount <= 1000` 也返回
//!    `Greater != Less`＝true，两支一起"为真"，只是运行时会先撞上列表里第一条 true 边才显得没错。
//! 2. 关系运算（`>` `>=` `<` `<=`）**只在两侧都是数字时判**，否则 false：非数字操作数的字典序
//!    没有业务含义，用它定分支就是把"字母排在数字后面"当流程语义。
//!
//! 空表达式判 false 是**保持 rust 运行时既有形状**（两腿调用点都先判 `is_empty` 才求值，
//! 该档实际不可达；PHP 那边空串返 true，此处不跟，跟了会改运行时那条腿）。

use crate::json::JsonValue;
use crate::spi::ExpressionEvaluator;
use std::collections::HashMap;

/// 引擎内置默认求值器的 SPI 形状（无状态，故可作静态默认件挂在 `ServiceContext` 上）。
pub struct DefaultExpressionEvaluator;

/// 归一后的操作数（`#var` 缺省按 PHP 同形回落 0，裸名/`${}` 缺省＝Null）。
#[derive(Debug, Clone, PartialEq)]
enum Value {
    Num(f64),
    Str(String),
    Bool(bool),
    Null,
}

impl Value {
    fn from_json(v: &JsonValue) -> Value {
        match v {
            JsonValue::Number(n) => Value::Num(*n),
            JsonValue::Bool(b) => Value::Bool(*b),
            JsonValue::Str(s) => Value::Str(s.clone()),
            JsonValue::Null => Value::Null,
            // 复合值不参与比较（流程表达式不该拿数组/对象当操作数；这里不依赖 JsonValue 的 Display）
            JsonValue::Array(_) | JsonValue::Object(_) => Value::Null,
        }
    }

    fn as_num(&self) -> Option<f64> {
        match self {
            Value::Num(n) => Some(*n),
            _ => None,
        }
    }

    /// 单操作数的真值档（无比较运算符时走这档，形状同旧运行时的兜底判据）
    fn truthy(&self) -> bool {
        match self {
            Value::Num(n) => *n != 0.0,
            Value::Bool(b) => *b,
            Value::Null => false,
            Value::Str(s) => !s.is_empty() && s != "false" && s != "0",
        }
    }
}

/// 引擎内置求值：表达式 → 布尔。运行时与门面共用这一个出口（issues/158 的同源要求）。
pub fn evaluate_builtin(expression: &str, context: &HashMap<String, JsonValue>) -> bool {
    let expr = expression.trim();
    if expr.is_empty() {
        return false;
    }
    // 比较运算符：两支的顺序不能换——两位必须先于其一位前缀被试（`<=` 先于 `<`），
    // 否则 `amount <= 1000` 会被拆成 `amount` / `1000` 之外的错形状。
    for op in [">=", "<=", "!=", "==", ">", "<"] {
        if let Some(pos) = expr.find(op) {
            let left = resolve_token(&expr[..pos].trim(), context);
            let right = resolve_token(&expr[pos + op.len()..].trim(), context);
            return compare(&left, &right, op);
        }
    }
    // 无运算符：单个变量 / 布尔字面量
    resolve_token(expr, context).truthy()
}

/// 单侧词元解析（基准＝PHP `resolveValue`：`${}` 剥壳 → 引号字面量 → 数字 → 布尔/null →
/// `#名`（缺省 0）→ 流程变量（缺省视为未定义））
fn resolve_token(token: &str, context: &HashMap<String, JsonValue>) -> Value {
    let t = token.trim();
    let name = if let Some(inner) = t
        .strip_prefix("${")
        .and_then(|s| s.strip_suffix('}'))
    {
        inner.trim()
    } else {
        t
    };

    // 带引号的字符串字面量
    if (name.starts_with('\'') && name.ends_with('\'') && name.len() >= 2)
        || (name.starts_with('"') && name.ends_with('"') && name.len() >= 2)
    {
        return Value::Str(name[1..name.len() - 1].to_string());
    }
    if let Ok(n) = name.parse::<f64>() {
        return Value::Num(n);
    }
    if name.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }
    if name.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }
    if name.eq_ignore_ascii_case("null") {
        return Value::Null;
    }
    if let Some(raw) = name.strip_prefix('#') {
        // 会签门控变量（`#nrOfCompletedInstances` 等）：缺省回落 0，与 PHP 同形
        return match raw.parse::<f64>() {
            Ok(n) => Value::Num(n),
            Err(_) => context
                .get(raw)
                .map(Value::from_json)
                .unwrap_or(Value::Num(0.0)),
        };
    }
    // 裸变量名：查得到才代入（旧形状这里根本不代入，于是比较的是变量名本身的字典序）
    context.get(name).map(Value::from_json).unwrap_or(Value::Null)
}

/// 比较档：数值优先；`==` / `!=` 允许同型字符串与布尔等值；关系运算要求两侧都是数字。
fn compare(left: &Value, right: &Value, op: &str) -> bool {
    if let (Some(l), Some(r)) = (left.as_num(), right.as_num()) {
        return match op {
            ">" => l > r,
            ">=" => l >= r,
            "<" => l < r,
            "<=" => l <= r,
            "==" => l == r,
            "!=" => l != r,
            _ => false,
        };
    }
    match op {
        "==" => same_value(left, right),
        "!=" => !same_value(left, right),
        // 非数字操作数不参与定序：字典序不是流程语义（见模块头的收紧说明）
        _ => false,
    }
}

fn same_value(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Num(l), Value::Num(r)) => l == r,
        (Value::Str(l), Value::Str(r)) => l == r,
        (Value::Bool(l), Value::Bool(r)) => l == r,
        (Value::Null, Value::Null) => true,
        _ => false,
    }
}

/// SPI 形状（与宿主注册的求值器同接口，故门面/运行时只需换一个出口）。
impl ExpressionEvaluator for DefaultExpressionEvaluator {
    fn eval(
        &self,
        expression: &str,
        context: &HashMap<String, JsonValue>,
    ) -> crate::error::JeeflowResult<JsonValue> {
        Ok(JsonValue::Bool(evaluate_builtin(expression, context)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars() -> HashMap<String, JsonValue> {
        let mut m = HashMap::new();
        m.insert("amount".into(), JsonValue::Number(5000.0));
        m.insert("days".into(), JsonValue::Number(3.0));
        m.insert("status".into(), JsonValue::Str("approved".into()));
        m.insert("nrOfCompletedInstances".into(), JsonValue::Number(2.0));
        m
    }

    #[test]
    fn i158_builtin_is_numeric_not_lexicographic() {
        let v = vars();
        // 这两条正是旧 simple_eval 蒙错的方向：不代入裸变量时两条都为 true
        assert!(evaluate_builtin("amount > 1000", &v));
        assert!(!evaluate_builtin("amount <= 1000", &v));
        assert!(evaluate_builtin("amount >= 5000", &v));
        assert!(!evaluate_builtin("amount < 1000", &v));
        assert!(evaluate_builtin("${amount} > 1000", &v));
        assert!(evaluate_builtin("days == 3", &v));
        assert!(!evaluate_builtin("days != 3", &v));
        assert!(evaluate_builtin("#nrOfCompletedInstances == 2", &v));
        assert!(!evaluate_builtin("#nrOfCompletedInstances > 3", &v));
        assert!(evaluate_builtin("status == 'approved'", &v));
        assert!(!evaluate_builtin("status == 'rejected'", &v));
        assert!(evaluate_builtin("true", &v));
        assert!(!evaluate_builtin("false", &v));
        // 未定义变量：关系运算判 false（不得因字典序自等）
        assert!(!evaluate_builtin("missing_var > 1000", &v));
        assert!(!evaluate_builtin("missing_var == 1000", &v));
        // #var 缺省回落 0（PHP 同形）
        assert!(evaluate_builtin("#nrOfActiveInstances == 0", &v));
    }
}
