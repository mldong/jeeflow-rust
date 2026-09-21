//! Service context — immutable registry for SPI lookup.
//! Equivalent to Java ServiceContext / Go Context.
//! Uses Arc<dyn Trait> for thread-safe shared access.

use crate::spi::*;
use std::collections::HashMap;
use std::sync::Arc;

/// Service context — holds all SPI implementations.
/// Built once at startup, read-only at runtime.
#[derive(Clone)]
pub struct ServiceContext {
    /// By-name registry for arbitrary services.
    services: HashMap<String, Arc<dyn std::any::Any + Send + Sync>>,
    /// Typed service accessors.
    pub repository: Option<Arc<dyn ProcessRepository>>,
    pub ext_repository: Option<Arc<dyn ProcessExtRepository>>,
    pub user_provider: Option<Arc<dyn UserProvider>>,
    pub org_user_provider: Option<Arc<dyn OrgUserProvider>>,
    pub user_search_provider: Option<Arc<dyn UserSearchProvider>>,
    pub json_provider: Option<Arc<dyn JsonProvider>>,
    pub expression_evaluator: Option<Arc<dyn ExpressionEvaluator>>,
    pub transaction_template: Option<Arc<dyn TransactionTemplate>>,
    pub id_generator: Option<Arc<dyn IdGenerator>>,
    pub action_permission_provider: Option<Arc<dyn ActionPermissionProvider>>,
    pub biz_data_reader: Option<Arc<dyn BizDataReader>>,
    /// Assignment handlers by registration name (Java FQCN).
    pub assignment_handlers: HashMap<String, Arc<dyn AssignmentHandler>>,
    /// Decision handlers by name.
    pub decision_handlers: HashMap<String, Arc<dyn DecisionHandler>>,
    /// Flow interceptors (sorted by order).
    pub interceptors: Vec<Arc<dyn FlowInterceptor>>,
    /// Event listeners.
    pub event_listeners: Vec<Arc<dyn ProcessEventListener>>,
    /// 委托代理自动生效开关（issues/116 批次 D，**默认开启**）。
    ///
    /// 开启时引擎在新任务落库前把生效中的被委托人并入参与者集合
    /// （见 [`crate::surrogate::apply_surrogate_to_task`]，契约 06 §4.5 运行期语义）。
    /// 关闭一行：`ServiceContext::new().with_surrogate_auto_apply(false)`
    /// ——关闭后 `processSurrogate/*` 回到"仅台账"语义（配了委托也不会追加到任务参与者）。
    /// 依赖 `ext_repository`：未配置扩展仓储时本来就静默跳过（条款 4）。
    pub surrogate_auto_apply: bool,
}

impl ServiceContext {
    pub fn new() -> Self {
        ServiceContext {
            services: HashMap::new(),
            repository: None,
            ext_repository: None,
            user_provider: None,
            org_user_provider: None,
            user_search_provider: None,
            json_provider: None,
            expression_evaluator: None,
            transaction_template: None,
            id_generator: None,
            action_permission_provider: None,
            biz_data_reader: None,
            assignment_handlers: HashMap::new(),
            decision_handlers: HashMap::new(),
            interceptors: Vec::new(),
            event_listeners: Vec::new(),
            // 委托自动生效默认开启（issues/116 批次 D）——集成方零配置即生效，
            // 对齐内置版 mldong-wf `SurrogateInterceptor` 标 @Component 的"白拿"体验。
            surrogate_auto_apply: true,
        }
    }

    pub fn with_repository(mut self, repo: Arc<dyn ProcessRepository>) -> Self {
        self.repository = Some(repo);
        self
    }

    pub fn with_ext_repository(mut self, repo: Arc<dyn ProcessExtRepository>) -> Self {
        self.ext_repository = Some(repo);
        self
    }

    /// 开/关**委托代理自动生效**（issues/116 批次 D；契约 06 §4.5 条款 3）。
    ///
    /// 引擎内置该行为且**默认开启**，集成方零配置即生效；本方法是显式关闭的入口
    /// （`with_surrogate_auto_apply(false)`），关闭后回到"仅台账"行为。
    /// 等价关闭姿势：装配一个 `get_surrogate` 恒返回 `None` 的扩展仓储。
    pub fn with_surrogate_auto_apply(mut self, on: bool) -> Self {
        self.surrogate_auto_apply = on;
        self
    }

    pub fn with_user_provider(mut self, up: Arc<dyn UserProvider>) -> Self {
        self.user_provider = Some(up);
        self
    }

    pub fn with_org_user_provider(mut self, oup: Arc<dyn OrgUserProvider>) -> Self {
        self.org_user_provider = Some(oup);
        self
    }

    pub fn with_user_search_provider(mut self, usp: Arc<dyn UserSearchProvider>) -> Self {
        self.user_search_provider = Some(usp);
        self
    }

    pub fn with_id_generator(mut self, gen: Arc<dyn IdGenerator>) -> Self {
        self.id_generator = Some(gen);
        self
    }

    pub fn with_expression_evaluator(mut self, eval: Arc<dyn ExpressionEvaluator>) -> Self {
        self.expression_evaluator = Some(eval);
        self
    }

    pub fn with_transaction_template(mut self, tx: Arc<dyn TransactionTemplate>) -> Self {
        self.transaction_template = Some(tx);
        self
    }

    pub fn with_action_permission_provider(mut self, app: Arc<dyn ActionPermissionProvider>) -> Self {
        self.action_permission_provider = Some(app);
        self
    }

    pub fn with_biz_data_reader(mut self, reader: Arc<dyn BizDataReader>) -> Self {
        self.biz_data_reader = Some(reader);
        self
    }

    pub fn register_assignment_handler(&mut self, name: &str, handler: Arc<dyn AssignmentHandler>) {
        self.assignment_handlers.insert(name.to_string(), handler);
    }

    pub fn register_decision_handler(&mut self, name: &str, handler: Arc<dyn DecisionHandler>) {
        self.decision_handlers.insert(name.to_string(), handler);
    }

    pub fn register_interceptor(&mut self, interceptor: Arc<dyn FlowInterceptor>) {
        self.interceptors.push(interceptor);
        self.interceptors.sort_by_key(|i| i.order());
    }

    pub fn register_event_listener(&mut self, listener: Arc<dyn ProcessEventListener>) {
        self.event_listeners.push(listener);
    }

    pub fn put(&mut self, name: &str, service: Arc<dyn std::any::Any + Send + Sync>) {
        self.services.insert(name.to_string(), service);
    }

    pub fn find_by_name(&self, name: &str) -> Option<&Arc<dyn std::any::Any + Send + Sync>> {
        self.services.get(name)
    }

    pub fn find_assignment_handler(&self, name: &str) -> Option<&Arc<dyn AssignmentHandler>> {
        self.assignment_handlers.get(name)
    }

    pub fn get_repository(&self) -> &Arc<dyn ProcessRepository> {
        self.repository.as_ref().expect("ProcessRepository not registered")
    }

    pub fn get_id_generator(&self) -> Arc<dyn IdGenerator> {
        self.id_generator.clone().expect("IdGenerator not registered")
    }
}

impl Default for ServiceContext {
    fn default() -> Self {
        Self::new()
    }
}
