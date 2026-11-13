use super::test_store::MemoryAccountingStore;
use super::*;
use crate::{
    cache::{CacheTrait, in_memory::InMemoryCache, keys::CUSTOM_MODEL_COSTS_CACHE_KEY},
    db::spans::{Span, SpanType},
    language_model::costs::{ModelCosts, ModelInfo},
    traces::spans::SpanAttributes,
};
use chrono::Utc;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

fn message() -> RabbitMqSpanMessage {
    RabbitMqSpanMessage {
        span: Span {
            project_id: Uuid::from_u128(1),
            trace_id: Uuid::from_u128(2),
            span_id: Uuid::from_u128(3),
            parent_span_id: None,
            name: "model".into(),
            span_type: SpanType::LLM,
            attributes: SpanAttributes::new(
                serde_json::from_value(json!({
                    "gen_ai.system": "openai", "gen_ai.request.model": "receipt-test-model",
                    "gen_ai.usage.input_tokens": 10, "gen_ai.usage.output_tokens": 3,
                }))
                .unwrap(),
            ),
            input: None,
            output: None,
            start_time: Utc::now(),
            end_time: Utc::now(),
            events: vec![],
            status: None,
            tags: None,
            size_bytes: 0,
        },
        pre_processed: true,
        input_dedup: None,
        output_dedup: None,
        tool_dedup: None,
        agent_prompt: None,
    }
}

struct PricingHarness {
    db: Arc<DB>,
    cache: Arc<Cache>,
    store: MemoryAccountingStore,
}

impl PricingHarness {
    async fn new() -> Self {
        let db = Arc::new(DB {
            pool: PgPool::connect_lazy("postgres://127.0.0.1:1/unused").unwrap(),
        });
        // An unexpected pricing read fails immediately, rather than timing out.
        db.pool.close().await;
        Self {
            db,
            cache: Arc::new(Cache::InMemory(InMemoryCache::new(Some(100)))),
            store: MemoryAccountingStore::default(),
        }
    }

    async fn price(&self, rate: Option<f64>) {
        let msg = message();
        let custom_key = format!(
            "{CUSTOM_MODEL_COSTS_CACHE_KEY}:{}:openai:receipt-test-model",
            msg.span.project_id
        );
        self.cache
            .insert::<Option<ModelCosts>>(&custom_key, None)
            .await
            .unwrap();
        let key = ModelInfo::extract("receipt-test-model", Some("openai")).cache_key();
        self.cache
            .insert(
                &key,
                rate.map(|rate| {
                    ModelCosts(json!({
                        "input_cost_per_token": rate, "output_cost_per_token": rate,
                    }))
                }),
            )
            .await
            .unwrap();
    }

    async fn account(
        &self,
        messages: Vec<RabbitMqSpanMessage>,
    ) -> Result<Vec<AccountedMessage>, HandlerError> {
        account_span_messages(messages, &self.db, &self.cache, &self.store).await
    }
}

#[tokio::test]
async fn accepted_pricing_survives_rate_changes_and_unavailable_lookup() {
    let mut h = PricingHarness::new().await;
    h.price(Some(0.25)).await;
    let source = message();
    let first = h.account(vec![source.clone()]).await.unwrap();
    assert_eq!(first[0].usage.total_tokens, 13);
    assert_eq!(first[0].usage.total_cost, 3.25);
    assert_eq!(first[0].usage.pricing_basis["kind"], "calculated");

    h.price(Some(0.5)).await;
    let mut another = source.clone();
    another.span.span_id = Uuid::from_u128(4);
    let next = h.account(vec![source.clone(), another]).await.unwrap();
    assert_eq!(next[0].usage, first[0].usage);
    assert_eq!(
        next[1].usage.total_cost, 6.5,
        "a new operation uses current pricing"
    );

    h.cache = Arc::new(Cache::InMemory(InMemoryCache::new(Some(100))));
    let replay = h.account(vec![source]).await.unwrap();
    assert_eq!(replay[0].usage, first[0].usage);
    assert_eq!(
        *h.store.writes.lock().unwrap(),
        2,
        "ordinary replays do not rewrite receipts"
    );
}

#[tokio::test]
async fn lost_receipt_ack_reuses_committed_fact_without_repricing() {
    let mut h = PricingHarness::new().await;
    h.price(Some(0.25)).await;
    *h.store.fail_after_accept.lock().unwrap() = true;
    let source = message();
    let error = h.account(vec![source.clone()]).await.err().unwrap();
    assert!(error.should_requeue());
    assert_eq!(h.store.receipts.lock().unwrap().len(), 1);
    h.cache = Arc::new(Cache::InMemory(InMemoryCache::new(Some(100))));
    let replay = h.account(vec![source]).await.unwrap();
    assert_eq!(replay[0].usage.total_cost, 3.25);
    assert_eq!(*h.store.writes.lock().unwrap(), 1);
}

#[tokio::test]
async fn pricing_failure_is_retryable_and_does_not_freeze_zero() {
    let h = PricingHarness::new().await;
    let error = h.account(vec![message()]).await.err().unwrap();
    assert!(error.should_requeue());
    assert!(h.store.receipts.lock().unwrap().is_empty());
    h.price(Some(0.25)).await;
    assert_eq!(
        h.account(vec![message()]).await.unwrap()[0]
            .usage
            .total_cost,
        3.25
    );
}

#[tokio::test]
async fn supplied_zero_and_missing_rates_have_distinct_provenance() {
    let h = PricingHarness::new().await;
    let mut supplied = message();
    supplied
        .span
        .attributes
        .raw_attributes
        .insert("gen_ai.usage.total_cost".into(), json!(0));
    let accepted = h.account(vec![supplied]).await.unwrap();
    assert_eq!(accepted[0].usage.total_cost, 0.0);
    assert_eq!(accepted[0].usage.pricing_basis["kind"], "supplied");

    h.price(None).await;
    let mut missing = message();
    missing.span.span_id = Uuid::from_u128(4);
    let accepted = h.account(vec![missing.clone()]).await.unwrap();
    assert_eq!(accepted[0].usage.pricing_basis["kind"], "unpriced");
    h.price(Some(0.25)).await;
    assert_eq!(
        h.account(vec![missing]).await.unwrap()[0].usage,
        accepted[0].usage
    );
}

#[tokio::test]
async fn conflicting_deliveries_do_not_replace_usage_or_cross_scopes() {
    let h = PricingHarness::new().await;
    h.price(Some(0.25)).await;
    let source = message();
    let mut conflict = source.clone();
    conflict
        .span
        .attributes
        .raw_attributes
        .insert("gen_ai.usage.input_tokens".into(), json!(100));
    let first = h
        .account(vec![source.clone(), conflict.clone()])
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].usage.total_tokens, 13);
    assert!(
        h.store
            .receipts
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .conflict_detected
    );
    let reordered = h
        .account(vec![conflict.clone(), source.clone()])
        .await
        .unwrap();
    assert_eq!(reordered.len(), 1);
    assert_eq!(reordered[0].usage, first[0].usage);

    let mut other_trace = conflict.clone();
    other_trace.span.trace_id = Uuid::from_u128(9);
    let mut other_project = conflict;
    other_project.span.project_id = Uuid::from_u128(8);
    // Explicit costs avoid sharing any project-specific pricing cache entry.
    other_project
        .span
        .attributes
        .raw_attributes
        .insert("gen_ai.usage.total_cost".into(), json!(25.75));
    let accepted = h.account(vec![other_trace, other_project]).await.unwrap();
    assert_eq!(accepted.len(), 2);
    assert!(accepted.iter().all(|m| m.usage.total_tokens == 103));
    assert_eq!(h.store.receipts.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn cross_batch_conflict_is_durable_and_normal_retry_is_still_allowed() {
    let h = PricingHarness::new().await;
    h.price(Some(0.25)).await;
    let source = message();
    h.account(vec![source.clone()]).await.unwrap();
    let mut conflict = source.clone();
    conflict
        .span
        .attributes
        .raw_attributes
        .insert("gen_ai.request.service_tier".into(), json!("priority"));
    assert!(h.account(vec![conflict]).await.unwrap().is_empty());
    assert!(
        h.store
            .receipts
            .lock()
            .unwrap()
            .values()
            .next()
            .unwrap()
            .conflict_detected
    );
    assert_eq!(
        h.account(vec![source]).await.unwrap()[0].usage.total_tokens,
        13
    );
    assert_eq!(*h.store.writes.lock().unwrap(), 2);
}
