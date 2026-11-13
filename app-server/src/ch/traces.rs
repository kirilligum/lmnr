use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{traces_agg::CHTraceAgentVersion, utils::merge_json_objects};
use crate::db::spans::{Span, SpanType};
use crate::traces::spans::SpanUsage;

/// One immutable operation's accounting fact. The ClickHouse Map value is this
/// tuple in field order; replay merges by span id before any counters are summed.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SpanAccounting {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub cache_read_input_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub reasoning_tokens: u64,
    pub input_cost: f64,
    pub output_cost: f64,
    pub total_cost: f64,
}

impl SpanAccounting {
    fn from_span(span: &Span, usage: &SpanUsage) -> Self {
        if !span.is_llm_span() {
            return Self::default();
        }
        Self {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            total_tokens: usage.total_tokens,
            cache_read_input_tokens: usage.cache_read_input_tokens as u64,
            cache_creation_input_tokens: usage.cache_creation_input_tokens as u64,
            reasoning_tokens: usage.reasoning_tokens as u64,
            input_cost: usage.input_cost,
            output_cost: usage.output_cost,
            total_cost: usage.total_cost,
        }
    }
}

#[derive(Eq, Hash, PartialEq)]
struct TraceKey {
    project_id: Uuid,
    trace_id: Uuid,
}

/// One batch's trace-stats delta. Folded by `traces_agg` (aggregates) and
/// `traces_static` (set-once columns); never a cumulative row.
#[derive(Debug, Clone)]
pub struct TraceAggregation {
    pub trace_id: Uuid,
    pub project_id: Uuid,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_tokens: i64,
    pub cache_read_input_tokens: i64,
    pub cache_creation_input_tokens: i64,
    pub reasoning_tokens: i64,
    pub input_cost: f64,
    pub output_cost: f64,
    pub total_cost: f64,
    pub session_id: Option<String>,
    pub user_id: Option<String>,
    pub status: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub tags: HashSet<String>,
    pub num_spans: i32,
    pub top_span_id: Option<Uuid>,
    pub top_span_name: Option<String>,
    pub top_span_type: u8,
    pub trace_type: u8,
    pub has_browser_session: Option<bool>,
    pub span_names: HashSet<String>,
    pub release: Option<String>,
    /// Versions of the trace's decided releases, tagged at ingest.
    pub agent_versions: Vec<CHTraceAgentVersion>,
    pub span_accounting: BTreeMap<String, SpanAccounting>,
}

impl TraceAggregation {
    /// Aggregate statistics from a batch of Spans and SpanUsage grouped by trace_id
    pub fn from_spans(spans: &[Span], span_usage_vec: &[SpanUsage]) -> Vec<Self> {
        use std::collections::HashMap;

        let mut trace_aggregations: HashMap<TraceKey, TraceAggregation> = HashMap::new();

        for (span, span_usage) in spans.iter().zip(span_usage_vec.iter()) {
            let entry = trace_aggregations
                .entry(TraceKey {
                    project_id: span.project_id,
                    trace_id: span.trace_id,
                })
                .or_insert_with(|| TraceAggregation {
                    trace_id: span.trace_id,
                    project_id: span.project_id,
                    start_time: None,
                    end_time: None,
                    input_tokens: 0,
                    output_tokens: 0,
                    total_tokens: 0,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                    reasoning_tokens: 0,
                    input_cost: 0.0,
                    output_cost: 0.0,
                    total_cost: 0.0,
                    session_id: None,
                    user_id: None,
                    status: None,
                    metadata: None,
                    tags: HashSet::new(),
                    num_spans: 0,
                    top_span_id: None,
                    top_span_name: None,
                    top_span_type: 0,
                    trace_type: 0,
                    has_browser_session: None,
                    span_names: HashSet::new(),
                    release: None,
                    agent_versions: Vec::new(),
                    span_accounting: BTreeMap::new(),
                });

            entry
                .span_accounting
                .entry(span.span_id.simple().to_string())
                .or_insert_with(|| SpanAccounting::from_span(span, span_usage));

            // Aggregate min start_time
            entry.start_time = Some(match entry.start_time {
                Some(existing) => existing.min(span.start_time),
                None => span.start_time,
            });

            // Aggregate max end_time
            entry.end_time = Some(match entry.end_time {
                Some(existing) => existing.max(span.end_time),
                None => span.end_time,
            });

            // Use "any" strategy for these fields (take first non-empty value)
            if entry.session_id.is_none() {
                if let Some(session_id) = span.attributes.session_id() {
                    if !session_id.is_empty() {
                        entry.session_id = Some(session_id);
                    }
                }
            }
            if entry.user_id.is_none() {
                if let Some(user_id) = span.attributes.user_id() {
                    if !user_id.is_empty() {
                        entry.user_id = Some(user_id);
                    }
                }
            }
            if entry.release.is_none() {
                entry.release = span.attributes.release();
            }
            if let Some(status) = &span.status {
                if status == "error" {
                    entry.status = Some("error".to_string());
                } else if entry.status.is_none() && !status.is_empty() {
                    entry.status = Some(status.clone());
                }
            }
            if let Some(metadata) = span.attributes.metadata() {
                if let Ok(metadata_value) = serde_json::to_value(&metadata) {
                    entry.metadata = Some(match entry.metadata.take() {
                        Some(existing) => merge_json_objects(existing, metadata_value),
                        None => metadata_value,
                    });
                }
            }
            if entry.trace_type == 0 {
                if let Some(trace_type) = span.attributes.trace_type() {
                    entry.trace_type = trace_type.clone().into();
                }
            }

            if span.span_type == SpanType::Evaluation {
                entry.trace_type = 1;
            }

            if span.parent_span_id.is_none() {
                entry.top_span_id = Some(span.span_id);
                entry.top_span_name = Some(span.name.clone());
                entry.top_span_type = span.span_type.clone().into();
            }

            // An explicit trace name wins over the root span's name, which emitters
            // that send one (OpenRouter Broadcast) leave generic.
            if let Some(trace_name) = span.attributes.trace_name() {
                entry.top_span_name = Some(trace_name);
            }

            if entry.top_span_name.is_none() {
                let path = span.attributes.path().unwrap_or_default();
                path.first()
                    .map(|name| entry.top_span_name = Some(name.clone()));
            }

            // Collect unique tags
            for tag in span.attributes.tags() {
                entry.tags.insert(tag);
            }

            // Collect unique span names
            entry.span_names.insert(span.name.clone());

            if entry.has_browser_session.is_none() {
                if let Some(has_browser_session) = span.attributes.has_browser_session() {
                    entry.has_browser_session = Some(has_browser_session);
                }
            }
        }

        // Consumers of the in-memory aggregate must see the same unique
        // operations as the persistent accounting map, even if a queue batch
        // contains both an original delivery and its retry.
        for aggregate in trace_aggregations.values_mut() {
            aggregate.num_spans = aggregate.span_accounting.len() as i32;
            for fact in aggregate.span_accounting.values() {
                aggregate.input_tokens += fact.input_tokens;
                aggregate.output_tokens += fact.output_tokens;
                aggregate.total_tokens += fact.total_tokens;
                aggregate.cache_read_input_tokens += fact.cache_read_input_tokens as i64;
                aggregate.cache_creation_input_tokens += fact.cache_creation_input_tokens as i64;
                aggregate.reasoning_tokens += fact.reasoning_tokens as i64;
                aggregate.input_cost += fact.input_cost;
                aggregate.output_cost += fact.output_cost;
                aggregate.total_cost += fact.total_cost;
            }
        }
        trace_aggregations.into_values().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use serde_json::json;

    use super::*;
    use crate::traces::{span_attributes::SPAN_RELEASE, spans::SpanAttributes};

    fn make_span(trace_id: Uuid, span_type: SpanType, output_tokens: i64) -> Span {
        Span {
            span_id: Uuid::new_v4(),
            project_id: Uuid::from_u128(1),
            trace_id,
            parent_span_id: None,
            name: "test".to_string(),
            attributes: SpanAttributes::new(HashMap::from([(
                "gen_ai.usage.output_tokens".to_string(),
                json!(output_tokens),
            )])),
            input: None,
            output: None,
            span_type,
            start_time: Utc::now(),
            end_time: Utc::now(),
            events: vec![],
            status: None,
            tags: None,
            size_bytes: 0,
        }
    }

    fn make_usage(output_tokens: i64, total_cost: f64) -> SpanUsage {
        SpanUsage {
            input_tokens: 0,
            output_tokens,
            total_tokens: output_tokens,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            reasoning_tokens: 0,
            input_cost: 0.0,
            output_cost: total_cost,
            total_cost,
            request_model: None,
            response_model: None,
            provider_name: None,
            pricing_basis: serde_json::Value::Null,
        }
    }

    // A Default span carrying `gen_ai.usage.*` must NOT contribute to the trace token/cost
    // totals — only LLM spans do (LAM-1873).
    #[test]
    fn only_llm_spans_contribute_to_trace_usage() {
        let trace_id = Uuid::new_v4();
        let spans = vec![
            make_span(trace_id, SpanType::LLM, 100),
            make_span(trace_id, SpanType::Default, 50),
        ];
        let usage = vec![make_usage(100, 1.5), make_usage(50, 0.5)];

        let aggregations = TraceAggregation::from_spans(&spans, &usage);

        assert_eq!(aggregations.len(), 1);
        let agg = &aggregations[0];
        assert_eq!(agg.output_tokens, 100);
        assert_eq!(agg.total_tokens, 100);
        assert_eq!(agg.total_cost, 1.5);
        assert_eq!(agg.num_spans, 2);
    }

    #[test]
    fn explicit_trace_name_overrides_root_span_name() {
        let trace_id = Uuid::new_v4();
        let mut root = make_span(trace_id, SpanType::LLM, 100);
        root.name = "LLM Generation".to_string();
        root.attributes
            .raw_attributes
            .insert("trace.name".to_string(), json!("my-agent-run"));

        let aggregations = TraceAggregation::from_spans(&[root], &[make_usage(100, 1.5)]);

        assert_eq!(
            aggregations[0].top_span_name,
            Some("my-agent-run".to_string())
        );
        // The span tree itself is untouched.
        assert!(aggregations[0].span_names.contains("LLM Generation"));
    }

    #[test]
    fn first_release_in_the_batch_wins() {
        let trace_id = Uuid::new_v4();
        let mut spans = vec![
            make_span(trace_id, SpanType::Default, 0),
            make_span(trace_id, SpanType::Default, 0),
            make_span(trace_id, SpanType::Default, 0),
        ];
        spans[1]
            .attributes
            .raw_attributes
            .insert(SPAN_RELEASE.to_string(), json!("abc1234"));
        spans[2]
            .attributes
            .raw_attributes
            .insert(SPAN_RELEASE.to_string(), json!("def5678"));
        let usage = vec![make_usage(0, 0.0), make_usage(0, 0.0), make_usage(0, 0.0)];

        let aggregations = TraceAggregation::from_spans(&spans, &usage);

        assert_eq!(aggregations[0].release, Some("abc1234".to_string()));
    }

    #[test]
    fn accounting_identity_survives_replay_and_rebatching() {
        let trace = Uuid::new_v4();
        let model = make_span(trace, SpanType::LLM, 13);
        let tool = make_span(trace, SpanType::Tool, 100);
        let usage = [make_usage(13, 0.25), make_usage(100, 10.0)];
        let combined = TraceAggregation::from_spans(&[model.clone(), tool.clone()], &usage);
        let reversed = TraceAggregation::from_spans(
            &[tool.clone(), model.clone()],
            &[make_usage(100, 10.0), make_usage(13, 0.25)],
        );
        assert_eq!(combined[0].span_accounting, reversed[0].span_accounting);
        let mut split = TraceAggregation::from_spans(std::slice::from_ref(&model), &usage[..1])
            .remove(0)
            .span_accounting;
        split.extend(
            TraceAggregation::from_spans(&[tool], &usage[1..])
                .remove(0)
                .span_accounting,
        );
        split.extend(
            TraceAggregation::from_spans(&[model], &usage[..1])
                .remove(0)
                .span_accounting,
        );
        assert_eq!(combined[0].span_accounting, split);
        assert_eq!(split.len(), 2);
        assert_eq!(
            split.values().map(|fact| fact.total_tokens).sum::<i64>(),
            13
        );
        let persisted = crate::ch::traces_agg::CHTraceAgg::from_aggregation(&combined[0], 0);
        assert_eq!(
            persisted.total_tokens, 0,
            "new facts must not also enter legacy additive counters"
        );
        assert_eq!(persisted.num_spans, 0);
        assert_eq!(persisted.span_accounting, split);
    }

    #[test]
    fn repeated_operations_in_one_queue_batch_count_once() {
        let model = make_span(Uuid::new_v4(), SpanType::LLM, 13);
        let rows = TraceAggregation::from_spans(
            &[model.clone(), model],
            &[make_usage(13, 0.25), make_usage(13, 0.25)],
        );
        assert_eq!(rows[0].total_tokens, 13);
        assert_eq!(rows[0].total_cost, 0.25);
        assert_eq!(rows[0].num_spans, 1);
    }

    #[test]
    fn identical_trace_and_span_ids_are_scoped_to_project() {
        let first = make_span(Uuid::new_v4(), SpanType::LLM, 13);
        let mut second = first.clone();
        second.project_id = Uuid::from_u128(2);
        let rows = TraceAggregation::from_spans(
            &[first, second],
            &[make_usage(13, 0.25), make_usage(27, 0.5)],
        );
        assert_eq!(rows.len(), 2);
        for row in rows {
            let expected = if row.project_id == Uuid::from_u128(1) {
                13
            } else {
                27
            };
            assert_eq!(row.span_accounting.len(), 1);
            assert_eq!(
                row.span_accounting.values().next().unwrap().total_tokens,
                expected
            );
        }
    }
}
