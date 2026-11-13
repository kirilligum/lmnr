//! Finalized operation identity survives producer dedup and consumer retries.

use serde_json::{Value, json};

use crate::api::v1::traces::RabbitMqSpanMessage;

use super::{
    dedup::{content_hash, messages::MessageDedup, tools::extract_tool_definitions},
    provider::convert_span_to_provider_format,
    span_attributes::{ASSOCIATION_PROPERTIES_PREFIX, SPAN_AGENT_HASH, SPAN_PROMPT_HASH},
    utils::accounting_fingerprint,
};

/// Retain only a digest, never prompt/output text, in the accounting receipt.
/// Cache-dependent search markers, storage placement and enrichment aren't
/// source edits; changing the source content or lifecycle is a conflict.
pub(super) fn span_fingerprint(message: &mut RabbitMqSpanMessage) -> Vec<u8> {
    let accounting = accounting_fingerprint(&mut message.span);
    let mut converted;
    let span = if message.pre_processed {
        &message.span
    } else {
        converted = message.span.clone();
        convert_span_to_provider_format(&mut converted);
        &converted
    };
    let mut attributes = span.attributes.raw_attributes.clone();
    let raw_tools = if span.is_llm_span() {
        extract_tool_definitions(&mut attributes).map(|tools| content_hash(&Value::Array(tools)))
    } else {
        None
    };
    let tools = message
        .tool_dedup
        .as_ref()
        .map(|tools| tools.hash)
        .or(raw_tools);
    // These are backend enrichments: agent resolution and session-cache
    // locality can change between two deliveries of the same source span.
    attributes.remove(SPAN_AGENT_HASH);
    attributes.remove(SPAN_PROMPT_HASH);
    attributes.remove(&format!("{ASSOCIATION_PROPERTIES_PREFIX}.session_id"));

    let events: Vec<_> = span
        .events
        .iter()
        .map(|event| {
            json!({
                "timestamp": event.timestamp,
                "name": event.name,
                "attributes": event.attributes,
                "source": event.source,
                // Event UUIDs are assigned anew by OTLP decoding on every delivery.
            })
        })
        .collect();
    let source = json!({
        "version": 2,
        "accounting": accounting,
        "name": span.name,
        "parent": span.parent_span_id,
        "type": span.span_type,
        "start": span.start_time,
        "end": span.end_time,
        "status": span.status,
        "tags": span.tags,
        "attributes": attributes,
        "events": events,
        "input": field_identity(span.input.as_ref(), message.input_dedup.as_ref(), span.is_llm_span()),
        "output": field_identity(span.output.as_ref(), message.output_dedup.as_ref(), span.is_llm_span()),
        "tools": tools,
    });
    content_hash(&source).to_vec()
}

fn field_identity(value: Option<&Value>, dedup: Option<&MessageDedup>, is_llm: bool) -> Value {
    if let Some(dedup) = dedup {
        return json!({"messages": dedup.hashes});
    }
    match value {
        Some(Value::Array(messages)) if is_llm && !messages.is_empty() => {
            json!({"messages": messages.iter().map(content_hash).collect::<Vec<_>>()})
        }
        Some(value) => json!({"value": content_hash(value)}),
        None => json!({"absent": true}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cache::{Cache, in_memory::InMemoryCache},
        db::{
            events::{Event, EventSource},
            spans::{Span, SpanType},
        },
        traces::{
            dedup::{messages::build_message_dedup, tools::build_tool_dedup},
            spans::SpanAttributes,
        },
    };
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    fn source() -> RabbitMqSpanMessage {
        RabbitMqSpanMessage {
            span: Span {
                project_id: Uuid::from_u128(1),
                trace_id: Uuid::from_u128(2),
                span_id: Uuid::from_u128(3),
                name: "request".into(),
                span_type: SpanType::LLM,
                start_time: Utc.timestamp_opt(100, 0).unwrap(),
                end_time: Utc.timestamp_opt(101, 0).unwrap(),
                input: Some(json!([{"role":"user","content":"human input"}])),
                output: Some(json!([{"role":"assistant","content":"answer"}])),
                attributes: SpanAttributes::new(
                    serde_json::from_value(json!({
                        "gen_ai.usage.input_tokens": 10,
                        "ai.prompt.tools": [{"name":"search","parameters":{"type":"object"}}],
                    }))
                    .unwrap(),
                ),
                ..Default::default()
            },
            pre_processed: false,
            input_dedup: None,
            output_dedup: None,
            tool_dedup: None,
            agent_prompt: None,
        }
    }

    #[tokio::test]
    async fn raw_and_preprocessed_content_have_the_same_identity() {
        let mut raw = source();
        let mut wire = raw.clone();
        let cache = Cache::InMemory(InMemoryCache::new(Some(100)));
        wire.tool_dedup = build_tool_dedup(&mut wire.span, &cache).await;
        wire.input_dedup = build_message_dedup(&wire.span, wire.span.input.as_ref(), &cache).await;
        wire.output_dedup =
            build_message_dedup(&wire.span, wire.span.output.as_ref(), &cache).await;
        wire.span.input = None;
        wire.span.output = None;
        wire.pre_processed = true;
        let expected = span_fingerprint(&mut raw);
        assert_eq!(span_fingerprint(&mut wire), expected);
        for dedup in [&mut wire.input_dedup, &mut wire.output_dedup] {
            let dedup = dedup.as_mut().unwrap();
            dedup.contents.clear();
            dedup.storage_miss_indices.clear();
            dedup.trace_new_indices.clear();
        }
        wire.tool_dedup.as_mut().unwrap().content = None;
        assert_eq!(span_fingerprint(&mut wire), expected);
    }

    #[test]
    fn backend_enrichment_and_generated_event_ids_are_not_source_changes() {
        let mut raw = source();
        raw.span.events.push(Event {
            id: Uuid::new_v4(),
            span_id: raw.span.span_id,
            project_id: raw.span.project_id,
            trace_id: raw.span.trace_id,
            timestamp: raw.span.end_time,
            name: "exception".into(),
            attributes: json!({"exception.message":"synthetic"}),
            source: EventSource::Code,
        });
        let mut retry = raw.clone();
        retry.span.events[0].id = Uuid::new_v4();
        retry
            .span
            .attributes
            .raw_attributes
            .insert(SPAN_AGENT_HASH.into(), json!("resolved-later"));
        retry
            .span
            .attributes
            .raw_attributes
            .insert(SPAN_PROMPT_HASH.into(), json!("enriched"));
        retry.span.attributes.raw_attributes.insert(
            format!("{ASSOCIATION_PROPERTIES_PREFIX}.session_id"),
            json!("adopted-session"),
        );
        retry.span.input = Some(json!([{"content":"human input","role":"user"}]));
        assert_eq!(span_fingerprint(&mut retry), span_fingerprint(&mut raw));
        retry.span.events[0].attributes = json!({"exception.message":"different"});
        assert_ne!(span_fingerprint(&mut retry), span_fingerprint(&mut raw));
    }

    #[test]
    fn human_input_and_completed_output_cannot_be_replaced_or_blanked() {
        let mut raw = source();
        raw.span.span_type = SpanType::Default;
        let accepted = span_fingerprint(&mut raw);
        let mut blanked = raw.clone();
        blanked.span.input = None;
        assert_ne!(span_fingerprint(&mut blanked), accepted);
        let mut changed = raw.clone();
        changed.span.output = Some(json!("changed final answer"));
        assert_ne!(span_fingerprint(&mut changed), accepted);
        changed = raw.clone();
        changed.span.end_time += chrono::Duration::hours(8);
        assert_ne!(
            span_fingerprint(&mut changed),
            accepted,
            "an ended operation is not reopened"
        );
        changed = raw;
        changed.span.parent_span_id = Some(Uuid::from_u128(77));
        assert_ne!(
            span_fingerprint(&mut changed),
            accepted,
            "an operation cannot move to another request parent"
        );
    }
}
