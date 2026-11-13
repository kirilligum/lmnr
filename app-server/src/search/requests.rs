//! Request membership is independent of the number of matching span documents.
//! A bounded candidate set makes the terms aggregation exhaustive on every split.
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::{
    quickwit::{SPANS_INDEX_ID, client::QuickwitClient},
    routes::error::Error,
};

pub const MAX_CANDIDATES: usize = 256;

#[derive(Deserialize)]
struct SearchResponse {
    aggregations: Aggregations,
}

#[derive(Deserialize)]
struct Aggregations {
    requests: Terms,
}

#[derive(Deserialize)]
struct Terms {
    buckets: Vec<Bucket>,
    sum_other_doc_count: u64,
    doc_count_error_upper_bound: u64,
}

#[derive(Deserialize)]
struct Bucket {
    key: Uuid,
}

fn query_body(
    project_id: Uuid,
    candidates: &[Uuid],
    query: &str,
    fields: &[String],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> serde_json::Value {
    let ids = candidates
        .iter()
        .map(Uuid::to_string)
        .collect::<Vec<_>>()
        .join(" ");
    json!({
        "query": format!("project_id:{project_id} AND trace_id:IN [{ids}] AND ({})", super::escape_quickwit_query(query)),
        "search_field": fields.join(","),
        "start_timestamp": start.timestamp(),
        "end_timestamp": end.timestamp(),
        "max_hits": 0,
        "aggs": {"requests": {"terms": {
            "field": "trace_id", "size": candidates.len(), "shard_size": candidates.len(),
            "show_term_doc_count_error": true
        }}}
    })
}

fn parse_matches(value: serde_json::Value, candidates: &[Uuid]) -> anyhow::Result<Vec<Uuid>> {
    let response: SearchResponse = serde_json::from_value(value)?;
    let terms = response.aggregations.requests;
    anyhow::ensure!(
        terms.sum_other_doc_count == 0 && terms.doc_count_error_upper_bound == 0,
        "Request search returned incomplete results"
    );
    let mut ids = Vec::with_capacity(terms.buckets.len());
    for bucket in terms.buckets {
        anyhow::ensure!(
            candidates.contains(&bucket.key),
            "Request search returned an unexpected trace"
        );
        anyhow::ensure!(
            !ids.contains(&bucket.key),
            "Request search returned a duplicate trace"
        );
        ids.push(bucket.key);
    }
    Ok(ids)
}

pub async fn search_requests(
    client: &QuickwitClient,
    project_id: Uuid,
    candidates: &[Uuid],
    query: &str,
    fields: &[String],
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<Vec<Uuid>, Error> {
    if candidates.len() > MAX_CANDIDATES {
        return Err(anyhow::anyhow!("Too many request search candidates").into());
    }
    if candidates.is_empty() || query.trim().is_empty() {
        return Ok(Vec::new());
    }
    let value = client
        .search_index(
            &SPANS_INDEX_ID,
            query_body(project_id, candidates, query, fields, start, end),
        )
        .await?;
    Ok(parse_matches(value, candidates)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_membership_ignores_span_hit_limit_and_rejects_incomplete_results() {
        let ids = [Uuid::new_v4(), Uuid::new_v4()];
        let response = json!({"hits": [], "num_hits": 10001, "aggregations": {"requests": {
            "buckets": [{"key": ids[0], "doc_count": 10000}, {"key": ids[1], "doc_count": 1}],
            "sum_other_doc_count": 0, "doc_count_error_upper_bound": 0
        }}});
        assert_eq!(parse_matches(response.clone(), &ids).unwrap(), ids);
        for field in ["sum_other_doc_count", "doc_count_error_upper_bound"] {
            let mut incomplete = response.clone();
            incomplete["aggregations"]["requests"][field] = json!(1);
            assert!(parse_matches(incomplete, &ids).is_err());
        }
        assert!(parse_matches(response, &ids[..1]).is_err());
        assert!(parse_matches(json!({"hits": []}), &ids).is_err());
    }

    #[test]
    fn request_query_bounds_every_split_by_project_and_candidate_identity() {
        let ids = [Uuid::new_v4(), Uuid::new_v4()];
        let project = Uuid::new_v4();
        let start = DateTime::from_timestamp(0, 0).unwrap();
        let end = Utc::now();
        let body = query_body(
            project,
            &ids,
            "human request",
            &["input".into()],
            start,
            end,
        );
        assert!(
            body["query"]
                .as_str()
                .unwrap()
                .contains(&format!("project_id:{project}"))
        );
        for id in ids {
            assert!(body["query"].as_str().unwrap().contains(&id.to_string()));
        }
        assert_eq!(body["aggs"]["requests"]["terms"]["size"], 2);
        assert_eq!(body["aggs"]["requests"]["terms"]["shard_size"], 2);
        assert_eq!(body["max_hits"], 0);
        assert_eq!(body["search_field"], "input");
        assert_eq!(body["start_timestamp"], 0);
        assert_eq!(body["end_timestamp"], end.timestamp());
    }

    /// Uses one explicitly test-owned index on the configured Quickwit server.
    /// No application index or production trace is written.
    #[tokio::test]
    #[ignore = "requires QUICKWIT_SEARCH_URL and a unique test QUICKWIT_SPANS_INDEX_ID"]
    async fn quickwit_request_discovery_survives_dominant_trace_and_old_input() -> anyhow::Result<()>
    {
        use crate::quickwit::client::QuickwitConfig;
        let index = crate::env::quickwit::SPANS_INDEX_ID.get();
        anyhow::ensure!(
            index.starts_with("lmnr_request_search_test_"),
            "test index required"
        );
        let config = QuickwitConfig::from_env();
        let base = config.search_endpoint.clone();
        let client = QuickwitClient::connect_lazy(config)?;
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()?;
        let template = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../frontend/lib/quickwit/indexes/spans_v2.yaml"),
        )?;
        let schema = template.replace("index_id: spans_v2", &format!("index_id: {index}"));
        http.post(format!("{base}/api/v1/indexes"))
            .header("Content-Type", "application/yaml")
            .body(schema)
            .send()
            .await?
            .error_for_status()?;
        let result: anyhow::Result<()> = async {
            let project = Uuid::new_v4();
            let foreign = Uuid::new_v4();
            let ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
            let now = Utc::now();
            let document = |project, trace, time: DateTime<Utc>, input, output| json!({
                "project_id": project, "trace_id": trace, "span_id": Uuid::new_v4(),
                "start_time": time.to_rfc3339(), "input": input, "output": output
            }).to_string();
            let frequent = (0..600).map(|_| document(project, ids[0], now, "human request", "ordinary"))
                .collect::<Vec<_>>().join("\n");
            let sparse = [
                document(project, ids[1], now - chrono::Duration::days(9), "human request", "ordinary"),
                document(project, ids[2], now, "ordinary", "human request"),
                document(foreign, ids[2], now, "human request", "ordinary"),
            ].join("\n");
            for batch in [frequent, sparse] {
                http.post(format!("{base}/api/v1/{index}/ingest?commit=force"))
                    .header("Content-Type", "application/json").body(batch)
                    .send().await?.error_for_status()?;
            }
            let start = DateTime::from_timestamp(0, 0).unwrap();
            // Quickwit filters timestamp bounds at second resolution.
            let end = now + chrono::Duration::seconds(1);
            let mut found = search_requests(&client, project, &ids, "human request", &["input".into()], start, end).await?;
            found.sort();
            let mut expected = vec![ids[0], ids[1]];
            expected.sort();
            anyhow::ensure!(found == expected, "input request membership is incomplete or crossed projects");
            let output = search_requests(&client, project, &ids, "human request", &["output".into()], start, end).await?;
            anyhow::ensure!(output == vec![ids[2]], "search fields were not respected");
            let mut capped = query_body(project, &ids, "human request", &["input".into()], start, end);
            capped.as_object_mut().unwrap().remove("aggs");
            capped["max_hits"] = json!(500);
            capped["sort_by"] = json!("start_time");
            let raw = client.search_index(&index, capped).await?;
            let hits = raw["hits"].as_array().unwrap();
            anyhow::ensure!(hits.len() == 500 && hits.iter().all(|hit| hit["trace_id"] == ids[0].to_string()),
                "fixture did not reproduce the original span-hit truncation");
            println!("request search: 603 documents, 2 input requests, 1 output request, tenant isolation, old input and 500-hit regression passed");
            Ok(())
        }.await;
        let cleanup = http
            .delete(format!("{base}/api/v1/indexes/{index}"))
            .send()
            .await?
            .error_for_status();
        cleanup?;
        result
    }
}
