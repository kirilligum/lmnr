use sqlx::{FromRow, PgPool, Postgres, QueryBuilder, types::Json};
use uuid::Uuid;

use crate::traces::spans::SpanUsage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, FromRow)]
pub(crate) struct AccountingIdentity {
    pub project_id: Uuid,
    pub trace_id: Uuid,
    pub span_id: Uuid,
}

#[derive(Debug, Clone, FromRow)]
pub(crate) struct AccountingReceipt {
    #[sqlx(flatten)]
    pub identity: AccountingIdentity,
    pub fingerprint: Vec<u8>,
    #[sqlx(json)]
    pub usage: SpanUsage,
    pub conflict_detected: bool,
}

pub(crate) async fn get_receipts(
    pool: &PgPool,
    identities: &[AccountingIdentity],
) -> anyhow::Result<Vec<AccountingReceipt>> {
    let mut receipts = Vec::new();
    for chunk in identities.chunks(1000) {
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT project_id, trace_id, span_id, fingerprint, usage, conflict_detected \
             FROM span_accounting_receipts WHERE (project_id, trace_id, span_id) IN (",
        );
        let mut rows = query.separated(", ");
        for identity in chunk {
            rows.push("(")
                .push_bind_unseparated(identity.project_id)
                .push_unseparated(", ")
                .push_bind_unseparated(identity.trace_id)
                .push_unseparated(", ")
                .push_bind_unseparated(identity.span_id)
                .push_unseparated(")");
        }
        query.push(")");
        receipts.extend(
            query
                .build_query_as::<AccountingReceipt>()
                .fetch_all(pool)
                .await?,
        );
    }
    Ok(receipts)
}

/// Callers provide unique, sorted identities. The unique constraint arbitrates
/// concurrent workers; accepted fingerprints and pricing are never updated.
pub(crate) async fn accept_receipts(
    pool: &PgPool,
    proposals: &[AccountingReceipt],
) -> anyhow::Result<Vec<AccountingReceipt>> {
    let mut receipts = Vec::with_capacity(proposals.len());
    for chunk in proposals.chunks(1000) {
        let mut query = QueryBuilder::<Postgres>::new(
            "INSERT INTO span_accounting_receipts \
             (project_id, trace_id, span_id, fingerprint, usage, conflict_detected) ",
        );
        query.push_values(chunk, |mut row, proposal| {
            row.push_bind(proposal.identity.project_id)
                .push_bind(proposal.identity.trace_id)
                .push_bind(proposal.identity.span_id)
                .push_bind(&proposal.fingerprint)
                .push_bind(Json(&proposal.usage))
                .push_bind(proposal.conflict_detected);
        });
        query.push(
            " ON CONFLICT (project_id, trace_id, span_id) DO UPDATE SET \
             conflict_detected = span_accounting_receipts.conflict_detected \
                 OR EXCLUDED.conflict_detected \
                 OR span_accounting_receipts.fingerprint <> EXCLUDED.fingerprint \
             RETURNING project_id, trace_id, span_id, fingerprint, usage, conflict_detected",
        );
        receipts.extend(
            query
                .build_query_as::<AccountingReceipt>()
                .fetch_all(pool)
                .await?,
        );
    }
    Ok(receipts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::ensure;
    use sqlx::{
        Executor,
        postgres::{PgConnectOptions, PgPoolOptions},
    };
    use std::{str::FromStr, sync::Arc};
    use tokio::{sync::Barrier, task::JoinSet};

    /// Uses only a unique temporary schema and synthetic project rows. Never
    /// runs the application migrator or writes existing application tables.
    #[tokio::test]
    #[ignore = "requires DATABASE_URL and permission to create a temporary schema"]
    async fn postgres_receipts_arbitrate_concurrent_prices_and_conflicts() -> anyhow::Result<()> {
        let url = std::env::var(crate::env::database::URL)?;
        let admin = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await?;
        let schema = format!("lmnr_accounting_test_{}", Uuid::new_v4().simple());
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&admin)
            .await?;
        let result: anyhow::Result<()> = async {
            let options =
                PgConnectOptions::from_str(&url)?.options([("search_path", schema.as_str())]);
            let pool = PgPoolOptions::new()
                .max_connections(4)
                .connect_with(options)
                .await?;
            pool.execute("CREATE TABLE projects (id uuid PRIMARY KEY)")
                .await?;
            pool.execute(include_str!(
                "../../../frontend/lib/db/migrations/0129_span_accounting_receipts.sql"
            ))
            .await?;
            for project_id in [Uuid::from_u128(1), Uuid::from_u128(9)] {
                sqlx::query("INSERT INTO projects (id) VALUES ($1)")
                    .bind(project_id)
                    .execute(&pool)
                    .await?;
            }
            let identity = AccountingIdentity {
                project_id: Uuid::from_u128(1),
                trace_id: Uuid::from_u128(2),
                span_id: Uuid::from_u128(3),
            };
            let barrier = Arc::new(Barrier::new(8));
            let mut workers = JoinSet::new();
            for n in 1..=8 {
                let pool = pool.clone();
                let barrier = barrier.clone();
                workers.spawn(async move {
                    let proposal = AccountingReceipt {
                        identity,
                        fingerprint: vec![1; 32],
                        conflict_detected: false,
                        usage: SpanUsage {
                            total_tokens: 13,
                            total_cost: n as f64,
                            pricing_basis: serde_json::json!({"rate_version": n}),
                            ..Default::default()
                        },
                    };
                    barrier.wait().await;
                    accept_receipts(&pool, &[proposal]).await
                });
            }
            let mut winner: Option<SpanUsage> = None;
            while let Some(result) = workers.join_next().await {
                let receipts = result??;
                ensure!(
                    receipts.len() == 1 && receipts[0].identity == identity,
                    "scoped receipt missing"
                );
                if let Some(winner) = &winner {
                    ensure!(
                        *winner == receipts[0].usage,
                        "concurrent worker changed accepted pricing"
                    );
                } else {
                    winner = Some(receipts[0].usage.clone());
                }
            }
            let winner = winner.unwrap();
            let receipt = AccountingReceipt {
                identity,
                fingerprint: vec![1; 32],
                usage: winner.clone(),
                conflict_detected: false,
            };
            // Discard a successful result, as after a lost acknowledgement.
            accept_receipts(&pool, &[receipt.clone()]).await?;
            let mut repriced = receipt.clone();
            repriced.usage.total_cost = 99.0;
            ensure!(
                accept_receipts(&pool, &[repriced]).await?[0].usage == winner,
                "replay repriced accepted usage"
            );
            let mut conflict = receipt.clone();
            conflict.fingerprint = vec![2; 32];
            conflict.usage.total_tokens = 999;
            let result = accept_receipts(&pool, &[conflict]).await?;
            ensure!(
                result[0].conflict_detected
                    && result[0].usage == winner
                    && result[0].fingerprint == vec![1; 32],
                "conflict replaced the canonical fact"
            );

            let mut other_project = receipt.clone();
            other_project.identity.project_id = Uuid::from_u128(9);
            other_project.usage.total_tokens = 21;
            let mut other_trace = receipt.clone();
            other_trace.identity.trace_id = Uuid::from_u128(8);
            other_trace.usage.total_tokens = 34;
            accept_receipts(&pool, &[other_trace.clone(), other_project.clone()]).await?;
            let loaded = get_receipts(
                &pool,
                &[identity, other_trace.identity, other_project.identity],
            )
            .await?;
            ensure!(loaded.len() == 3, "project or trace scope collided");
            for expected in [other_trace, other_project] {
                ensure!(
                    loaded
                        .iter()
                        .any(|r| r.identity == expected.identity && r.usage == expected.usage),
                    "scoped usage changed"
                );
            }
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM span_accounting_receipts")
                .fetch_one(&pool)
                .await?;
            ensure!(count == 3, "retry inserted extra receipts");
            sqlx::query("DELETE FROM projects WHERE id = $1")
                .bind(identity.project_id)
                .execute(&pool)
                .await?;
            let count: i64 = sqlx::query_scalar("SELECT count(*) FROM span_accounting_receipts")
                .fetch_one(&pool)
                .await?;
            ensure!(count == 1, "project deletion failed to cascade");
            pool.close().await;
            Ok(())
        }
        .await;
        let cleanup = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA \"{schema}\" CASCADE"
        )))
        .execute(&admin)
        .await;
        admin.close().await;
        result?;
        cleanup?;
        Ok(())
    }
}
