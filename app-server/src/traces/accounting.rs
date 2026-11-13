use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::{Context, ensure};
use async_trait::async_trait;

use crate::{
    api::v1::traces::RabbitMqSpanMessage,
    cache::Cache,
    db::{
        DB,
        span_accounting::{AccountingIdentity, AccountingReceipt, accept_receipts, get_receipts},
    },
    worker::HandlerError,
};

use super::{delivery::span_fingerprint, spans::SpanUsage, utils::get_llm_usage_for_span};

#[async_trait]
pub(crate) trait AccountingStore: Send + Sync {
    async fn load(
        &self,
        identities: &[AccountingIdentity],
    ) -> anyhow::Result<Vec<AccountingReceipt>>;
    async fn accept(
        &self,
        proposals: &[AccountingReceipt],
    ) -> anyhow::Result<Vec<AccountingReceipt>>;
}

#[async_trait]
impl AccountingStore for DB {
    async fn load(
        &self,
        identities: &[AccountingIdentity],
    ) -> anyhow::Result<Vec<AccountingReceipt>> {
        get_receipts(&self.pool, identities).await
    }
    async fn accept(
        &self,
        proposals: &[AccountingReceipt],
    ) -> anyhow::Result<Vec<AccountingReceipt>> {
        accept_receipts(&self.pool, proposals).await
    }
}

pub(super) struct AccountedMessage {
    pub message: RabbitMqSpanMessage,
    pub usage: SpanUsage,
}

struct ProposedMessage {
    message: RabbitMqSpanMessage,
    identity: AccountingIdentity,
    fingerprint: Vec<u8>,
}

/// Pricing and identity acceptance precede every analytical write. A lost DB
/// acknowledgement can therefore be retried without changing the accepted fact.
pub(super) async fn account_span_messages(
    messages: Vec<RabbitMqSpanMessage>,
    db: &Arc<DB>,
    cache: &Arc<Cache>,
    store: &impl AccountingStore,
) -> Result<Vec<AccountedMessage>, HandlerError> {
    let mut pending = Vec::with_capacity(messages.len());
    for mut message in messages {
        let identity = AccountingIdentity {
            project_id: message.span.project_id,
            trace_id: message.span.trace_id,
            span_id: message.span.span_id,
        };
        let fingerprint = span_fingerprint(&mut message);
        pending.push(ProposedMessage {
            message,
            identity,
            fingerprint,
        });
    }
    let identities: BTreeSet<_> = pending.iter().map(|p| p.identity).collect();
    let loaded = store
        .load(&identities.iter().copied().collect::<Vec<_>>())
        .await
        .context("load immutable span accounting receipts")
        .map_err(HandlerError::transient)?;
    let loaded = index_receipts(&identities, loaded).map_err(HandlerError::transient)?;
    let mut proposals: BTreeMap<AccountingIdentity, AccountingReceipt> = BTreeMap::new();
    for pending in &mut pending {
        if let Some(proposal) = proposals.get_mut(&pending.identity) {
            proposal.conflict_detected |= proposal.fingerprint != pending.fingerprint;
        } else if let Some(receipt) = loaded.get(&pending.identity) {
            let mut proposal = receipt.clone();
            proposal.conflict_detected |= proposal.fingerprint != pending.fingerprint;
            proposals.insert(pending.identity, proposal);
        } else {
            let usage = if pending.message.span.is_llm_span() {
                get_llm_usage_for_span(
                    &mut pending.message.span.attributes,
                    db.clone(),
                    cache.clone(),
                    &pending.identity.project_id,
                )
                .await
                .map_err(HandlerError::transient)?
            } else {
                SpanUsage::default()
            };
            proposals.insert(
                pending.identity,
                AccountingReceipt {
                    identity: pending.identity,
                    fingerprint: pending.fingerprint.clone(),
                    usage,
                    conflict_detected: false,
                },
            );
        }
    }
    // Existing immutable receipts need no write on a normal replay. New keys
    // still use the unique constraint: another worker may have won since load.
    let proposed: Vec<_> = proposals
        .into_values()
        .filter(|proposal| {
            loaded
                .get(&proposal.identity)
                .is_none_or(|receipt| receipt.conflict_detected != proposal.conflict_detected)
        })
        .collect();
    let written = store
        .accept(&proposed)
        .await
        .context("accept immutable span accounting receipts")
        .map_err(HandlerError::transient)?;
    let written = validate_receipts(&proposed, written).map_err(HandlerError::transient)?;
    let mut accepted = loaded;
    accepted.extend(written);
    let mut result = Vec::with_capacity(pending.len());
    for pending in pending {
        let receipt = &accepted[&pending.identity];
        if receipt.fingerprint != pending.fingerprint {
            log::error!(
                "span_delivery_conflict project={} trace={} span={} delivery_excluded=true",
                pending.identity.project_id,
                pending.identity.trace_id,
                pending.identity.span_id
            );
            continue;
        }
        result.push(AccountedMessage {
            message: pending.message,
            usage: receipt.usage.clone(),
        });
    }
    Ok(result)
}

fn validate_receipts(
    proposals: &[AccountingReceipt],
    receipts: Vec<AccountingReceipt>,
) -> anyhow::Result<BTreeMap<AccountingIdentity, AccountingReceipt>> {
    let identities = proposals.iter().map(|p| p.identity).collect();
    let receipts = index_receipts(&identities, receipts)?;
    ensure!(
        receipts.len() == proposals.len(),
        "accounting store returned incomplete identities"
    );
    Ok(receipts)
}

fn index_receipts(
    identities: &BTreeSet<AccountingIdentity>,
    receipts: Vec<AccountingReceipt>,
) -> anyhow::Result<BTreeMap<AccountingIdentity, AccountingReceipt>> {
    let count = receipts.len();
    let receipts: BTreeMap<_, _> = receipts
        .into_iter()
        .map(|receipt| (receipt.identity, receipt))
        .collect();
    ensure!(
        count == receipts.len(),
        "accounting store returned duplicate identities"
    );
    ensure!(
        receipts.keys().all(|key| identities.contains(key)),
        "accounting store returned a different scoped identity"
    );
    Ok(receipts)
}

#[cfg(test)]
pub(super) mod test_store {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    pub(crate) struct MemoryAccountingStore {
        pub receipts: Mutex<BTreeMap<AccountingIdentity, AccountingReceipt>>,
        pub fail_after_accept: Mutex<bool>,
        pub writes: Mutex<usize>,
    }

    #[async_trait]
    impl AccountingStore for MemoryAccountingStore {
        async fn load(
            &self,
            identities: &[AccountingIdentity],
        ) -> anyhow::Result<Vec<AccountingReceipt>> {
            let stored = self.receipts.lock().unwrap();
            Ok(identities
                .iter()
                .filter_map(|id| stored.get(id).cloned())
                .collect())
        }
        async fn accept(
            &self,
            proposals: &[AccountingReceipt],
        ) -> anyhow::Result<Vec<AccountingReceipt>> {
            *self.writes.lock().unwrap() += proposals.len();
            let mut stored = self.receipts.lock().unwrap();
            let mut result = Vec::new();
            for proposal in proposals {
                let receipt = stored
                    .entry(proposal.identity)
                    .or_insert_with(|| proposal.clone());
                receipt.conflict_detected |=
                    proposal.conflict_detected || receipt.fingerprint != proposal.fingerprint;
                result.push(receipt.clone());
            }
            if std::mem::take(&mut *self.fail_after_accept.lock().unwrap()) {
                anyhow::bail!("accounting receipt acknowledgement lost");
            }
            Ok(result)
        }
    }
}

#[cfg(test)]
#[path = "accounting_tests.rs"]
mod tests;
