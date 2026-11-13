# Replay-safe span accounting candidate

This branch is an unfinished candidate. Do not deploy its writer independently
of PostgreSQL migration 0129 and ClickHouse migrations 84 and 85. Receipt
capacity qualification, deployed realtime acceptance and a qualified
legacy cutover remain release gates.

## Persist facts before reducing them

An OTLP acknowledgement can be lost after successful persistence. The sender
then repeats span identities, and RabbitMQ can regroup them into different
batches. The former additive `traces_agg` counters count each delivery.

`TraceAggregation` now retains an accounting fact keyed by span id, scoped to
both project and trace. Each fact contains all token categories and costs.
Non-LLM operations carry zero usage but retain their identity; non-recordable
LLM operations retain their usage even though they have no stored span body.
Metadata-only operations retain the virtual span's identity too.

The appended `span_accounting` column uses
`SimpleAggregateFunction(maxMap, Map(String, Tuple(...)))`. Repeating an
immutable fact cannot change the map. Both native trace views merge maps before
summing their values. New span writes leave legacy additive counters zero;
the views add existing legacy counters to the new facts without rewriting old
totals. A new model attempt has a new span id and therefore contributes usage.

Before any regular-span analytical writes, the consumer loads PostgreSQL
receipts keyed by `(project_id, trace_id, span_id)`. An existing receipt supplies
the accepted usage and pricing without consulting current model costs. For a
new identity, the consumer calculates usage and atomically accepts a receipt;
the unique constraint arbitrates concurrent workers. The accepted fingerprint
and usage are never updated. Only the persistent `conflict_detected` flag can
change. Ordinary replays do not rewrite the receipt.

The fingerprint covers normalized source billing inputs, operation identity,
lifecycle and content: model/provider, token categories, supplied costs, service
tier, batch status, parent, timestamps, status, events, attributes and ordered
input/output/tool hashes. Producer dedup payload placement, generated event IDs
and backend agent/prompt/session enrichment do not change the fingerprint.
Processing-time prices are excluded. The receipt retains supplied,
calculated or unpriced provenance, including the rate card and calculator input
when applicable. Explicit zero costs remain supplied costs. A failed database
lookup is retryable and cannot be accepted as an unpriced zero.

A conflicting fingerprint is recorded and its delivery excluded before agent
prompts, span bodies, trace accounting, content and search writes. Other valid
messages still proceed. The original matching delivery remains replayable even
after a conflict. The contract covers finalized facts; provisional snapshots
and later corrections under the same identity are not supported updates.

A receipt records accepted accounting, not completion of downstream writes.
After a lost acknowledgement or partial ClickHouse write, the queue retries the
whole message and repairs missing writes using the same accounting fact. The
ClickHouse map therefore sees identical tuples for every accepted identity,
instead of choosing among repriced or conflicting values.

Receipts must outlive their analytical facts and every replayable queue or
checkpoint. There is no independent receipt-expiry job. Project deletion
cascades receipts; backup/restore and cutover must account for this new table.
The map's storage and reduction cost grows with unique spans per trace. The
bounded synthetic probe covered 100,000 identities under a 512 MiB query limit;
this is not production capacity qualification.

## Preserve whole requests under time filtering

Filtering partial rows by time before aggregation clips long requests. A
synthetic request with 13 tokens at its start and 5 eight hours later returned
13 when queried near the start, but 18 with an unrestricted window.

Migration 84 first selects candidate trace identities within the requested
window. It then folds all aggregate and static rows for those identities and
applies the requested start-time condition to the final result. This preserves
late usage and static fields without assuming a maximum request duration.
Tenant scoping applies to candidate discovery and both subsequent reads.
API callers retain start-time semantics by default. The trace monitor chooses
last activity by default and exposes a Started / Last activity selector. List,
count and chart queries share the chosen time field. Activity searches select
matching request identities first and search their full retained history with
an explicit range; an omitted search range would silently clip to seven days.
The query validator already treats an end-time lower bound as insufficient to
bound the trace start. The RowBinary integration exercises this rewrite: a
recent-activity window returns all 18 tokens and the original input from an
eight-hour-old request, while the same start-time window excludes it.

Last activity means the latest observed span end, not a heartbeat or evidence
that a quiet operation is still running. Lifecycle display requires explicit
source state. Candidate-ID materialization and full-history search need load
qualification; the existing search result cap still applies.

## Canonical reads and partial writes

Migration 85 retains the physical span deliveries and adds a canonical view
keyed by project, trace and span id. Native span views, evaluation joins, trace
lookup, search snippets, usage-limit reads and autocomplete use that view.
Canonicalization precedes aggregation. Selecting a whole row preserves its
content and masking fields together; screened rows and the original trace-new
content markers take precedence over a retry's empty markers. This selection
assumes immutable operation content, not arbitrary span updates.
The receipt rejects a changed source body or lifecycle before analytical writes,
so rows accepted under one identity have the same source content. Backend
masking and dedup markers remain separate projections of that source.

Metadata-only queue deliveries preserve the virtual span identity in the map,
so retrying a patch adds neither a second span count nor model usage. Their
zero end time cannot advance observed activity. The RowBinary integration
replays both aggregate and static patch rows in a different partition from the
request, then reads both native trace views. It checks unchanged input, usage,
activity and raw existence/start-time readers. Separate metadata API calls still
create separate virtual operations; this does not make that API idempotent.

The database byte-usage reader sums canonical span rows. Its integration case
uses the real billing-window query with a smaller retry row, another project
and an earlier billing period, before and after physical merges. This verifies
the stored read model only. The existing cloud usage-limit cache still adds
bytes per delivery, and non-recordable spans have no stored body to reconstruct
their bytes. Do not claim replay-safe cloud quota enforcement from this change.
The processing path is gated by `Feature::UsageLimit`; deployment qualification
must check that feature separately.

The raw `traces_agg` readers outside native views use existence, minimum start
time, historical-backfill membership, signal/cluster identity sets, or physical
table telemetry. The telemetry counter explicitly measures physical rows, not
logical traces. Historical backfill still writes legacy additive counters and
must finish before a cutover is qualified. Signal execution is compiled out of
the default OSS Docker build; a signals-enabled build needs its own replay
qualification and is not covered by the default-image checks.

The processor awaits both trace and span persistence branches. Either failure
returns a transient handler error, allowing the existing queue worker to retry.
Trace realtime notifications follow successful persistence. A recording-sink
pipeline test injects failures before persistence and after persistence with a
lost acknowledgement at each of `traces_agg`, `traces_static`, `unique_content`
and `spans`. It checks requeue decisions, absence of premature notifications,
successful repair and unchanged accounting facts, including non-recordable
usage. Real ClickHouse reduction is qualified separately by the integration
test below; these tests do not constitute a deployed queue/restart test.

## Reconcile realtime views from stored rows

The trace and evaluation tables treat SSE events as invalidations. They
coalesce notifications, refresh their loaded page window without clearing it,
and read totals, input and metadata from the normal policy-aware query paths.
An event during a fetch schedules another reconciliation. Superseded requests
are aborted and their results discarded even if the transport ignores abort.
The last fetched page controls pagination; a refresh retains the loaded window.
Trace charts and evaluation statistics also reload their stored aggregates.

The debugger uses its existing batched row endpoint. Loads are serialized;
an invalidation during a load remains pending for a subsequent read. New blocks
can appear immediately, but their rows wait for authoritative data instead of
using a batch delta as a total. Reconnects refresh known rows, the block index
and expanded traces. Span reads use trace identity without an old row's time
bounds; reconnects during a span read schedule another read. Teardown aborts
loads and prevents late responses from updating the store.

The SSE wire shape retains its delta fields for compatibility, but these UI
consumers no longer add or seed totals from them. The obsolete accumulation
helpers and their behavior tests were replaced with refresh-contract tests:
duplicate notifications, missed-event reconciliation, all loaded pages, changes
during fetches, stale responses, cancellation, and failed-refresh recovery.
Debugger-store tests cover the actual row loading and new-run/span flow.

## Remaining release gates

- Legacy/new overlap: an old span already represented by additive history must
  not also contribute a new accounting fact when it is replayed after cutover.
  Historical non-recordable operations lack stored span bodies, so a blanket
  reconstruction from `spans` cannot prove that boundary. Preserve old totals
  and qualify the cutover explicitly before enabling the writer.
- Qualify deployed receipt storage/write load, queue recovery and coordinated
  restore; isolated SQL and processor tests do not establish those properties.
- Qualify the combined content/accounting receipt with the installed producers;
  metadata-only virtual writes retain their existing separate path.
- Verify recent-activity discovery and lifecycle display for old running
  requests, separately from start-time analytical filters.
- Qualify canonical-read cost against production query shapes and data volume;
  a bounded synthetic aggregate does not prove payload-heavy UI capacity.
- Qualify the compiled native UI, reconnect behavior and query load with the
  candidate backend. Source tests do not establish installed UI acceptance.
- Verify the deployed queue/restart boundary and partial-write repair with both
  existing producers after all other replay gates pass.
- Verify that the deployed build's raw readers and feature gates match the
  audited paths above. Rollback and the existing consumer queue/checkpoint
  boundary still require operational evidence.
- Qualify the actual RowBinary writer, migration views, reordered/rebatched
  replay, mixed projects, non-recordable usage, long requests and late fields.

The unit tests exercise the accounting keys and real processor projection.
`ch::traces_agg::tests::span_accounting_rowbinary_replay` is an explicit ignored
integration test using the registered `CLICKHOUSE_URL`, `CLICKHOUSE_USER` and
`CLICKHOUSE_PASSWORD`. The opt-in database tests read migration files from the
full repository at runtime; ordinary app-server-only Docker test builds do not
need the frontend source. It creates and removes a uniquely named database, clones
the existing table schemas, applies migrations 84 and 85, and verifies a replayed
real Rust insert plus both native trace views with eight-hour continuations and
late static fields. It also verifies canonical native span counts before and
after merges, tenant isolation, original content markers and fail-closed PII
masking. It never writes existing application tables.

`traces::accounting` tests accepted pricing across changed/missing rates, lost
receipt acknowledgements, retryable pricing failures, explicit zero versus
unpriced provenance, conflicting facts and project/trace isolation. The
processor tests exercise receipt acknowledgement loss before any analytical
write and exclusion of conflicting usage while preserving unrelated valid
work, alongside the existing eight partial-write failure cases.
`traces::delivery` tests raw/deduplicated/stripped payload parity, backend-only
enrichment, event identity, source content and lifecycle conflicts. The processor
regression accepts an identical replay and rejects a changed answer before any
analytical write.

`db::span_accounting::tests::postgres_receipts_arbitrate_concurrent_prices_and_conflicts`
is an opt-in integration test using `DATABASE_URL`. It creates a unique
temporary schema, a minimal synthetic `projects` table and migration 0129.
Eight concurrent workers propose different prices for the same source fact;
all must receive one accepted snapshot. It also checks replay, durable conflict
flags, scoped identities and project-deletion cascade. A bounded probe then
inserts 10,000 synthetic receipts through the production batch API and reads
them in reversed order and different batch sizes, checking every accepted fact
and total cardinality. It reports insert/read time and table-plus-index bytes;
there is no hardware-dependent timing assertion. The schema is dropped after
the probe. It does not run the application migrator, modify application tables,
or establish full ingestion-pipeline capacity.
