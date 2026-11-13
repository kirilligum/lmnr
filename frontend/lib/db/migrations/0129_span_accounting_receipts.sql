-- Apply before enabling the matching consumer. Receipts must be retained for
-- at least as long as their analytical facts and replayable queue/checkpoints.
-- Dropping receipts alone permits historical deliveries to be repriced.
CREATE TABLE "span_accounting_receipts" (
    "project_id" uuid NOT NULL,
    "trace_id" uuid NOT NULL,
    "span_id" uuid NOT NULL,
    "fingerprint" bytea NOT NULL,
    "usage" jsonb NOT NULL,
    "conflict_detected" boolean DEFAULT false NOT NULL,
    "accepted_at" timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT "span_accounting_receipts_pkey" PRIMARY KEY ("project_id", "trace_id", "span_id"),
    CONSTRAINT "span_accounting_receipts_fingerprint_check" CHECK (octet_length("fingerprint") = 32),
    CONSTRAINT "span_accounting_receipts_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "projects"("id") ON DELETE cascade ON UPDATE no action
);
