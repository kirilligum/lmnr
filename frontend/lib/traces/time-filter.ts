export type TraceTimeField = "start" | "activity";

export const traceTimeColumn = (field: TraceTimeField = "start"): "start_time" | "end_time" =>
  field === "activity" ? "end_time" : "start_time";

// The monitor follows current work; server callers keep start-time semantics
// unless they explicitly request activity filtering.
export const monitorTimeField = (value: string | null): TraceTimeField => (value === "start" ? "start" : "activity");
