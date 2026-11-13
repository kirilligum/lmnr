import assert from "node:assert/strict";
import { test } from "node:test";

import { Operator } from "@/lib/actions/common/operators";
import {
  buildTracesCountQueryWithParams,
  buildTracesIdsQueryWithParams,
  buildTracesQueryWithParams,
} from "@/lib/actions/traces/utils";
import { monitorTimeField } from "@/lib/traces/time-filter";

const options = {
  projectId: "00000000-0000-0000-0000-000000000001",
  traceType: "DEFAULT" as const,
  traceIds: [],
  filters: [],
  limit: 50,
  offset: 0,
  pastHours: "1",
};

test("activity list, count and search candidates use last activity without clipping request start", () => {
  for (const build of [buildTracesQueryWithParams, buildTracesCountQueryWithParams, buildTracesIdsQueryWithParams]) {
    const { query, parameters } = build({ ...options, timeField: "activity" });
    assert.match(query, /end_time >= now\(\) - INTERVAL \{pastHours:UInt32\} HOUR/);
    assert.doesNotMatch(query, /start_time >=/);
    assert.equal(parameters.pastHours, 1);
  }
  const { query, parameters } = buildTracesQueryWithParams({ ...options, timeField: "activity" });
  assert.match(query, /ORDER BY end_time DESC/);
  assert.match(query, /LIMIT \{limit:UInt32\}/);
  assert.equal(parameters.limit, 50);
});

test("existing API callers retain start-time filtering and sorting", () => {
  const { query } = buildTracesQueryWithParams(options);
  assert.match(query, /start_time >= now\(\)/);
  assert.match(query, /ORDER BY start_time DESC/);
  assert.equal(monitorTimeField(null), "activity");
  assert.equal(monitorTimeField("start"), "start");
});

test("activity bounds and custom filters also constrain full-history search candidates", () => {
  const { query, parameters } = buildTracesIdsQueryWithParams({
    traceType: "DEFAULT",
    filters: [{ column: "custom:request", operator: Operator.Eq, value: "human-request-v1" }],
    customColumns: [{ id: "custom:request", sql: "JSONExtractString(metadata, 'cli_capture_contract')" }],
    timeField: "activity",
    startTime: "2026-01-01T07:00:00Z",
    endTime: "2026-01-01T09:00:00Z",
  });
  assert.match(query, /end_time >= \{startTime:String\}/);
  assert.match(query, /end_time <= \{endTime:String\}/);
  assert.doesNotMatch(query, /start_time >=/);
  assert.match(query, /JSONExtractString\(metadata, 'cli_capture_contract'\)/);
  assert.ok(Object.values(parameters).includes("human-request-v1"));
});
