import assert from "node:assert/strict";
import { test, type TestContext } from "node:test";
import { setImmediate } from "node:timers/promises";

import { createDebuggerSessionViewStore } from "@/components/debugger-sessions/debugger-session-view/store";
import { type RealtimeTracePayload, type TraceRow } from "@/lib/traces/types";

const traceId = "00000000-0000-0000-0000-000000000001";
const projectId = "00000000-0000-0000-0000-000000000002";
const startTime = "2026-01-01T00:00:00Z";
const notification = { id: traceId, startTime, totalTokens: 13 } as RealtimeTracePayload;
const row = (tokens: number) =>
  ({
    id: traceId,
    startTime,
    endTime: "2026-01-01T08:00:00Z",
    totalTokens: tokens,
    agentInput: "human request",
    metadata: {},
  }) as TraceRow;

function fixture(t: TestContext) {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const descriptor = Object.getOwnPropertyDescriptor(globalThis, "localStorage");
  Object.defineProperty(globalThis, "localStorage", {
    configurable: true,
    value: { getItem: () => null, setItem: () => {}, removeItem: () => {} },
  });
  const store = createDebuggerSessionViewStore({
    projectId,
    sessionId: "session",
    storeKey: "synthetic-realtime-test",
  });
  t.after(() => {
    store.getState().cancelTraceLoads();
    if (descriptor) Object.defineProperty(globalThis, "localStorage", descriptor);
    else Reflect.deleteProperty(globalThis, "localStorage");
  });
  return store;
}

test("debugger notifications never add counters; reconnect-style refresh reads the stored total", async (t) => {
  const store = fixture(t);
  let tokens = 13;
  let requests = 0;
  t.mock.method(globalThis, "fetch", async () => {
    requests++;
    return Response.json({ traces: [row(tokens)] });
  });
  store.setState({
    traces: [row(13)],
    traceRowStates: { [traceId]: "loaded" },
    blocks: [{ id: "block", type: "trace", traceId, createdAt: startTime }],
  });
  store.getState().applyTraceUpdates([notification, notification]);
  assert.equal(store.getState().traces[0].totalTokens, 13);
  t.mock.timers.tick(150);
  await setImmediate();
  assert.equal(requests, 1);
  assert.equal(store.getState().traces[0].totalTokens, 13);
  tokens = 18;
  store.getState().refreshTraceRows([traceId]);
  t.mock.timers.tick(150);
  await setImmediate();
  assert.equal(store.getState().traces[0].totalTokens, 18);
});

test("a new debugger run loads authoritative input and all spans without stale time bounds", async (t) => {
  const store = fixture(t);
  const urls: string[] = [];
  t.mock.method(globalThis, "fetch", async (url: string, init?: RequestInit) => {
    urls.push(url);
    if (url.endsWith("/spans")) return Response.json([]);
    if (init?.method === "POST") return Response.json({ traces: [row(13)] });
    return Response.json({ blocks: [{ id: "block", type: "trace", traceId, createdAt: startTime }] });
  });
  store.setState({ isInitialTracesLoaded: true });
  store.getState().applyTraceUpdates([notification]);
  assert.deepEqual(store.getState().traces, [], "a batch delta is insufficient to seed a trace total");
  t.mock.timers.tick(150);
  await setImmediate();
  assert.equal(store.getState().traces[0].agentInput, "human request");
  assert.equal(store.getState().traces[0].totalTokens, 13);
  assert.deepEqual(store.getState().traceSpans[traceId], []);
  assert.ok(urls.includes(`/api/projects/${projectId}/traces/${traceId}/spans`));
  assert.ok(urls.every((url) => !url.includes("startDate") && !url.includes("endDate")));
});

test("a reconnect during a span read queues another read and teardown discards late responses", async (t) => {
  const store = fixture(t);
  store.setState({ traces: [row(13)] });
  let resolveFirst!: (response: Response) => void;
  const first = new Promise<Response>((resolve) => {
    resolveFirst = resolve;
  });
  let requests = 0;
  const signals: AbortSignal[] = [];
  t.mock.method(globalThis, "fetch", async (_url: string, init: RequestInit) => {
    requests++;
    signals.push(init.signal as AbortSignal);
    return requests === 1 ? first : Response.json([]);
  });
  const pending = store.getState().fetchTraceSpans(row(13));
  await store.getState().fetchTraceSpans(row(18));
  assert.equal(requests, 1);
  resolveFirst(Response.json([]));
  await pending;
  await setImmediate();
  assert.equal(requests, 2);
  assert.equal(store.getState().traceSpansFetching[traceId], false);

  let resolveLate!: (response: Response) => void;
  t.mock.method(globalThis, "fetch", (_url: string, init: RequestInit) => {
    signals.push(init.signal as AbortSignal);
    return new Promise<Response>((resolve) => {
      resolveLate = resolve;
    });
  });
  const late = store.getState().fetchTraceSpans(row(18));
  store.getState().cancelTraceLoads();
  assert.equal(signals.at(-1)?.aborted, true);
  resolveLate(Response.json([{ spanId: "late" }]));
  await late;
  assert.deepEqual(store.getState().traceSpans[traceId], []);
});
