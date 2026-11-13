import assert from "node:assert/strict";
import { test } from "node:test";
import { setImmediate } from "node:timers/promises";

import { createIdBatchLoader } from "@/lib/id-batch-loader";

type Row = { id: string; tokens: number };

test("row invalidations are coalesced and serialized, including an invalidation during a fetch", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let resolveFirst!: (rows: Row[]) => void;
  const first = new Promise<Row[]>((resolve) => {
    resolveFirst = resolve;
  });
  const requests: string[][] = [];
  const rows = new Map<string, Row>();
  const loader = createIdBatchLoader<Row>({
    batchSize: 2,
    debounceMs: 150,
    getId: (row) => row.id,
    fetchBatch: async (ids) => {
      requests.push(ids);
      return requests.length === 1 ? first : ids.map((id) => ({ id, tokens: 18 }));
    },
    onBatch: (_ids, incoming) => {
      for (const [id, row] of incoming) rows.set(id, row);
    },
    onError: () => assert.fail("unexpected fetch failure"),
  });
  t.after(() => loader.cancel());
  loader.load(["a", "a"]);
  t.mock.timers.tick(150);
  loader.load(["a", "b", "c"]);
  t.mock.timers.tick(1000);
  assert.deepEqual(requests, [["a"]]);
  resolveFirst([{ id: "a", tokens: 13 }]);
  await setImmediate();
  t.mock.timers.tick(150);
  await setImmediate();
  assert.deepEqual(requests, [["a"], ["a", "b"], ["c"]]);
  assert.equal(rows.get("a")?.tokens, 18);
});

test("cancellation aborts transport and discards a late response without affecting a later load", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let resolveOld!: (rows: Row[]) => void;
  const old = new Promise<Row[]>((resolve) => {
    resolveOld = resolve;
  });
  const signals: AbortSignal[] = [];
  const committed: Row[] = [];
  const loader = createIdBatchLoader<Row>({
    batchSize: 100,
    debounceMs: 150,
    getId: (row) => row.id,
    fetchBatch: async (_ids, signal) => {
      signals.push(signal);
      return signals.length === 1 ? old : [{ id: "a", tokens: 18 }];
    },
    onBatch: (_ids, rows) => {
      committed.push(...rows.values());
    },
    onError: () => assert.fail("cancelled fetch must not report a failure"),
  });
  t.after(() => loader.cancel());
  loader.load(["a"]);
  t.mock.timers.tick(150);
  loader.cancel();
  assert.equal(signals[0].aborted, true);
  loader.load(["a"]);
  t.mock.timers.tick(150);
  await setImmediate();
  resolveOld([{ id: "a", tokens: 13 }]);
  await setImmediate();
  assert.deepEqual(committed, [{ id: "a", tokens: 18 }]);
});
