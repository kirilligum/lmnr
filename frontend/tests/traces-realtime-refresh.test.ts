import assert from "node:assert/strict";
import { test } from "node:test";
import { setImmediate } from "node:timers/promises";

import { createPagedLoader, type PageResult } from "@/lib/paged-loader";

type Row = { id: string; tokens: number };
const deferred = <T>() => {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<T>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
};

function harness(fetchPage: (page: number, signal: AbortSignal) => Promise<PageResult<Row>>) {
  const state = { rows: [] as Row[], page: 0, more: true, fetching: false, errors: [] as Error[] };
  const loader = createPagedLoader({
    fetchPage,
    pageSize: 2,
    getCurrentPage: () => state.page,
    onStart: (reset) => {
      state.fetching = true;
      if (reset) {
        state.rows = [];
        state.page = 0;
      }
    },
    onResult: (result, page, replace, more) => {
      state.rows = replace ? result.items : [...state.rows, ...result.items];
      state.page = page;
      state.more = more;
    },
    onIdle: () => {
      state.fetching = false;
    },
    onError: (error) => {
      state.errors.push(error);
    },
  });
  return { state, loader };
}

test("repeated notifications reconcile totals and preserve all loaded pages", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let tokens = 13;
  const pages: number[] = [];
  const { state, loader } = harness(async (page) => {
    pages.push(page);
    return {
      items:
        page === 0
          ? [
              { id: "a", tokens },
              { id: "b", tokens: 2 },
            ]
          : [{ id: "c", tokens: 1 }],
    };
  });
  t.after(() => loader.cancel());
  await loader.load(0, true);
  await loader.load(1);
  loader.invalidate();
  loader.invalidate();
  loader.invalidate();
  t.mock.timers.tick(250);
  await setImmediate();
  assert.equal(state.rows[0].tokens, 13, "duplicate delivery must not add usage");
  assert.deepEqual(pages, [0, 1, 0, 1]);
  assert.deepEqual(
    state.rows.map((row) => row.id),
    ["a", "b", "c"]
  );
  assert.equal(state.page, 1);
  assert.equal(state.more, false, "the last page controls pagination, not the combined length");
  tokens = 18;
  loader.invalidate(); // Same reconciliation on SSE reconnect, after missed events.
  t.mock.timers.tick(250);
  await setImmediate();
  assert.equal(state.rows[0].tokens, 18);
});

test("a superseded response cannot overwrite the newer result even if fetch ignores abort", async () => {
  const old = deferred<PageResult<Row>>();
  const latest = deferred<PageResult<Row>>();
  const signals: AbortSignal[] = [];
  const { state, loader } = harness((_page, signal) => {
    signals.push(signal);
    return signals.length === 1 ? old.promise : latest.promise;
  });
  const first = loader.load(0, true);
  const second = loader.load(0, true);
  assert.equal(signals[0].aborted, true);
  latest.resolve({ items: [{ id: "a", tokens: 18 }] });
  await second;
  old.resolve({ items: [{ id: "a", tokens: 13 }] });
  await first;
  assert.deepEqual(state.rows, [{ id: "a", tokens: 18 }]);
  assert.deepEqual(state.errors, []);
  loader.cancel();
});

test("notifications during a fetch trigger a subsequent read", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const first = deferred<PageResult<Row>>();
  let calls = 0;
  const { state, loader } = harness(async () => {
    calls++;
    return calls === 1 ? first.promise : { items: [{ id: "a", tokens: 18 }] };
  });
  t.after(() => loader.cancel());
  const pending = loader.load(0, true);
  loader.invalidate();
  loader.invalidate();
  t.mock.timers.tick(1000);
  assert.equal(calls, 1, "refreshes must not overlap");
  first.resolve({ items: [{ id: "a", tokens: 13 }] });
  await pending;
  t.mock.timers.tick(250);
  await setImmediate();
  assert.equal(calls, 2);
  assert.equal(state.rows[0].tokens, 18);
});

test("loading another page does not discard a pending refresh of earlier rows", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let tokens = 13;
  const { state, loader } = harness(async (page) => ({
    items:
      page === 0
        ? [
            { id: "a", tokens },
            { id: "b", tokens: 2 },
          ]
        : [{ id: "c", tokens: 1 }],
  }));
  t.after(() => loader.cancel());
  await loader.load(0, true);
  tokens = 18;
  loader.invalidate();
  await loader.load(1);
  t.mock.timers.tick(250);
  await setImmediate();
  assert.equal(state.rows[0].tokens, 18);
  assert.equal(state.page, 1);
});

test("cancellation discards late responses and errors; a failed refresh preserves visible rows", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let fail = false;
  const { state, loader } = harness(async () => {
    if (fail) throw new Error("unavailable");
    return { items: [{ id: "a", tokens: 13 }] };
  });
  await loader.load(0, true);
  fail = true;
  loader.invalidate();
  t.mock.timers.tick(250);
  await setImmediate();
  assert.equal(state.rows[0].tokens, 13);
  assert.equal(state.errors.length, 1);
  assert.equal(state.fetching, false);
  loader.cancel();

  const late = deferred<PageResult<Row>>();
  const cancelled = harness(() => late.promise);
  const pending = cancelled.loader.load(0, true);
  cancelled.loader.cancel();
  late.reject(new Error("response from previous view"));
  await pending;
  assert.deepEqual(cancelled.state.errors, []);
  assert.deepEqual(cancelled.state.rows, []);
});
