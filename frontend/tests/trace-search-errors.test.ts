import assert from "node:assert/strict";
import { registerHooks } from "node:module";
import { after, afterEach, before, test } from "node:test";

// Keep the real search action, transport and error conversion. Only the
// request-scoped session lookup is replaced; no Next server or database is used.
const hooks = registerHooks({
  resolve(specifier, context, nextResolve) {
    if (specifier === "@/lib/auth-session") {
      return {
        url: "test:search-session",
        format: "commonjs",
        shortCircuit: true,
      };
    }
    return nextResolve(specifier, context);
  },
  load(url, context, nextLoad) {
    if (url === "test:search-session") {
      return {
        format: "commonjs",
        source: 'exports.getServerSession = async () => ({ user: { id: "00000000-0000-0000-0000-000000000001" } });',
        shortCircuit: true,
      };
    }
    return nextLoad(url, context);
  },
});
let searchSpans: typeof import("@/lib/actions/traces/search").searchSpans;
let searchRequestIds: typeof import("@/lib/actions/traces/search-requests").searchRequestIds;
let traces: typeof import("@/lib/actions/traces");
let traceStats: typeof import("@/lib/actions/traces/stats");
before(async () => {
  ({ searchSpans } = await import("@/lib/actions/traces/search.ts"));
  ({ searchRequestIds } = await import("@/lib/actions/traces/search-requests.ts"));
  traces = await import("@/lib/actions/traces/index.ts");
  traceStats = await import("@/lib/actions/traces/stats.ts");
});
after(() => hooks.deregister());

const originalFetch = globalThis.fetch;
afterEach(() => {
  globalThis.fetch = originalFetch;
});
const options = { projectId: "00000000-0000-0000-0000-000000000002", searchQuery: "human request" };

test("search errors cannot become a successful empty result", async () => {
  for (const status of [400, 403, 503]) {
    globalThis.fetch = async () => Response.json({ error: "Search unavailable" }, { status });
    await assert.rejects(searchSpans(options), /Search unavailable/);
  }
  globalThis.fetch = async () => {
    throw new TypeError("fetch failed");
  };
  await assert.rejects(searchSpans(options), /fetch failed/);
});

test("successful empty searches stay empty and retain the server actor", async () => {
  globalThis.fetch = async (_url, init) => {
    const body = JSON.parse(String(init?.body));
    assert.deepEqual(body.actor, { type: "user", userId: "00000000-0000-0000-0000-000000000001" });
    assert.equal(body.searchQuery, options.searchQuery);
    return Response.json([]);
  };
  assert.deepEqual(await searchSpans(options), []);
});

test("blank searches do not contact the backend", async () => {
  globalThis.fetch = async () => {
    assert.fail("blank search must not issue a request");
  };
  assert.deepEqual(await searchSpans({ ...options, searchQuery: "  " }), []);
});

test("request discovery scans bounded identity pages and searches the entire request history", async () => {
  const ids = Array.from({ length: 513 }, (_, i) => `00000000-0000-4000-8000-${i.toString(16).padStart(12, "0")}`);
  const matching = [ids[0], ids[256], ids[512]];
  const batches: number[] = [];
  const starts: string[] = [];
  globalThis.fetch = async (url, init) => {
    const body = JSON.parse(String(init?.body));
    if (String(url).endsWith("/sql/query")) {
      assert.match(body.query, /ORDER BY id ASC/);
      assert.equal(body.parameters.limit, 256);
      assert.equal(body.parameters.pastHours, undefined);
      starts.push(body.parameters.startTime);
      const offset = body.parameters.cursor ? ids.indexOf(body.parameters.cursor) + 1 : 0;
      return Response.json(ids.slice(offset, offset + body.parameters.limit).map((id) => ({ id })));
    }
    assert.ok(String(url).endsWith("/traces/search"));
    assert.ok(body.traceIds.length <= 256);
    assert.equal(body.startTime, "1970-01-01T00:00:00.000Z");
    assert.deepEqual(body.searchIn, ["input"]);
    batches.push(body.traceIds.length);
    return Response.json(matching.filter((id) => body.traceIds.includes(id)));
  };
  assert.deepEqual(
    await searchRequestIds({
      projectId: options.projectId,
      search: "original human input",
      searchIn: ["input"],
      traceType: "DEFAULT",
      filters: [],
      timeField: "activity",
      pastHours: "1",
    }),
    matching
  );
  assert.deepEqual(batches, [256, 256, 1]);
  assert.equal(new Set(starts).size, 1, "the activity window must not slide between pages");
});

test("request discovery cannot return earlier matches after a later batch fails", async () => {
  const ids = Array.from({ length: 256 }, (_, i) => `00000000-0000-4000-8000-${i.toString(16).padStart(12, "0")}`);
  let searches = 0;
  globalThis.fetch = async (url, init) => {
    const body = JSON.parse(String(init?.body));
    if (String(url).endsWith("/sql/query")) {
      return Response.json(
        (body.parameters.cursor ? ["ffffffff-ffff-4fff-8fff-ffffffffffff"] : ids).map((id) => ({ id }))
      );
    }
    searches++;
    return searches === 1 ? Response.json([ids[0]]) : Response.json({ error: "Incomplete search" }, { status: 503 });
  };
  await assert.rejects(
    searchRequestIds({
      projectId: options.projectId,
      search: "original human input",
      searchIn: [],
      traceType: "DEFAULT",
      filters: [],
      timeField: "activity",
      pastHours: "1",
    }),
    /Incomplete search/
  );
  assert.equal(searches, 2);
});

test("list pagination, count and charts include matching requests beyond the old 500-span cap", async () => {
  const ids = Array.from({ length: 513 }, (_, i) => `00000000-0000-4000-8000-${i.toString(16).padStart(12, "0")}`);
  const mainQueries: { limit: number; offset: number }[] = [];
  globalThis.fetch = async (url, init) => {
    const body = JSON.parse(String(init?.body));
    if (String(url).endsWith("/traces/search")) return Response.json(body.traceIds);
    if (String(url).endsWith("/spans/search")) {
      assert.deepEqual(body.traceIds, ids.slice(50, 100), "snippets must be restricted to the displayed page");
      return Response.json([
        { trace_id: ids[50], span_id: "one-preview", input_snippet: { text: "human request", highlight: [0, 5] } },
      ]);
    }
    assert.ok(String(url).endsWith("/sql/query"));
    const { query, parameters } = body;
    if (parameters.limit === 256) {
      const offset = parameters.cursor ? ids.indexOf(parameters.cursor) + 1 : 0;
      return Response.json(ids.slice(offset, offset + 256).map((id) => ({ id })));
    }
    assert.deepEqual(parameters.traceIds, ids);
    if (query.includes("GROUP BY timestamp"))
      return Response.json([{ timestamp: "2026-01-01T00:00:00Z", successCount: ids.length, errorCount: 0 }]);
    if (query.includes("count() as count")) return Response.json([{ count: ids.length }]);
    mainQueries.push({ limit: parameters.limit, offset: parameters.offset });
    return Response.json(ids.slice(parameters.offset, parameters.offset + parameters.limit).map((id) => ({ id })));
  };
  const raw = {
    projectId: options.projectId,
    search: "human request",
    searchIn: ["input"],
    pastHours: "24",
    pageSize: "50",
    pageNumber: "1",
    timeField: "activity",
  };
  const input = traces.GetTracesSchema.parse(raw);
  const rows = await traces.getTraces(input);
  assert.deepEqual(
    rows.items.map(({ id }) => id),
    ids.slice(50, 100)
  );
  assert.deepEqual(mainQueries, [{ limit: 50, offset: 50 }]);
  assert.equal(rows.items[0].inputSnippet?.text, "human request");
  assert.equal(rows.items[0].snippetsCount, undefined, "a capped snippet sample is not a match count");
  assert.deepEqual(await traces.countTraces(input), { count: 513 });
  const stats = await traceStats.getTraceStats(traceStats.GetTraceStatsSchema.parse(raw));
  assert.equal(stats.items[0].successCount, 513);
});
