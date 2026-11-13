import { executeQuery } from "@/lib/actions/sql";
import { searchSpans, type SpanSearchHit } from "@/lib/actions/traces/search";
import { type BuildTracesIdsQueryOptions, buildTracesIdsQueryWithParams } from "@/lib/actions/traces/utils";
import { type SpanSearchType } from "@/lib/clickhouse/types";
import { getTimeRange } from "@/lib/clickhouse/utils";

type RequestSearchOptions = BuildTracesIdsQueryOptions & {
  projectId: string;
  search?: string | null;
  searchIn: string[];
  getSnippets?: boolean;
};

export async function searchRequestSpans(options: RequestSearchOptions): Promise<SpanSearchHit[]> {
  const { projectId, search, searchIn, startTime, endTime, pastHours, timeField, getSnippets } = options;
  if (!search) return [];

  let traceIds: string[] | undefined;
  if (timeField === "activity") {
    // Select active requests first, then search their entire history. A recent
    // tool result must not hide the human input submitted hours earlier.
    const { query, parameters } = buildTracesIdsQueryWithParams(options);
    const candidates = await executeQuery<{ id: string }>({ query, parameters, projectId });
    if (candidates.length === 0) return [];
    traceIds = candidates.map(({ id }) => id);
  }

  return searchSpans({
    projectId,
    traceIds,
    searchQuery: search,
    // Omitting a range invokes the backend's seven-day default. An active
    // request's original input can be older than that default.
    timeRange:
      timeField === "activity" ? { start: new Date(0), end: new Date() } : getTimeRange(pastHours, startTime, endTime),
    searchType: searchIn as SpanSearchType[],
    getSnippets,
  });
}
