import { executeQuery } from "@/lib/actions/sql";
import { resolveSqlActor } from "@/lib/actions/sql/actor";
import { type BuildTracesIdsQueryOptions, buildTracesIdsQueryWithParams } from "@/lib/actions/traces/utils";
import { callAppServer } from "@/lib/api/backend";

type RequestSearchOptions = BuildTracesIdsQueryOptions & {
  projectId: string;
  search?: string | null;
  searchIn: string[];
};

const CANDIDATE_PAGE_SIZE = 256;

export async function searchRequestIds(options: RequestSearchOptions): Promise<string[]> {
  const search = options.search?.trim();
  if (!search) return [];

  const actor = await resolveSqlActor();
  const matches: string[] = [];
  let cursor: string | null = null;
  const end = new Date().toISOString();
  const candidateOptions = options.pastHours
    ? {
        ...options,
        pastHours: undefined,
        startTime: new Date(Date.parse(end) - Number(options.pastHours) * 3_600_000).toISOString(),
        endTime: end,
      }
    : options;
  for (;;) {
    // Identity pagination avoids materializing every candidate or sending an
    // unbounded IN clause to Quickwit. Activity changes cannot reorder this scan.
    const { query, parameters } = buildTracesIdsQueryWithParams({
      ...candidateOptions,
      cursor,
      limit: CANDIDATE_PAGE_SIZE,
    });
    const candidates = await executeQuery<{ id: string }>({ query, parameters, projectId: options.projectId });
    if (candidates.length === 0) break;
    const ids = await callAppServer<string[]>(`/projects/${options.projectId}/traces/search`, {
      method: "POST",
      body: {
        traceIds: candidates.map(({ id }) => id),
        searchQuery: search,
        searchIn: options.searchIn,
        // The window selects requests; matching examines their full retained
        // history, including human input older than the latest activity.
        startTime: new Date(0).toISOString(),
        endTime: end,
        actor,
      },
    });
    matches.push(...ids);
    if (candidates.length < CANDIDATE_PAGE_SIZE) break;
    const next = candidates.at(-1)!.id;
    if (next === cursor) throw new Error("Request search did not advance");
    cursor = next;
  }
  return matches;
}
