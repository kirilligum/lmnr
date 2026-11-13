import { compact } from "lodash";
import { z } from "zod/v4";

import { type Filter } from "@/lib/actions/common/filters";
import { PaginationFiltersSchema, TimeRangeSchema } from "@/lib/actions/common/types";
import { executeQuery } from "@/lib/actions/sql";
import { searchSpans, type SpanSearchHit } from "@/lib/actions/traces/search";
import { searchRequestIds } from "@/lib/actions/traces/search-requests";
import {
  buildTracesCountQueryWithParams,
  buildTracesQueryWithParams,
  parseCustomColumnsJson,
} from "@/lib/actions/traces/utils";
import { clickhouseClient } from "@/lib/clickhouse/client.ts";
import { type SpanSearchType } from "@/lib/clickhouse/types";
import { type TraceRow } from "@/lib/traces/types.ts";

export const GetTracesSchema = PaginationFiltersSchema.extend({
  ...TimeRangeSchema.shape,
  projectId: z.guid(),
  traceType: z
    .enum(["DEFAULT", "EVALUATION", "EVENT", "PLAYGROUND"])
    .nullable()
    .optional()
    .transform((val) => val || "DEFAULT"),
  search: z.string().nullable().optional(),
  searchIn: z.array(z.string()).default([]),
  sortSql: z.string().optional(),
  customColumns: z.string().optional(),
  timeField: z.enum(["start", "activity"]).default("start"),
});

export const DeleteTracesSchema = z.object({
  projectId: z.guid(),
  traceIds: z.array(z.string()).min(1),
});

export async function getTraces(input: z.infer<typeof GetTracesSchema>): Promise<{ items: TraceRow[] }> {
  const {
    projectId,
    pastHours,
    timeField,
    startDate: startTime,
    endDate: endTime,
    pageNumber,
    pageSize,
    traceType,
    search,
    searchIn,
    filter: inputFilters,
    sortBy,
    sortSql,
    sortDirection,
    customColumns: customColumnsJson,
  } = input;

  const filters: Filter[] = compact(inputFilters);

  const limit = pageSize;
  const offset = Math.max(0, pageNumber * pageSize);

  const customColumns = parseCustomColumnsJson(customColumnsJson);
  const traceIds = await searchRequestIds({
    projectId,
    search,
    searchIn,
    startTime,
    endTime,
    pastHours,
    timeField,
    traceType,
    filters,
    customColumns,
  });

  if (search?.trim() && traceIds.length === 0) {
    return { items: [] };
  }

  const { query: mainQuery, parameters: mainParams } = buildTracesQueryWithParams({
    projectId,
    traceType,
    traceIds,
    filters,
    limit,
    offset,
    startTime,
    endTime,
    pastHours,
    timeField,
    sortBy,
    sortSql,
    sortDirection: sortDirection as "ASC" | "DESC" | undefined,
    customColumns,
  });

  const items = await executeQuery<TraceRow>({ query: mainQuery, parameters: mainParams, projectId });

  if (search?.trim() && items.length > 0) {
    // Snippets decorate the requested page. A span-hit cap cannot determine
    // which requests exist, their count, or their chart totals.
    const spanHits = await searchSpans({
      projectId,
      traceIds: items.map(({ id }) => id),
      searchQuery: search,
      searchType: searchIn as SpanSearchType[],
      timeRange: { start: new Date(0), end: new Date() },
      getSnippets: true,
    });

    const snippetMap = new Map<string, SpanSearchHit>();
    for (const hit of spanHits) {
      if (!snippetMap.has(hit.trace_id) && (hit.input_snippet || hit.output_snippet || hit.attributes_snippet)) {
        snippetMap.set(hit.trace_id, hit);
      }
    }
    for (const item of items) {
      const hit = snippetMap.get(item.id);
      if (hit) {
        item.inputSnippet = hit.input_snippet;
        item.outputSnippet = hit.output_snippet;
        item.attributesSnippet = hit.attributes_snippet;
      }
    }
  }

  return {
    items,
  };
}

export async function countTraces(input: z.infer<typeof GetTracesSchema>): Promise<{ count: number }> {
  const {
    projectId,
    pastHours,
    timeField,
    startDate: startTime,
    endDate: endTime,
    traceType,
    search,
    searchIn,
    filter: inputFilters,
    customColumns: customColumnsJson,
  } = input;

  const filters: Filter[] = compact(inputFilters);

  const customColumns = parseCustomColumnsJson(customColumnsJson);

  const traceIds = await searchRequestIds({
    projectId,
    search,
    searchIn,
    startTime,
    endTime,
    pastHours,
    timeField,
    traceType,
    filters,
    customColumns,
  });

  if (search?.trim() && traceIds.length === 0) {
    return { count: 0 };
  }

  const { query: countQuery, parameters: countParams } = buildTracesCountQueryWithParams({
    projectId,
    traceType,
    traceIds,
    filters,
    startTime,
    endTime,
    pastHours,
    timeField,
    customColumns,
  });

  const result = await executeQuery<{ count: number }>({
    query: countQuery,
    parameters: countParams,
    projectId,
  });

  return {
    count: result[0]?.count ?? 0,
  };
}

export async function deleteTraces(input: z.infer<typeof DeleteTracesSchema>) {
  const { projectId, traceIds } = input;

  await clickhouseClient.command({
    query: `
        DELETE FROM spans
        WHERE project_id = {projectId: UUID} 
            AND trace_id in ({traceIds: Array(UUID)})
      `,
    query_params: {
      traceIds,
      projectId,
    },
  });
}
