import { compact } from "lodash";
import { z } from "zod/v4";

import { type Filter } from "@/lib/actions/common/filters";
import { buildTimeRangeWithFill } from "@/lib/actions/common/query-builder";
import { executeQuery } from "@/lib/actions/sql";
import { GetTracesSchema } from "@/lib/actions/traces";
import { searchRequestSpans } from "@/lib/actions/traces/search-requests";
import { type TracesStatsDataPoint } from "@/lib/actions/traces/stats-types";
import {
  buildTracesStatsWhereConditions,
  generateEmptyTimeBuckets,
  parseCustomColumnsJson,
} from "@/lib/actions/traces/utils";
import { getTimeRange } from "@/lib/clickhouse/utils";
import { traceTimeColumn } from "@/lib/traces/time-filter";

export const GetTraceStatsSchema = GetTracesSchema.omit({
  pageNumber: true,
  pageSize: true,
}).extend({
  intervalValue: z.coerce.number().default(1),
  intervalUnit: z.enum(["minute", "hour", "day"]).default("hour"),
});

export type { TracesStatsDataPoint };

export async function getTraceStats(
  input: z.infer<typeof GetTraceStatsSchema>
): Promise<{ items: TracesStatsDataPoint[] }> {
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
    intervalValue,
    intervalUnit,
    customColumns: customColumnsJson,
  } = input;

  const filters: Filter[] = compact(inputFilters);

  const customColumns = parseCustomColumnsJson(customColumnsJson);

  const spanHits = await searchRequestSpans({
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
  const traceIds = [...new Set(spanHits.map((span) => span.trace_id))];

  if (search && traceIds?.length === 0) {
    const timeRange = getTimeRange(pastHours, startTime, endTime);
    const items = generateEmptyTimeBuckets(timeRange);
    return { items };
  }

  const { conditions: whereConditions, params: whereParams } = buildTracesStatsWhereConditions({
    traceType,
    traceIds,
    filters,
    customColumns,
  });

  const {
    condition: timeCondition,
    params: timeParams,
    fillFrom,
    fillTo,
  } = buildTimeRangeWithFill({
    startTime,
    endTime,
    pastHours,
    timeColumn: traceTimeColumn(timeField),
    intervalValue,
    intervalUnit,
  });

  const allConditions = [...whereConditions];
  if (timeCondition) {
    allConditions.push(timeCondition);
  }

  const withFillClause =
    fillFrom && fillTo
      ? `WITH FILL
    FROM ${fillFrom}
    TO ${fillTo}
    STEP toInterval({intervalValue:UInt32}, {intervalUnit:String})`
      : "";

  const query = `
    SELECT 
      toStartOfInterval(${traceTimeColumn(timeField)}, toInterval({intervalValue:UInt32}, {intervalUnit:String})) as timestamp,
      countIf(status != 'error') as successCount,
      countIf(status = 'error') as errorCount
    FROM traces
    WHERE ${allConditions.join(" AND ")}
    GROUP BY timestamp
    ORDER BY timestamp ASC
    ${withFillClause}
  `;

  const parameters = {
    ...whereParams,
    ...timeParams,
    intervalValue,
    intervalUnit,
  };

  const items = await executeQuery<TracesStatsDataPoint>({
    query,
    parameters,
    projectId,
  });

  return { items };
}
