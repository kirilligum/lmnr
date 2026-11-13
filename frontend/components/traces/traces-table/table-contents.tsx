"use client";

import { type Row } from "@tanstack/react-table";
import { debounce, isEmpty, map } from "lodash";
import { useParams, usePathname, useSearchParams } from "next/navigation";
import { memo, type PropsWithChildren, type RefObject, useCallback, useEffect, useMemo } from "react";
import { useSWRConfig } from "swr";

import { useTraceViewNavigation } from "@/components/traces/trace-view/navigation-context";
import { useTracesStoreContext } from "@/components/traces/traces-store";
import { FETCH_SIZE } from "@/components/traces/traces-table/constants";
import { TracesEmptyRow } from "@/components/traces/traces-table/empty-row";
import { type buildColumnDefs, buildFetchParams } from "@/components/traces/traces-table/traces-table-store";
import { InfiniteDataTable } from "@/components/ui/infinite-datatable";
import { useInfiniteScroll } from "@/components/ui/infinite-datatable/hooks";
import { useLocalStorage } from "@/hooks/use-local-storage.tsx";
import { fetchApi } from "@/lib/api/fetch-api";
import { useRealtime } from "@/lib/hooks/use-realtime";
import { useToast } from "@/lib/hooks/use-toast";
import { type TraceTimeField } from "@/lib/traces/time-filter";
import { type TraceRow } from "@/lib/traces/types";

export interface TracesTableContentsProps {
  refetchRef: RefObject<() => void>;
  onRefreshStats: () => void;
  columnDefs: ReturnType<typeof buildColumnDefs>;
  effectiveColumns: ReturnType<typeof buildColumnDefs>;
  pinnedColumns: string[];
  columnSqls: (string | undefined)[];
  filter: string[];
  textSearchFilter: string | null;
  sortBy?: string;
  sortDirection?: "asc" | "desc";
  onSort: (columnId: string, direction: "asc" | "desc") => void;
  onClearFilters: () => void;
  pastHours: string | null;
  timeField: TraceTimeField;
  startDate: string | null;
  endDate: string | null;
  searchIn: string[];
  isViewLoading: boolean;
}

export const TracesTableContents = memo(function TracesTableContents({
  children,
  refetchRef,
  onRefreshStats,
  columnDefs,
  effectiveColumns,
  pinnedColumns,
  columnSqls,
  filter,
  textSearchFilter,
  sortBy,
  sortDirection,
  onSort,
  onClearFilters,
  pastHours,
  timeField,
  startDate,
  endDate,
  searchIn,
  isViewLoading,
}: PropsWithChildren<TracesTableContentsProps>) {
  const searchParams = useSearchParams();
  const pathName = usePathname();
  const { projectId } = useParams();
  const { toast } = useToast();
  const { mutate: globalMutate } = useSWRConfig();

  const [realtimeEnabled] = useLocalStorage("traces-table:realtime", true);

  const traceId = useTracesStoreContext((s) => s.traceId);
  const onRowClick = useTracesStoreContext((s) => s.setTraceId);

  const { setNavigationRefList } = useTraceViewNavigation();
  const isCurrentTimestampIncluded = !!pastHours || (!!endDate && new Date(endDate) >= new Date());

  const fetchTraces = useCallback(
    async (pageNumber: number, signal?: AbortSignal) => {
      try {
        const urlParams = buildFetchParams(
          {
            pageNumber,
            pageSize: FETCH_SIZE,
            filter,
            sortBy: sortBy ?? null,
            sortDirection: sortDirection?.toUpperCase() as string | null,
          },
          columnDefs
        );

        if (pastHours != null) urlParams.set("pastHours", pastHours);
        urlParams.set("timeField", timeField);
        if (startDate != null) urlParams.set("startDate", startDate);
        if (endDate != null) urlParams.set("endDate", endDate);

        if (typeof textSearchFilter === "string" && textSearchFilter.length > 0) {
          urlParams.set("search", textSearchFilter);
        }

        if (isEmpty(searchIn) || searchIn?.length === 2) {
          urlParams.append("searchIn", "input");
          urlParams.append("searchIn", "output");
        } else if (searchIn?.length > 0) {
          urlParams.set("searchIn", searchIn[0]);
        }

        const url = `/api/projects/${projectId}/traces?${urlParams.toString()}`;

        const { data, error } = await fetchApi<{ items: TraceRow[] }>(url, { signal });
        if (error !== null) throw new Error(error);
        if (signal?.aborted) throw new DOMException("Aborted", "AbortError");

        data.items.map((trace) =>
          globalMutate(`/api/projects/${projectId}/traces/${trace.id}/tags`, trace.traceTags ?? [], {
            revalidate: false,
          })
        );

        return { items: data.items, count: 0 };
      } catch (error) {
        if (signal?.aborted) throw error;
        toast({
          title: "Error",
          description: error instanceof Error ? error.message : "Failed to load traces. Please try again.",
          variant: "destructive",
        });
        throw error;
      }
    },
    [
      columnDefs,
      endDate,
      filter,
      pastHours,
      timeField,
      projectId,
      searchIn,
      sortBy,
      sortDirection,
      startDate,
      textSearchFilter,
      toast,
      globalMutate,
    ]
  );

  const {
    data: traces,
    hasMore,
    isFetching,
    isLoading,
    fetchNextPage,
    refetch,
    refresh,
  } = useInfiniteScroll<TraceRow>({
    fetchFn: fetchTraces,
    enabled: !!(pastHours || (startDate && endDate)),
    deps: [
      endDate,
      filter,
      pastHours,
      timeField,
      projectId,
      searchIn,
      sortBy,
      sortDirection,
      startDate,
      textSearchFilter,
      columnSqls,
    ],
  });

  useEffect(() => {
    refetchRef.current = refetch;
  }, [refetch, refetchRef]);

  useEffect(() => {
    setNavigationRefList(map(traces, "id"));
  }, [setNavigationRefList, traces]);

  const refreshStats = useMemo(() => debounce(onRefreshStats, 500, { maxWait: 1000 }), [onRefreshStats]);
  useEffect(() => () => refreshStats.cancel(), [refreshStats]);
  const reconcile = useCallback(() => {
    refresh();
    refreshStats();
  }, [refresh, refreshStats]);
  const eventHandlers = useMemo(
    () => ({
      trace_update: reconcile,
      trace_agent_input_update: reconcile,
    }),
    [reconcile]
  );

  useRealtime({
    key: "traces",
    projectId: projectId as string,
    enabled: realtimeEnabled && filter.length === 0 && !textSearchFilter && isCurrentTimestampIncluded,
    eventHandlers,
    onConnect: reconcile,
  });

  const handleRowClick = useCallback(
    (row: Row<TraceRow>) => {
      onRowClick?.(row.id);
    },
    [onRowClick]
  );

  const getRowHref = useCallback(
    (row: Row<TraceRow>) => {
      const params = new URLSearchParams(searchParams.toString());
      params.set("traceId", row.id);
      params.delete("spanId");
      return `${pathName}?${params.toString()}`;
    },
    [pathName, searchParams]
  );

  return (
    <InfiniteDataTable<TraceRow>
      className="w-full"
      columns={effectiveColumns}
      data={traces}
      getRowId={(trace) => trace.id}
      onRowClick={handleRowClick}
      focusedRowId={traceId || searchParams.get("traceId")}
      hasMore={!textSearchFilter && hasMore}
      isFetching={isFetching}
      isLoading={isLoading || isViewLoading}
      fetchNextPage={fetchNextPage}
      getRowHref={getRowHref}
      emptyRow={<TracesEmptyRow hasFilters={filter.length > 0 || !!textSearchFilter} onClearFilters={onClearFilters} />}
      pinnedColumns={pinnedColumns}
      sortBy={sortBy}
      sortDirection={sortDirection}
      onSort={onSort}
    >
      {children}
    </InfiniteDataTable>
  );
});
