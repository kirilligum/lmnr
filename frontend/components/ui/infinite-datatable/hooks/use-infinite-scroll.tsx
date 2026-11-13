"use client";

import { type DependencyList, useCallback, useEffect, useRef } from "react";
import { shallow } from "zustand/shallow";
import { useStoreWithEqualityFn } from "zustand/traditional";

import { createPagedLoader } from "@/lib/paged-loader";

import { useTableConfigStore } from "../model/table-config-store.tsx";
import { useTableStore } from "../model/table-store.tsx";

export interface InfiniteScrollOptions<TData> {
  fetchFn: (pageParam: number, signal?: AbortSignal) => Promise<{ items: TData[]; count?: number }>;
  enabled?: boolean;
  deps?: DependencyList;
}

export function useInfiniteScroll<TData>({ fetchFn, enabled = true, deps = [] }: InfiniteScrollOptions<TData>) {
  const store = useTableStore<TData>();
  // Auto-gate on view resolution; no-op for tables without views.
  const isViewLoading = useTableConfigStore((s) => s.isViewLoading);

  const { data, isFetching, isLoading, error, hasMore } = useStoreWithEqualityFn(
    store,
    (state) => ({
      data: state.data,
      isFetching: state.isFetching,
      isLoading: state.isLoading,
      error: state.error,
      hasMore: state.hasMore,
    }),
    shallow
  );

  const loader = useRef<ReturnType<typeof createPagedLoader<TData>> | null>(null);
  const fetchFnRef = useRef(fetchFn);
  useEffect(() => {
    fetchFnRef.current = fetchFn;
  }, [fetchFn]);
  const depsString = JSON.stringify(deps);

  const fetchNextPage = useCallback(() => {
    const state = store.getState();
    if (!state.isFetching && state.hasMore) void loader.current?.load(state.currentPage + 1);
  }, [store]);

  const refetch = useCallback(() => {
    void loader.current?.load(0, true);
  }, []);
  const refresh = useCallback(() => {
    loader.current?.invalidate();
  }, []);
  const updateData = useCallback(
    (updater: (prevData: TData[]) => TData[]) => {
      store.getState().setData(updater);
    },
    [store]
  );

  useEffect(() => {
    if (!enabled || isViewLoading) return;
    const instance = createPagedLoader<TData>({
      fetchPage: (page, signal) => fetchFnRef.current(page, signal),
      getCurrentPage: () => store.getState().currentPage,
      pageSize: store.getState().pageSize,
      onStart: (reset) => {
        const state = store.getState();
        if (reset) state.resetInfiniteScroll();
        state.setIsFetching(true);
        if (reset) state.setIsLoading(true);
      },
      onResult: (result, page, replace, more) => {
        const state = store.getState();
        if (replace) state.replaceData(result.items, result.count);
        else state.appendData(result.items, result.count);
        state.setCurrentPage(page);
        state.setHasMore(more);
      },
      onError: (error) => store.getState().setError(error),
      onIdle: () => {
        store.getState().setIsFetching(false);
        store.getState().setIsLoading(false);
      },
    });
    loader.current = instance;
    void instance.load(0, true);
    return () => {
      instance.cancel();
      if (loader.current === instance) loader.current = null;
    };
  }, [enabled, isViewLoading, depsString, store]);

  return {
    data,
    totalFetched: data.length,
    hasMore,
    isFetching,
    isLoading,
    error,
    fetchNextPage,
    refetch,
    refresh,
    updateData,
  };
}
