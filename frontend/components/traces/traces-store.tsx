import { createContext, type PropsWithChildren, useContext, useState } from "react";
import { createStore } from "zustand";
import { useStoreWithEqualityFn } from "zustand/traditional";

import { type TracesStatsDataPoint } from "@/lib/actions/traces/stats";
import { fetchApi } from "@/lib/api/fetch-api";
import { toast } from "@/lib/hooks/use-toast";

type TracesState = {
  traceId: string | null;
  spanId: string | null;
  stats?: TracesStatsDataPoint[];
  isLoadingStats: boolean;
  chartContainerWidth: number | null;
};

type TracesActions = {
  setTraceId: (traceId: string | null) => void;
  setSpanId: (spanId: string | null) => void;
  fetchStats: (url: string, background?: boolean) => Promise<void>;
  setChartContainerWidth: (width: number) => void;
};

export interface TracesProps {
  traceId: string | null;
  spanId: string | null;
}

export type TracesStore = TracesState & TracesActions;

type TracesStoreApi = ReturnType<typeof createTracesStore>;

const createTracesStore = (initProps?: Partial<TracesProps>) => {
  const DEFAULT_PROPS: TracesState = {
    traceId: null,
    spanId: null,
    stats: undefined,
    isLoadingStats: false,
    chartContainerWidth: null,
  };

  let statsRequest: AbortController | undefined;
  return createStore<TracesStore>()((set) => ({
    ...DEFAULT_PROPS,
    ...initProps,

    setTraceId: (traceId) => set({ traceId }),

    setSpanId: (spanId: string | null) => set({ spanId }),

    setChartContainerWidth: (width: number) => set({ chartContainerWidth: width }),

    fetchStats: async (url, background = false) => {
      statsRequest?.abort();
      const controller = new AbortController();
      statsRequest = controller;
      if (!background) set({ isLoadingStats: true });
      try {
        const { data, error } = await fetchApi<{ items: TracesStatsDataPoint[] }>(url, { signal: controller.signal });
        if (controller.signal.aborted) return;
        if (error !== null) throw new Error(error);
        set({ stats: data.items });
      } catch (error) {
        if (!controller.signal.aborted) {
          toast({
            title: "Failed to load trace statistics",
            description: error instanceof Error ? error.message : String(error),
            variant: "destructive",
          });
        }
      } finally {
        if (statsRequest === controller) {
          statsRequest = undefined;
          set({ isLoadingStats: false });
        }
      }
    },
  }));
};

const TracesContext = createContext<TracesStoreApi | null>(null);

export const useTracesStoreContext = <T,>(
  selector: (state: TracesStore) => T,
  equalityFn?: (a: T, b: T) => boolean
): T => {
  const store = useContext(TracesContext);
  if (!store) throw new Error("Missing TracesContext.Provider in the tree");
  return useStoreWithEqualityFn(store, selector, equalityFn);
};

export const TracesStoreProvider = ({ children, ...props }: PropsWithChildren<TracesProps>) => {
  const [storeState] = useState(() => createTracesStore(props));

  return <TracesContext.Provider value={storeState}>{children}</TracesContext.Provider>;
};
