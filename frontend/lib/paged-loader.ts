export interface PageResult<T> {
  items: T[];
  count?: number;
}

interface PagedLoaderOptions<T> {
  fetchPage: (page: number, signal: AbortSignal) => Promise<PageResult<T>>;
  getCurrentPage: () => number;
  pageSize: number;
  onStart: (reset: boolean) => void;
  onResult: (result: PageResult<T>, page: number, replace: boolean, hasMore: boolean) => void;
  onError: (error: Error) => void;
  onIdle: () => void;
  debounceMs?: number;
}

/** Reconcile a loaded page window from authoritative rows. Notifications carry
 * no accounting state; an event during a fetch schedules another reconciliation.
 */
export function createPagedLoader<T>(options: PagedLoaderOptions<T>) {
  let active: AbortController | undefined;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let dirty = false;
  let disposed = false;

  const schedule = () => {
    if (disposed || active || timer !== undefined || !dirty) return;
    timer = setTimeout(() => {
      timer = undefined;
      dirty = false;
      void run(options.getCurrentPage(), false, true);
    }, options.debounceMs ?? 250);
  };

  const run = async (page: number, reset: boolean, refresh: boolean) => {
    const controller = new AbortController();
    active = controller;
    options.onStart(reset);
    try {
      let result: PageResult<T> = { items: [] };
      let lastPage = page;
      let hasMore = true;
      for (let current = refresh ? 0 : page; current <= page; current++) {
        const fetched = await options.fetchPage(current, controller.signal);
        if (controller.signal.aborted) return;
        result = { items: [...result.items, ...fetched.items], count: fetched.count };
        lastPage = current;
        hasMore = fetched.items.length >= options.pageSize;
        if (!hasMore) break;
      }
      options.onResult(result, lastPage, reset || refresh, hasMore);
    } catch (error) {
      if (!controller.signal.aborted) {
        options.onError(error instanceof Error ? error : new Error("Failed to fetch data"));
      }
    } finally {
      if (active === controller) {
        active = undefined;
        options.onIdle();
        schedule();
      }
    }
  };

  return {
    load: (page: number, reset = false) => {
      if (disposed) return Promise.resolve();
      active?.abort();
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
      if (reset) dirty = false;
      return run(page, reset, false);
    },
    invalidate: () => {
      if (disposed) return;
      dirty = true;
      schedule();
    },
    cancel: () => {
      disposed = true;
      active?.abort();
      active = undefined;
      if (timer !== undefined) clearTimeout(timer);
      timer = undefined;
      dirty = false;
      options.onIdle();
    },
  };
}
