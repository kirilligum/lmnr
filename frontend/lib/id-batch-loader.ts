export interface IdBatchLoaderOptions<T> {
  // Max ids per fetchBatch call (server request cap).
  batchSize: number;
  // Debounce window to coalesce rapid enqueues (e.g. scroll).
  debounceMs: number;
  // Fetch one chunk (≤ batchSize ids); returns the items found.
  fetchBatch: (ids: string[], signal: AbortSignal) => Promise<T[]>;
  // Stable id for an item, used to key the per-chunk result map.
  getId: (item: T) => string;
  // One successful chunk: the ids requested + found items keyed by id (misses absent).
  onBatch: (requestedIds: string[], itemsById: Map<string, T>) => void;
  // One failed chunk (fetchBatch threw).
  onError: (requestedIds: string[]) => void;
}

export interface IdBatchLoader {
  // Queue ids; coalesced + debounced, then chunked into fetchBatch calls. The
  // caller owns dedupe — the loader has no memory across flushes.
  load: (ids: string[]) => void;
  // Drop pending work and abort in-flight reads (teardown).
  cancel: () => void;
}

/**
 * Window-driven batch id loader: accumulates ids, debounces, chunks to
 * `batchSize`, and reports each chunk's result via `onBatch` / `onError`.
 * Isolates the scheduling + chunking plumbing so callers keep only their own
 * merge/state logic.
 */
export function createIdBatchLoader<T>(options: IdBatchLoaderOptions<T>): IdBatchLoader {
  const { batchSize, debounceMs, fetchBatch, getId, onBatch, onError } = options;
  const pending = new Set<string>();
  let timer: ReturnType<typeof setTimeout> | null = null;
  let active: AbortController | null = null;

  const schedule = () => {
    if (active || timer || pending.size === 0) return;
    timer = setTimeout(() => void flush(), debounceMs);
  };

  const flush = async () => {
    timer = null;
    const controller = new AbortController();
    active = controller;
    const ids = [...pending];
    pending.clear();
    try {
      for (let i = 0; i < ids.length; i += batchSize) {
        const chunk = ids.slice(i, i + batchSize);
        try {
          const items = await fetchBatch(chunk, controller.signal);
          if (controller.signal.aborted) return;
          onBatch(chunk, new Map(items.map((item) => [getId(item), item])));
        } catch {
          if (controller.signal.aborted) return;
          onError(chunk);
        }
      }
    } finally {
      if (active === controller) {
        active = null;
        schedule();
      }
    }
  };

  return {
    load: (ids) => {
      if (ids.length === 0) return;
      for (const id of ids) pending.add(id);
      schedule();
    },
    cancel: () => {
      if (timer) clearTimeout(timer);
      timer = null;
      pending.clear();
      active?.abort();
      active = null;
    },
  };
}
