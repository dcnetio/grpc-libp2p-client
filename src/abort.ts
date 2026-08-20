type AbortListener = EventListenerOrEventListenerObject;

interface AbortListenerRecord {
  listener: AbortListener;
  once: boolean;
}

type RuntimeGlobal = {
  AbortController?: typeof AbortController;
  DOMException?: typeof DOMException;
  Event?: typeof Event;
};

function runtimeGlobal(): RuntimeGlobal {
  if (typeof globalThis !== "undefined") return globalThis as RuntimeGlobal;
  if (typeof self !== "undefined") return self as RuntimeGlobal;
  if (typeof window !== "undefined") return window as RuntimeGlobal;
  return {};
}

function abortEvent(): Event {
  const EventConstructor = runtimeGlobal().Event;
  if (typeof EventConstructor === "function") {
    try {
      return new EventConstructor("abort");
    } catch {
      // Older WebViews may expose Event without a constructable constructor.
    }
  }
  return { type: "abort" } as Event;
}

function invokeAbortListener(listener: AbortListener, event: Event): void {
  if (typeof listener === "function") {
    listener(event);
  } else {
    listener.handleEvent(event);
  }
}

export function createAbortError(
  message: string,
  name = "AbortError",
): Error {
  const DOMExceptionConstructor = runtimeGlobal().DOMException;
  if (typeof DOMExceptionConstructor === "function") {
    return new DOMExceptionConstructor(message, name);
  }
  const error = new Error(message);
  error.name = name;
  return error;
}

/**
 * This package is also consumed independently of web-dc-api. Keep its
 * cancellation path self-contained for WebViews without AbortController.
 * Native browsers take the first branch and incur no compatibility overhead.
 */
export function createAbortController(): AbortController {
  const NativeAbortController = runtimeGlobal().AbortController;
  if (typeof NativeAbortController === "function") {
    return new NativeAbortController();
  }

  let aborted = false;
  let reason: unknown;
  let onabort: ((this: AbortSignal, event: Event) => unknown) | null = null;
  const listeners: AbortListenerRecord[] = [];
  const signal = {
    get aborted() {
      return aborted;
    },
    get reason() {
      return reason;
    },
    get onabort() {
      return onabort;
    },
    set onabort(listener: ((this: AbortSignal, event: Event) => unknown) | null) {
      onabort = listener;
    },
    addEventListener(
      type: string,
      listener: AbortListener | null,
      options?: boolean | AddEventListenerOptions,
    ) {
      if (type !== "abort" || !listener) return;
      if (listeners.some((record) => record.listener === listener)) return;
      listeners.push({
        listener,
        once: typeof options === "object" && options?.once === true,
      });
    },
    removeEventListener(type: string, listener: AbortListener | null) {
      if (type !== "abort" || !listener) return;
      const index = listeners.findIndex((record) => record.listener === listener);
      if (index !== -1) listeners.splice(index, 1);
    },
    dispatchEvent(event: Event) {
      if (event.type !== "abort") return true;
      for (const record of [...listeners]) {
        if (record.once) {
          const index = listeners.indexOf(record);
          if (index !== -1) listeners.splice(index, 1);
        }
        invokeAbortListener(record.listener, event);
      }
      onabort?.call(signal as AbortSignal, event);
      return true;
    },
    throwIfAborted() {
      if (aborted) {
        throw reason instanceof Error
          ? reason
          : createAbortError("Operation aborted");
      }
    },
  } as unknown as AbortSignal;

  return {
    signal,
    abort(nextReason?: unknown) {
      if (aborted) return;
      aborted = true;
      reason =
        nextReason === undefined
          ? createAbortError("Operation aborted")
          : nextReason;
      signal.dispatchEvent(abortEvent());
    },
  } as AbortController;
}
