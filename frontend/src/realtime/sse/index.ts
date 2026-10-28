import { ApiError, type MachineApi } from "../../api/client";
import { MAX_BODY, rawText } from "../../api/codec";
import { emptyLive, mergeLive } from "../reducers";
import type { LiveEvent, LiveSnapshot } from "../../types/live";

export type Connection = "CONNECTED" | "RECONNECTING" | "OFFLINE";
type Callbacks = {
  state: (value: LiveSnapshot) => void;
  connection: (value: Connection) => void;
  error: (error: unknown) => void;
};
function pause(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const finish = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timer = setTimeout(finish, ms);
    signal.addEventListener("abort", finish, { once: true });
    if (signal.aborted) finish();
  });
}
export function watchMachine(
  api: MachineApi,
  machine: string,
  callbacks: Callbacks,
): () => void {
  const lifetime = new AbortController();
  let current = emptyLive(machine),
    resync: () => Promise<void> = async () => {};
  const visible = () =>
    typeof document === "undefined" || document.visibilityState === "visible";
  const refresh = () => {
    if (visible())
      void resync().catch((error) => {
        if (!lifetime.signal.aborted) callbacks.error(error);
      });
  };
  const timer = setInterval(refresh, 30000);
  if (typeof document !== "undefined")
    document.addEventListener("visibilitychange", refresh);
  async function run() {
    let failures = 0;
    while (!lifetime.signal.aborted) {
      const attempt = new AbortController(),
        signal = AbortSignal.any([lifetime.signal, attempt.signal]);
      let boot: Promise<void> | undefined,
        bootError: unknown,
        booting = true,
        buffer: LiveEvent[] = [],
        size = 0;
      const apply = (value: LiveSnapshot | LiveEvent) => {
        if (signal.aborted) return;
        current = mergeLive(current, value);
        callbacks.state(current);
      };
      let inFlight: Promise<void> | undefined;
      resync = () =>
        (inFlight ??= (async () => {
          try {
            const value = await api.live(machine, signal);
            apply(value);
          } finally {
            inFlight = undefined;
          }
        })());
      callbacks.connection(failures >= 3 ? "OFFLINE" : "RECONNECTING");
      try {
        await api.stream(
          machine,
          signal,
          () => {
            callbacks.connection("CONNECTED");
            boot = (async () => {
              await resync();
              if (signal.aborted) return;
              booting = false;
              for (const value of buffer) apply(value);
              buffer = [];
              size = 0;
              failures = 0;
            })().catch((error) => {
              bootError = error;
              attempt.abort();
            });
          },
          (value) => {
            if (booting) {
              size += new TextEncoder().encode(rawText(value)).length;
              if (size > MAX_BODY || buffer.length >= 128)
                throw new Error("Bootstrap buffer exceeds budget");
              buffer.push(value);
            } else apply(value);
          },
        );
        if (boot) await boot;
        throw new ApiError(503, "STREAM_DISCONNECTED", true);
      } catch (caught) {
        if (lifetime.signal.aborted) break;
        const error = bootError ?? caught;
        callbacks.error(error);
        failures++;
        callbacks.connection(failures >= 3 ? "OFFLINE" : "RECONNECTING");
        if (
          error instanceof ApiError &&
          (!error.retryable || [401, 403, 404].includes(error.status))
        ) {
          if ([401, 403].includes(error.status)) {
            current = emptyLive(machine);
            callbacks.state(current);
          }
          callbacks.connection("OFFLINE");
          break;
        }
      } finally {
        attempt.abort();
        if (boot) await boot;
        resync = async () => {};
      }
      const delay = Math.min(30000, 1000 * 2 ** Math.min(failures - 1, 5));
      await pause(delay * (0.9 + Math.random() * 0.2), lifetime.signal);
    }
  }
  void run().catch((error) => {
    if (!lifetime.signal.aborted) {
      callbacks.error(error);
      callbacks.connection("OFFLINE");
    }
  });
  return () => {
    lifetime.abort();
    clearInterval(timer);
    if (typeof document !== "undefined")
      document.removeEventListener("visibilitychange", refresh);
  };
}
