import {
  decodeSSE,
  MAX_BODY,
  parseLiveSnapshot,
  parseMachineDetail,
  parseMachinePage,
} from "./codec";
import type {
  LiveEvent,
  LiveSnapshot,
  MachineDetail,
  MachinePage,
} from "../types/live";

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    readonly retryable = false,
  ) {
    super(
      status === 401
        ? "Phiên đăng nhập đã hết hạn. Vui lòng đăng nhập lại."
        : status === 403
          ? "Bạn không có quyền xem dữ liệu này."
          : status === 404
            ? "Không tìm thấy máy."
            : "Không thể tải dữ liệu. Vui lòng thử lại.",
    );
  }
}
export interface MachineApi {
  live(machine: string, signal: AbortSignal): Promise<LiveSnapshot>;
  detail(machine: string, signal: AbortSignal): Promise<MachineDetail>;
  fleet(
    state: string,
    offset: number,
    signal: AbortSignal,
  ): Promise<MachinePage>;
  stream(
    machine: string,
    signal: AbortSignal,
    ready: () => void,
    event: (value: LiveEvent) => void,
  ): Promise<void>;
}
export function pathSegment(value: string): string {
  if (value === "." || value === "..")
    throw new Error("Định danh này chưa được hỗ trợ trên trình duyệt.");
  return encodeURIComponent(value);
}
export async function boundedText(response: Response): Promise<string> {
  if (!response.body) throw new Error("Empty response body");
  const reader = response.body.getReader(),
    decoder = new TextDecoder("utf-8", { fatal: true });
  let size = 0,
    text = "";
  try {
    while (true) {
      const part = await reader.read();
      if (part.done) break;
      size += part.value.byteLength;
      if (size > MAX_BODY) throw new Error("Response exceeds decoding budget");
      text += decoder.decode(part.value, { stream: true });
    }
    return text + decoder.decode();
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
export class BusinessApi implements MachineApi {
  constructor(
    readonly base: string,
    readonly token: () => Promise<string>,
    readonly request: typeof fetch = globalThis.fetch.bind(globalThis),
    readonly unauthorized: () => void = () => {},
  ) {}
  private async response(path: string, signal: AbortSignal) {
    let token: string;
    try {
      token = await this.token();
    } catch (error) {
      if (error instanceof ApiError && error.status === 401)
        this.unauthorized();
      throw error;
    }
    if (!token) {
      this.unauthorized();
      throw new ApiError(401, "UNAUTHORIZED");
    }
    const response = await this.request(this.base.replace(/\/$/, "") + path, {
      signal,
      redirect: "error",
      headers: {
        Authorization: "Bearer " + token,
        Accept: path.startsWith("/events/")
          ? "text/event-stream"
          : "application/json",
      },
    });
    if (!response.ok) {
      if (response.status === 401) this.unauthorized();
      let code = "DEPENDENCY_UNAVAILABLE",
        retryable = [429, 503, 504].includes(response.status);
      try {
        const value = JSON.parse(await boundedText(response));
        code = typeof value.error?.code === "string" ? value.error.code : code;
        retryable = retryable && value.error?.retryable !== false;
      } catch {
        /* Sanitized status still identifies failure. */
      }
      throw new ApiError(response.status, code, retryable);
    }
    return response;
  }
  private async json<T>(
    path: string,
    signal: AbortSignal,
    parse: (text: string) => T,
  ) {
    const deadline = new AbortController();
    const timer = setTimeout(() => deadline.abort(), 10000);
    try {
      const response = await this.response(
        path,
        AbortSignal.any([signal, deadline.signal]),
      );
      if (
        !response.headers
          .get("Content-Type")
          ?.toLowerCase()
          .startsWith("application/json")
      ) {
        await response.body?.cancel();
        throw new Error("Unsupported response representation");
      }
      return parse(await boundedText(response));
    } catch (error) {
      if (deadline.signal.aborted && !signal.aborted)
        throw new ApiError(504, "DEPENDENCY_TIMEOUT", true);
      throw error;
    } finally {
      clearTimeout(timer);
      deadline.abort();
    }
  }
  async live(machine: string, signal: AbortSignal) {
    const value = await this.json(
      "/machines/" + pathSegment(machine) + "/live",
      signal,
      parseLiveSnapshot,
    );
    if (value.machine_id !== machine)
      throw new Error("Response machine scope mismatch");
    return value;
  }
  async detail(machine: string, signal: AbortSignal) {
    const value = await this.json(
      "/machines/" + pathSegment(machine),
      signal,
      parseMachineDetail,
    );
    if (value.machine_id !== machine)
      throw new Error("Response machine scope mismatch");
    return value;
  }
  fleet(state: string, offset: number, signal: AbortSignal) {
    const query = new URLSearchParams({ limit: "50", offset: String(offset) });
    if (state) query.set("state", state);
    return this.json("/machines?" + query, signal, parseMachinePage);
  }
  async stream(
    machine: string,
    signal: AbortSignal,
    ready: () => void,
    event: (value: LiveEvent) => void,
  ) {
    const idle = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const touch = () => {
      clearTimeout(timer);
      timer = setTimeout(() => idle.abort(), 45000);
    };
    const combined = AbortSignal.any([signal, idle.signal]);
    try {
      touch();
      const response = await this.response(
        "/events/stream?" + new URLSearchParams({ machine_id: machine }),
        combined,
      );
      if (
        !response.body ||
        !response.headers.get("Content-Type")?.startsWith("text/event-stream")
      ) {
        await response.body?.cancel();
        throw new Error("Unsupported stream representation");
      }
      touch();
      const body = response.body.pipeThrough(
        new TransformStream<Uint8Array, Uint8Array>({
          transform(chunk, controller) {
            touch();
            controller.enqueue(chunk);
          },
        }),
        { signal: combined },
      );
      ready();
      for await (const value of decodeSSE(body)) {
        if (value.machine_id !== machine)
          throw new Error("SSE machine scope mismatch");
        if (combined.aborted) return;
        event(value);
      }
    } finally {
      clearTimeout(timer!);
      idle.abort();
    }
  }
}
