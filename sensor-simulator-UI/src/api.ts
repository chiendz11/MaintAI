import type { Command, ControlApi, ControlList, ControlState } from "./types";

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly retryAfterMs?: number,
  ) {
    super(message);
  }
  get uncertain() {
    return (
      this.status === 0 ||
      this.status === 408 ||
      (this.status >= 200 && this.status < 300) ||
      this.status >= 500
    );
  }
}

const object = (v: unknown): v is Record<string, unknown> =>
  !!v && typeof v === "object" && !Array.isArray(v);
const date = (v: unknown) =>
  typeof v === "string" &&
  /(?:Z|[+-]\d{2}:\d{2})$/.test(v) &&
  Number.isFinite(Date.parse(v));
const text = (v: unknown): v is string => typeof v === "string" && v.length > 0;
const controlKeys = [
  "machine_id",
  "stream_state",
  "scenario",
  "control_version",
  "updated_at",
  "last_controlled_by",
  "source",
];
export function parseControl(v: unknown): ControlState {
  if (
    !object(v) ||
    Object.keys(v).some((k) => !controlKeys.includes(k)) ||
    !text(v.machine_id) ||
    !["RUNNING", "PAUSED"].includes(String(v.stream_state)) ||
    !text(v.scenario) ||
    !Number.isSafeInteger(v.control_version) ||
    Number(v.control_version) < 0 ||
    !date(v.updated_at) ||
    !text(v.last_controlled_by) ||
    v.source !== "SIMULATOR_CONTROL"
  )
    throw new ApiError(
      0,
      "INVALID_RESPONSE",
      "Control API trả dữ liệu không đúng contract.",
    );
  return v as unknown as ControlState;
}
export function parseList(v: unknown): ControlList {
  if (
    !object(v) ||
    Object.keys(v).some((k) => !["items", "as_of"].includes(k)) ||
    !Array.isArray(v.items) ||
    !date(v.as_of)
  )
    throw new ApiError(
      0,
      "INVALID_RESPONSE",
      "Danh sách máy không đúng contract.",
    );
  const items = v.items.map(parseControl);
  if (new Set(items.map((m) => m.machine_id)).size !== items.length)
    throw new ApiError(
      0,
      "INVALID_RESPONSE",
      "Control API trả machine_id trùng lặp.",
    );
  return { items, as_of: v.as_of as string };
}

export class HttpControlApi implements ControlApi {
  constructor(
    private readonly baseUrl: string,
    private readonly timeoutMs: number,
    private readonly accessToken: () => Promise<string | undefined>,
  ) {}
  private async request(path: string, command?: Command): Promise<unknown> {
    const token = await this.accessToken();
    const abort = new AbortController();
    const timeout = setTimeout(() => abort.abort(), this.timeoutMs);
    try {
      const headers: Record<string, string> = { Accept: "application/json" };
      if (token) headers.Authorization = `Bearer ${token}`;
      if (command) {
        headers["Content-Type"] = "application/json";
        headers["Idempotency-Key"] = command.id;
      }
      const response = await fetch(
        `${this.baseUrl}/api/v1/simulator/machines${path}`,
        {
          method: command
            ? command.action === "scenario"
              ? "PUT"
              : "POST"
            : "GET",
          headers,
          body: command ? JSON.stringify(command.body) : undefined,
          signal: abort.signal,
          cache: "no-store",
          credentials: "omit",
          redirect: "error",
        },
      );
      if (response.status !== 200) {
        const wait = response.headers.get("Retry-After");
        const retryAfterMs = wait
          ? Math.max(
              0,
              /^\d+$/.test(wait)
                ? Number(wait) * 1000
                : Date.parse(wait) - Date.now(),
            )
          : undefined;
        const data: unknown = await response.json().catch(() => null);
        const code =
          object(data) && object(data.error) && text(data.error.code)
            ? data.error.code
            : `HTTP_${response.status}`;
        // Never display arbitrary provider messages: they can contain credentials/internal URLs.
        throw new ApiError(
          response.status,
          code,
          `Control API trả HTTP ${response.status}.`,
          retryAfterMs,
        );
      }
      return await response.json();
    } catch (error) {
      if (error instanceof ApiError) throw error;
      throw new ApiError(
        0,
        "NETWORK_OR_TIMEOUT",
        "Mất kết nối hoặc hết thời gian chờ Control API.",
      );
    } finally {
      clearTimeout(timeout);
    }
  }
  async list() {
    return parseList(await this.request(""));
  }
  async get(machineId: string) {
    const result = parseControl(
      await this.request(`/${encodeURIComponent(machineId)}/control`),
    );
    if (result.machine_id !== machineId)
      throw new ApiError(0, "INVALID_RESPONSE", "Control API trả sai máy.");
    return result;
  }
  async execute(command: Command) {
    const result = parseControl(
      await this.request(
        `/${encodeURIComponent(command.machineId)}/${command.action}`,
        command,
      ),
    );
    if (
      result.machine_id !== command.machineId ||
      result.control_version < command.body.expected_control_version ||
      (command.action === "pause" && result.stream_state !== "PAUSED") ||
      (command.action === "resume" && result.stream_state !== "RUNNING") ||
      (command.action === "scenario" &&
        result.scenario !== command.body.scenario)
    )
      throw new ApiError(
        0,
        "INVALID_RESPONSE",
        "Phản hồi chưa xác nhận đúng lệnh điều khiển.",
      );
    return result;
  }
}
