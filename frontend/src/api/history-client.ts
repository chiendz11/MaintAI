import { ApiError, boundedText, pathSegment } from "./client";
import { parseHistory, parseChart, parseExportJob } from "./history-codec";
import type {
  Collection,
  ExportAccepted,
  ExportRequest,
  HistoryApi,
  HistoryQuery,
} from "../types/history";

export const HISTORY_PROFILE = "M6-HISTORY-01/1.0.0";
export class ExportUnknown extends Error {
  constructor(readonly key: string) {
    super(
      "Chưa xác định kết quả tạo export. Kiểm tra lại cùng yêu cầu và mã idempotency.",
    );
  }
}
type Transport = (
  path: string,
  signal: AbortSignal,
  body?: unknown,
  headers?: Record<string, string>,
) => Promise<Response>;
async function blob(response: Response, max: number): Promise<Blob> {
  if (!response.body) throw new Error("Tệp tải về rỗng.");
  const reader = response.body.getReader();
  const parts: Uint8Array<ArrayBuffer>[] = [];
  let size = 0;
  try {
    while (true) {
      const item = await reader.read();
      if (item.done) break;
      size += item.value.byteLength;
      if (size > max) throw new Error("Tệp vượt giới hạn tải.");
      parts.push(new Uint8Array(item.value));
    }
    const length = response.headers.get("Content-Length");
    if (length !== null && Number(length) !== size)
      throw new Error("Tệp tải chưa đầy đủ.");
    return new Blob(parts, {
      type: response.headers.get("Content-Type") ?? "application/octet-stream",
    });
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}
export class HistoryClient implements HistoryApi {
  constructor(readonly transport: Transport) {}
  private async bounded<T>(
    signal: AbortSignal,
    operation: (combined: AbortSignal) => Promise<T>,
    timeout = 12000,
  ): Promise<T> {
    const deadline = new AbortController();
    const timer = setTimeout(() => deadline.abort(), timeout);
    try {
      return await operation(AbortSignal.any([signal, deadline.signal]));
    } catch (error) {
      if (deadline.signal.aborted && !signal.aborted)
        throw new ApiError(504, "DEPENDENCY_TIMEOUT", true);
      throw error;
    } finally {
      clearTimeout(timer);
      deadline.abort();
    }
  }
  private request(
    path: string,
    signal: AbortSignal,
    value?: unknown,
    key?: string,
  ) {
    return this.transport(path, signal, value, {
      "X-MaintAI-History-Profile": HISTORY_PROFILE,
      ...(key ? { "Idempotency-Key": key } : {}),
    });
  }
  private async json<T>(
    path: string,
    signal: AbortSignal,
    parse: (text: string) => T,
  ) {
    return this.bounded(signal, async (combined) => {
      const response = await this.request(path, combined);
      if (
        response.headers.get("Content-Type")?.split(";", 1)[0] !==
        "application/json"
      ) {
        await response.body?.cancel();
        throw new Error("Phản hồi lịch sử không phải JSON.");
      }
      return parse(await boundedText(response));
    });
  }
  async page(
    machine: string,
    collection: Collection,
    query: HistoryQuery,
    signal: AbortSignal,
  ) {
    const value = await this.json(
      "/machines/" +
        pathSegment(machine) +
        (collection === "PROCESSING" ? "/predictions?" : "/sensor-readings?") +
        new URLSearchParams(query),
      signal,
      (text) => parseHistory(text, collection),
    );
    if (value.items.some((item) => item.machine_id !== machine))
      throw new Error("Sai phạm vi máy trong lịch sử.");
    return value;
  }
  chart(machine: string, query: HistoryQuery, signal: AbortSignal) {
    return this.json(
      "/machines/" +
        pathSegment(machine) +
        "/timeseries?" +
        new URLSearchParams(query),
      signal,
      parseChart,
    );
  }
  async export(
    value: ExportRequest,
    key: string,
    signal: AbortSignal,
  ): Promise<ExportAccepted> {
    try {
      return await this.bounded(signal, async (combined) => {
        const response = await this.request(
          "/history-exports",
          combined,
          value,
          key,
        );
        const type = response.headers.get("Content-Type")?.split(";", 1)[0];
        if (response.status === 200 && type === "text/csv")
          return { file: await blob(response, 10 * 1024 * 1024) };
        if (response.status === 202 && type === "application/json")
          return { job: parseExportJob(await boundedText(response)) };
        await response.body?.cancel();
        throw new Error("Không xác định kết quả export.");
      });
    } catch (error) {
      if (
        error instanceof ApiError &&
        error.status >= 400 &&
        error.status < 500 &&
        error.status !== 408
      )
        throw error;
      throw new ExportUnknown(key);
    }
  }
  job(id: string, signal: AbortSignal) {
    return this.json(
      "/history-exports/" + pathSegment(id),
      signal,
      parseExportJob,
    );
  }
  download(id: string, signal: AbortSignal) {
    return this.bounded(
      signal,
      async (combined) => {
        const response = await this.request(
          "/history-exports/" + pathSegment(id) + "/download",
          combined,
        );
        if (
          !["text/csv", "application/vnd.apache.parquet"].includes(
            response.headers.get("Content-Type")?.split(";", 1)[0] ?? "",
          )
        ) {
          await response.body?.cancel();
          throw new Error("Sai định dạng tệp export.");
        }
        return blob(response, 512 * 1024 * 1024);
      },
      120000,
    );
  }
}
