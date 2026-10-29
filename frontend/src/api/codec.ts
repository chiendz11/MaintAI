import type {
  AlertEvent,
  AlertPage,
  AlertDetail,
  Transition,
  FeedbackCreated,
} from "../types/alerts";
import {
  alertPageValidator,
  alertDetailValidator,
  transitionValidator,
  feedbackValidator,
  liveValidator,
  eventValidator,
  pageValidator,
  detailValidator,
} from "@maintai/validators";
import type {
  LiveEvent,
  LiveSnapshot,
  MachineDetail,
  MachinePage,
} from "../types/live";

export const MAX_BODY = 2 * 1024 * 1024;

export class NumericToken {
  constructor(readonly source: string) {}
}
type RecordValue = Record<string, unknown>;
function object(value: unknown): value is RecordValue {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    !(value instanceof NumericToken)
  );
}
export function sourceContextSupported(): boolean {
  let supported = false;
  JSON.parse("1", (_key, _value, context?: { source: string }) => {
    supported = context?.source === "1";
  });
  return supported;
}
function tokens(text: string): unknown {
  if (new TextEncoder().encode(text).byteLength > MAX_BODY)
    throw new Error("Response exceeds decoding budget");
  if (!sourceContextSupported())
    throw new Error(
      "Trình duyệt chưa hỗ trợ đọc số thứ tự chính xác. Cần cập nhật trình duyệt.",
    );
  return JSON.parse(text, (_key, value, context?: { source: string }) => {
    if (typeof value !== "number") return value;
    if (!context?.source) throw new Error("Numeric source is unavailable");
    return new NumericToken(context.source);
  });
}
function plain(value: unknown): unknown {
  if (value instanceof NumericToken) return Number(value.source);
  if (Array.isArray(value)) return value.map(plain);
  if (object(value))
    return Object.fromEntries(
      Object.entries(value).map(([k, v]) => [k, plain(v)]),
    );
  return value;
}
function integer(value: unknown): bigint {
  if (!(value instanceof NumericToken))
    throw new Error("Sequence must be an integer");
  const match = /^(\d+)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/.exec(value.source);
  if (!match) throw new Error("Sequence must be positive");
  const coefficient = match[1] + (match[2] ?? "");
  const scale = Number(match[3] ?? 0) - (match[2]?.length ?? 0);
  if (
    !Number.isSafeInteger(scale) ||
    Math.abs(scale) + coefficient.length > MAX_BODY
  )
    throw new Error("Sequence exceeds decoding budget");
  let digits = coefficient;
  if (scale < 0) {
    const places = -scale;
    if (
      places > digits.length ||
      !/^0*$/.test(digits.slice(digits.length - places))
    )
      throw new Error("Fractional sequence");
    digits = digits.slice(0, digits.length - places) || "0";
  }
  const result = BigInt(digits) * (scale > 0 ? 10n ** BigInt(scale) : 1n);
  if (result <= 0n) throw new Error("Sequence must be positive");
  return result;
}
function axis(value: unknown): unknown {
  if (value === null) return null;
  if (!object(value)) throw new Error("Invalid projection");
  return {
    ...(plain(value) as RecordValue),
    sequence_number: integer(value.sequence_number),
    ...(Object.hasOwn(value, "features_raw")
      ? { features_raw: value.features_raw }
      : {}),
  };
}
export function parseLiveSnapshot(text: string): LiveSnapshot {
  const value = tokens(text);
  if (!object(value)) throw new Error("Invalid live snapshot");
  const check = plain(value) as RecordValue;
  for (const key of [
    "telemetry",
    "latest_valid_prediction",
    "latest_processing",
  ]) {
    if (object(value[key])) {
      integer(value[key].sequence_number);
      (check[key] as RecordValue).sequence_number = 1;
    }
  }
  if (!liveValidator(check)) throw new Error("Invalid live snapshot schema");
  return {
    ...check,
    telemetry: axis(value.telemetry),
    latest_valid_prediction: axis(value.latest_valid_prediction),
    latest_processing: axis(value.latest_processing),
  } as LiveSnapshot;
}
function parseEvent(text: string, name: string): LiveEvent | AlertEvent {
  const value = tokens(text);
  if (!object(value) || !object(value.data))
    throw new Error("Invalid SSE envelope");
  if (name === "alert.created" || name === "alert.updated") {
    const check = plain(value);
    if (!eventValidator(check) || value.type !== name)
      throw new Error("Invalid alert event");
    return check as AlertEvent;
  }
  integer(value.data.sequence_number);
  const check = plain(value) as RecordValue;
  (check.data as RecordValue).sequence_number = 1;
  if (
    !eventValidator(check) ||
    value.type !== name ||
    typeof value.machine_id !== "string"
  )
    throw new Error("Invalid SSE schema or event name");
  return { ...check, data: axis(value.data) } as LiveEvent;
}
export function parseMachinePage(text: string): MachinePage {
  const value = plain(tokens(text));
  if (!pageValidator(value)) throw new Error("Invalid fleet schema");
  return value as MachinePage;
}
export function parseMachineDetail(text: string): MachineDetail {
  const value = plain(tokens(text));
  if (!detailValidator(value)) throw new Error("Invalid machine detail schema");
  return value as MachineDetail;
}
export function rawText(value: unknown): string {
  if (value instanceof NumericToken) return value.source;
  if (typeof value === "bigint") return value.toString();
  if (Array.isArray(value)) return "[" + value.map(rawText).join(", ") + "]";
  if (object(value))
    return (
      "{" +
      Object.entries(value)
        .map(([key, v]) => JSON.stringify(key) + ": " + rawText(v))
        .join(", ") +
      "}"
    );
  return JSON.stringify(value) ?? "—";
}
export async function* decodeEvents(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<LiveEvent | AlertEvent> {
  const reader = stream.getReader(),
    decoder = new TextDecoder("utf-8", { fatal: true });
  let pending = "",
    name = "",
    data: string[] = [],
    frameBytes = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) return;
      pending += decoder.decode(next.value, { stream: true });
      if (new TextEncoder().encode(pending).length + frameBytes > MAX_BODY)
        throw new Error("SSE exceeds frame budget");
      while (true) {
        const match = /\r\n|\r|\n/.exec(pending);
        if (!match || (match[0] === "\r" && match.index === pending.length - 1))
          break;
        const line = pending.slice(0, match.index);
        pending = pending.slice(match.index + match[0].length);
        frameBytes += new TextEncoder().encode(line).length;
        if (frameBytes > MAX_BODY) throw new Error("SSE exceeds frame budget");
        if (line === "") {
          if (
            data.length &&
            [
              "telemetry.received",
              "prediction.completed",
              "processing.status_changed",
              "alert.created",
              "alert.updated",
            ].includes(name)
          )
            yield parseEvent(data.join("\n"), name);
          name = "";
          data = [];
          frameBytes = 0;
        } else if (!line.startsWith(":")) {
          const colon = line.indexOf(":");
          const field = colon === -1 ? line : line.slice(0, colon);
          let value = colon === -1 ? "" : line.slice(colon + 1);
          if (value.startsWith(" ")) value = value.slice(1);
          if (field === "event") name = value;
          if (field === "data") data.push(value);
        }
      }
    }
  } finally {
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export async function* decodeSSE(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<LiveEvent> {
  for await (const event of decodeEvents(stream))
    if (event.type !== "alert.created" && event.type !== "alert.updated")
      yield event as LiveEvent;
}
function parseAlertDto<T>(text: string, validate: (v: unknown) => boolean): T {
  const value = plain(tokens(text));
  if (!validate(value)) throw new Error("Invalid alert response schema");
  return value as T;
}
export const parseAlertPage = (text: string) =>
  parseAlertDto<AlertPage>(text, alertPageValidator);
export const parseAlertDetail = (text: string) =>
  parseAlertDto<AlertDetail>(text, alertDetailValidator);
export const parseTransition = (text: string) =>
  parseAlertDto<Transition>(text, transitionValidator);
export const parseFeedback = (text: string) =>
  parseAlertDto<FeedbackCreated>(text, feedbackValidator);
