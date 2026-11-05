import {
  copilotValidator,
  briefAcceptedValidator,
  briefJobValidator,
  monitoringValidator,
  driftListValidator,
  driftReportValidator,
} from "@maintai/validators";
import { ApiError, boundedText, pathSegment } from "./client";

export type Intent =
  "MAINTENANCE_BRIEF" | "TECHNICIAN_CHECKLIST" | "ALERT_EXPLANATION";
export type Brief = {
  brief_id: string;
  alert_id: string;
  grounded_status: string;
  summary: string;
  uncertainty: string;
  checklist: { step: number; action: string; citation_ids: string[] }[];
  citations: {
    citation_id: string;
    document_id: string;
    document_version: string;
    chunk_id: string;
    page?: number | null;
    section?: string | null;
  }[];
};
export type Copilot = {
  copilot_status: string;
  message: string;
  brief: Brief | null;
};
export type Accepted = {
  job_id: string;
  alert_id: string;
  copilot_status: "PENDING";
  status_url: string;
};
export type BriefJob = {
  job_id: string;
  status: string;
  copilot_status: string;
  error_code: string | null;
};
export type Manifest = {
  report_id: string;
  computed_at: string;
  publication_at: string;
  model_version: string;
  dataset_version: string;
  overall_status: string;
};
export type DriftPage = {
  items: Manifest[];
  next_cursor: string | null;
  catalog_as_of: string;
  stale: boolean;
};
export type DriftReport = {
  report_id: string;
  computed_at: string;
  overall_status: string;
  reference: { model_version: string; dataset_version: string };
  current_window: { start_at: string; end_at: string; sample_count: number };
  reason?: string | null;
  [key: string]: unknown;
};
export type Monitoring = {
  status: string;
  observed_at: string;
  services: Record<
    "backend" | "ml" | "rag",
    { status: string; message: string }
  >;
  drift: {
    worker_status: string;
    last_success_at: string | null;
    stale_after_seconds: number;
    latest_report: {
      report_id: string;
      window_end_at: string;
      model_version: string;
      overall_status: string;
    } | null;
  };
};
function parse<T>(raw: string, validator: (value: unknown) => boolean): T {
  const value: unknown = JSON.parse(raw);
  if (!validator(value))
    throw new Error("Dữ liệu phản hồi không đúng contract.");
  return value as T;
}
export const parseCopilot = (raw: string) =>
  parse<Copilot>(raw, copilotValidator);
export class BriefUnknown extends Error {
  constructor() {
    super(
      "Chưa xác định kết quả yêu cầu. Đọc lại trạng thái trước khi thử lại cùng yêu cầu.",
    );
  }
}
type Request = (
  path: string,
  signal: AbortSignal,
  body?: unknown,
  headers?: Record<string, string>,
) => Promise<Response>;
export class M7Client {
  constructor(readonly request: Request) {}
  private async read<T>(
    path: string,
    signal: AbortSignal,
    validator: (value: unknown) => boolean,
    body?: unknown,
    headers?: Record<string, string>,
  ): Promise<T> {
    const timer = new AbortController();
    const timeout = setTimeout(() => timer.abort(), 10000);
    try {
      const response = await this.request(
        path,
        AbortSignal.any([signal, timer.signal]),
        body,
        headers,
      );
      if (
        !response.headers.get("Content-Type")?.startsWith("application/json")
      ) {
        await response.body?.cancel();
        throw new Error("Kiểu phản hồi không hợp lệ.");
      }
      return parse<T>(await boundedText(response), validator);
    } finally {
      clearTimeout(timeout);
      timer.abort();
    }
  }
  brief(id: string, signal: AbortSignal) {
    return this.read<Copilot>(
      "/alerts/" + pathSegment(id) + "/brief",
      signal,
      copilotValidator,
    );
  }
  job(id: string, signal: AbortSignal) {
    return this.read<BriefJob>(
      "/brief-jobs/" + pathSegment(id),
      signal,
      briefJobValidator,
    );
  }
  async generate(id: string, intent: Intent, key: string, signal: AbortSignal) {
    try {
      return await this.read<Accepted>(
        "/alerts/" + pathSegment(id) + "/brief",
        signal,
        briefAcceptedValidator,
        { intent },
        { "Idempotency-Key": key },
      );
    } catch (error) {
      if (error instanceof ApiError && error.status < 500) throw error;
      throw new BriefUnknown();
    }
  }
  summary(signal: AbortSignal) {
    return this.read<Monitoring>(
      "/monitoring/summary",
      signal,
      monitoringValidator,
    );
  }
  reports(
    filters: {
      model_version?: string;
      overall_status?: string;
      cursor?: string;
    },
    signal: AbortSignal,
  ) {
    const query = new URLSearchParams({ limit: "20" });
    for (const [key, value] of Object.entries(filters))
      if (value) query.set(key, value);
    return this.read<DriftPage>(
      "/drift-reports?" + query,
      signal,
      driftListValidator,
    );
  }
  report(id: string, signal: AbortSignal) {
    return this.read<DriftReport>(
      "/drift-reports/" + pathSegment(id),
      signal,
      driftReportValidator,
    );
  }
}

export type M7Event = {
  schema_version: "1.0.0";
  type:
    | "brief.ready"
    | "brief.updated"
    | "drift.updated"
    | "service.degraded"
    | "projection.resync";
  machine_id: string | null;
  occurred_at: string;
  data: Record<string, string>;
};
