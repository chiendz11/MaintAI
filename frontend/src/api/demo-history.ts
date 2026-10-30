import processing from "../../../contracts/v1.3/examples/m6/processing-page.json?raw";
import raw from "../../../contracts/v1.3/examples/m6/raw-page-offset-coverage.json?raw";
import processingChart from "../../../contracts/v1.3/examples/m6/processing-chart.json?raw";
import rawChart from "../../../contracts/v1.3/examples/m6/raw-chart.json?raw";
import { ApiError } from "./client";
import { rawText } from "./codec";
import { parseHistory, parseChart } from "./history-codec";
import type {
  Collection,
  HistoryApi,
  HistoryQuery,
  ExportRequest,
  ExportAccepted,
  ExportJob,
} from "../types/history";

// Explicit offline contract fixtures; this driver never handles live failures.
export class DemoHistory implements HistoryApi {
  readonly defaultRange = {
    from: "2026-10-18T00:00:00Z",
    to: "2026-10-18T01:00:00Z",
  };
  async page(
    machine: string,
    collection: Collection,
    query: HistoryQuery,
    signal: AbortSignal,
  ) {
    signal.throwIfAborted();
    const value = parseHistory(
      collection === "PROCESSING" ? processing : raw,
      collection,
    );
    value.as_of = new Date().toISOString();
    value.expires_at = new Date(Date.now() + 600000).toISOString();
    value.items = value.items
      .filter((item) => {
        const at = "outcome" in item ? item.processed_at : item.received_at;
        return (
          (!query.from || Date.parse(at) >= Date.parse(query.from)) &&
          (!query.to || Date.parse(at) < Date.parse(query.to)) &&
          (!("outcome" in item) ||
            ((!query.model_version ||
              item.outcome.model_version === query.model_version) &&
              (!query.result_kind || item.outcome.kind === query.result_kind)))
        );
      })
      .map((item) => ({
        ...item,
        machine_id: machine,
        ...("outcome" in item
          ? { outcome: { ...item.outcome, machine_id: machine } }
          : {}),
      }));
    return value;
  }
  async chart(_machine: string, query: HistoryQuery, signal: AbortSignal) {
    signal.throwIfAborted();
    if (!["risk_score", "rotational_speed_rpm"].includes(query.metric))
      throw new Error(
        "Biểu đồ demo chỉ có risk_score và rotational_speed_rpm; các chỉ số khác cần provider thật.",
      );
    const fixture =
      query.metric === "risk_score" || query.metric === "failure_probability"
        ? processingChart
        : rawChart;
    const value = parseChart(fixture);
    value.metric = query.metric;
    value.buckets = value.buckets.filter(
      (bucket) =>
        (!query.from || Date.parse(bucket.end) > Date.parse(query.from)) &&
        (!query.to || Date.parse(bucket.start) < Date.parse(query.to)),
    );
    value.as_of = new Date().toISOString();
    value.expires_at = new Date(Date.now() + 600000).toISOString();
    return value;
  }
  async export(
    value: ExportRequest,
    _key: string,
    signal: AbortSignal,
  ): Promise<ExportAccepted> {
    signal.throwIfAborted();
    if (value.format !== "CSV")
      throw new Error(
        "Demo ngoại tuyến chỉ có CSV; Parquet job cần provider PostgreSQL/MinIO.",
      );
    const page = await this.page(
      value.machine_ids[0],
      value.resource,
      { from: value.from, to: value.to },
      signal,
    );
    const csv =
      "record_json\r\n" +
      page.items
        .map((item) => '"' + rawText(item).replaceAll('"', '""') + '"')
        .join("\r\n");
    return { file: new Blob([csv], { type: "text/csv" }) };
  }
  async job(_id: string, _signal: AbortSignal): Promise<ExportJob> {
    throw new ApiError(503, "DEMO_JOB_UNAVAILABLE");
  }
  async download(_id: string, _signal: AbortSignal): Promise<Blob> {
    throw new ApiError(503, "DEMO_JOB_UNAVAILABLE");
  }
}
