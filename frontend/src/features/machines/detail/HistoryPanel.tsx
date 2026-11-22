import { useEffect, useMemo, useRef, useState } from "react";
import type {
  Chart,
  Collection,
  ExportJob,
  ExportRequest,
  HistoryApi,
  HistoryPage,
  HistoryQuery,
  ProcessingRecord,
  RawRecord,
} from "../../../types/history";
import type { Telemetry } from "../../../types/live";
import { ExportUnknown } from "../../../api/history-client";
import { rawText } from "../../../api/codec";
import {
  computeDerivedFeatures,
  evaluateMetricValue,
  extractTelemetryFeatures,
  FEATURE_METADATA,
  type MetricKey,
  type PhysicalStatus,
  type TelemetryFeatures,
  type TrendFeatureKey,
} from "../../../shared/physical-guardrails";
import { Badge, ErrorNotice, date } from "../../../shared/ui";

const SHORT_TERM_HORIZON_MS = 10 * 60 * 1000;

export type TrendPoint = {
  event_id: string;
  timestamp: string;
  epochMs: number;
  features: TelemetryFeatures;
  temp_delta_k: number | null;
  mechanical_power_w: number | null;
  isLive: boolean;
};

export function buildFeatureTrendSeries(
  historyItems: (ProcessingRecord | RawRecord)[],
  liveTelemetry: Telemetry[] = [],
  maxPoints = 60,
): TrendPoint[] {
  const byEvent = new Map<string, TrendPoint>();

  for (const item of historyItems) {
    const isProcessing = "outcome" in item;
    const eventId = isProcessing ? item.source_event_id : item.event_id;
    const timestamp = isProcessing
      ? item.source_server_accepted_at || item.processed_at
      : item.received_at || item.server_accepted_at;
    const epochMs = Date.parse(timestamp);
    if (!eventId || !Number.isFinite(epochMs)) continue;
    const features = extractTelemetryFeatures(item.features_raw);
    const hasNumeric =
      features.air_temperature_k !== undefined ||
      features.process_temperature_k !== undefined ||
      features.rotational_speed_rpm !== undefined ||
      features.torque_nm !== undefined ||
      features.tool_wear_min !== undefined;
    if (!hasNumeric) continue;
    const derived = computeDerivedFeatures(features);
    byEvent.set(eventId, {
      event_id: eventId,
      timestamp,
      epochMs,
      features,
      temp_delta_k: derived.temp_delta_k,
      mechanical_power_w: derived.mechanical_power_w,
      isLive: false,
    });
  }

  for (const item of liveTelemetry.slice(-maxPoints)) {
    const timestamp = item.received_at || item.server_accepted_at;
    const epochMs = Date.parse(timestamp);
    if (!item.event_id || !Number.isFinite(epochMs)) continue;
    const features = extractTelemetryFeatures(item.features_raw);
    const hasNumeric =
      features.air_temperature_k !== undefined ||
      features.process_temperature_k !== undefined ||
      features.rotational_speed_rpm !== undefined ||
      features.torque_nm !== undefined ||
      features.tool_wear_min !== undefined;
    if (!hasNumeric) continue;
    const derived = computeDerivedFeatures(features);
    byEvent.set(item.event_id, {
      event_id: item.event_id,
      timestamp,
      epochMs,
      features,
      temp_delta_k: derived.temp_delta_k,
      mechanical_power_w: derived.mechanical_power_w,
      isLive: true,
    });
  }

  const sorted = [...byEvent.values()].sort((a, b) => a.epochMs - b.epochMs);
  if (sorted.length === 0) return [];
  const latestEpoch = sorted[sorted.length - 1].epochMs;
  return sorted
    .filter((point) => latestEpoch - point.epochMs <= SHORT_TERM_HORIZON_MS)
    .slice(-maxPoints);
}

function local(value: string) {
  const time = new Date(value);
  return new Date(time.getTime() - time.getTimezoneOffset() * 60000)
    .toISOString()
    .slice(0, -1);
}
function save(blob: Blob, format: "CSV" | "PARQUET") {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = "maintai-history." + format.toLowerCase();
  anchor.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function HistoryPanel({
  api,
  machine,
  liveTelemetry = [],
  selectedFeature = null,
  onSelectFeature,
  aiLastSeenAt = null,
  aiDegraded = false,
  hideTrendChart = false,
  onItemsLoaded,
}: {
  api: HistoryApi;
  machine: string;
  liveTelemetry?: Telemetry[];
  selectedFeature?: TrendFeatureKey | null;
  onSelectFeature?: (feature: TrendFeatureKey | null) => void;
  aiLastSeenAt?: string | null;
  aiDegraded?: boolean;
  hideTrendChart?: boolean;
  onItemsLoaded?: (items: (ProcessingRecord | RawRecord)[]) => void;
}) {
  const initial = api.defaultRange ?? {
    from: new Date(Date.now() - 3600000).toISOString(),
    to: new Date().toISOString(),
  };
  const [from, setFrom] = useState(() => local(initial.from));
  const [to, setTo] = useState(() => local(initial.to));
  const [collection, setCollection] = useState<Collection>("PROCESSING");
  const [model, setModel] = useState("");
  const [kind, setKind] = useState("");
  const [metric, setMetric] = useState("risk_score");
  const [resolution, setResolution] = useState("auto");
  const [applied, setApplied] = useState<{
    collection: Collection;
    query: HistoryQuery;
  }>({
    collection: "PROCESSING",
    query: { from: initial.from, to: initial.to },
  });
  const [pages, setPages] = useState<HistoryPage[]>([]);
  const [index, setIndex] = useState(0);
  const [chart, setChart] = useState<Chart | null>(null);
  const [chartQuery, setChartQuery] = useState<HistoryQuery | null>(null);
  const [job, setJob] = useState<ExportJob | null>(null);
  const [jobId, setJobId] = useState("");
  const [intent, setIntent] = useState<{
    value: ExportRequest;
    key: string;
  } | null>(null);
  const [unknown, setUnknown] = useState(false);
  const [busy, setBusy] = useState(false);
  const [exportBusy, setExportBusy] = useState(false);
  const [error, setError] = useState<unknown>(null);
  const [trendMode, setTrendMode] = useState<TrendFeatureKey>(
    selectedFeature ?? "ALL",
  );
  const controller = useRef<AbortController | null>(null);
  const exporting = useRef<AbortController | null>(null);
  const chartRequest = useRef<AbortController | null>(null);
  const current = pages[index];

  useEffect(() => {
    if (selectedFeature) {
      setTrendMode(selectedFeature);
    }
  }, [selectedFeature]);

  useEffect(() => {
    const request = new AbortController();
    controller.current?.abort();
    controller.current = request;
    setPages([]);
    setIndex(0);
    setError(null);
    setBusy(true);
    api
      .page(machine, applied.collection, applied.query, request.signal)
      .then((page) => {
        if (!request.signal.aborted) setPages([page]);
      })
      .catch((error) => {
        if (!request.signal.aborted) setError(error);
      })
      .finally(() => {
        if (!request.signal.aborted) setBusy(false);
      });
    return () => request.abort();
  }, [api, machine, applied]);
  useEffect(
    () => () => {
      controller.current?.abort();
      exporting.current?.abort();
      chartRequest.current?.abort();
    },
    [api, machine],
  );
  useEffect(() => {
    if (!job || !["QUEUED", "RUNNING"].includes(job.status)) return;
    const request = new AbortController();
    const timer = setTimeout(() => {
      api
        .job(job.job_id, request.signal)
        .then((value) => {
          if (!request.signal.aborted) setJob(value);
        })
        .catch((error) => {
          if (!request.signal.aborted) setError(error);
        });
    }, 2000);
    return () => {
      request.abort();
      clearTimeout(timer);
    };
  }, [api, job]);

  const trendSeries = useMemo(() => {
    const allItems = pages.flatMap((page) => page.items);
    return buildFeatureTrendSeries(allItems, liveTelemetry);
  }, [pages, liveTelemetry]);

  useEffect(() => {
    if (!onItemsLoaded) return;
    onItemsLoaded(pages.flatMap((page) => page.items));
  }, [pages, onItemsLoaded]);

  function apply() {
    try {
      const start = new Date(from).toISOString(),
        end = new Date(to).toISOString();
      if (start >= end)
        throw new Error("Thời điểm bắt đầu phải trước thời điểm kết thúc.");
      setApplied({
        collection,
        query: {
          from: start,
          to: end,
          ...(collection === "PROCESSING" && model
            ? { model_version: model }
            : {}),
          ...(collection === "PROCESSING" && kind ? { result_kind: kind } : {}),
        },
      });
      chartRequest.current?.abort();
      setChart(null);
      setChartQuery(null);
    } catch (error) {
      setError(error);
    }
  }
  async function next() {
    if (!current?.next_cursor) return;
    if (pages[index + 1]) {
      setIndex(index + 1);
      return;
    }
    controller.current?.abort();
    const request = new AbortController();
    controller.current = request;
    setBusy(true);
    setError(null);
    try {
      const page = await api.page(
        machine,
        applied.collection,
        { ...applied.query, cursor: current.next_cursor },
        request.signal,
      );
      if (!request.signal.aborted) {
        setPages([...pages, page]);
        setIndex(index + 1);
      }
    } catch (error) {
      if (!request.signal.aborted) setError(error);
    } finally {
      if (!request.signal.aborted) setBusy(false);
    }
  }
  async function loadChart(more = false) {
    chartRequest.current?.abort();
    const request = new AbortController();
    chartRequest.current = request;
    const query =
      more && chartQuery
        ? chartQuery
        : {
            from: applied.query.from,
            to: applied.query.to,
            metric,
            resolution,
            max_points: "1000",
          };
    try {
      setError(null);
      const value = await api.chart(
        machine,
        {
          ...query,
          ...(more && chart?.markers_next_cursor
            ? { markers_cursor: chart.markers_next_cursor }
            : {}),
        },
        request.signal,
      );
      if (!request.signal.aborted) {
        setChart(
          more && chart
            ? { ...value, markers: [...chart.markers, ...value.markers] }
            : value,
        );
        setChartQuery(query);
      }
    } catch (error) {
      if (!request.signal.aborted) setError(error);
    }
  }
  async function exportData(format: "CSV" | "PARQUET", retry = false) {
    const pending =
      retry && intent
        ? intent
        : {
            key: crypto.randomUUID(),
            value: {
              profile: "M6-HISTORY-01/1.0.0" as const,
              machine_ids: [machine],
              from: applied.query.from,
              to: applied.query.to,
              resource: applied.collection,
              format,
              ...(current ? { snapshot_id: current.snapshot_id } : {}),
              ...(applied.query.model_version
                ? { model_version: applied.query.model_version }
                : {}),
              ...(applied.query.result_kind
                ? { result_kind: applied.query.result_kind }
                : {}),
            },
          };
    exporting.current?.abort();
    const request = new AbortController();
    exporting.current = request;
    setIntent(pending);
    setExportBusy(true);
    setError(null);
    try {
      const value = await api.export(
        pending.value,
        pending.key,
        request.signal,
      );
      if (!request.signal.aborted) {
        setUnknown(false);
        if (value.file) save(value.file, pending.value.format);
        else {
          setJob(value.job);
          setJobId(value.job.job_id);
        }
      }
    } catch (error) {
      if (!request.signal.aborted) {
        setUnknown(error instanceof ExportUnknown);
        setError(error);
      }
    } finally {
      if (!request.signal.aborted) setExportBusy(false);
    }
  }
  async function lookup() {
    exporting.current?.abort();
    const request = new AbortController();
    exporting.current = request;
    try {
      const value = await api.job(jobId, request.signal);
      if (!request.signal.aborted) {
        setJob(value);
        setError(null);
      }
    } catch (error) {
      if (!request.signal.aborted) setError(error);
    }
  }
  async function download() {
    if (!job) return;
    exporting.current?.abort();
    const request = new AbortController();
    exporting.current = request;
    try {
      const value = await api.download(job.job_id, request.signal);
      if (!request.signal.aborted)
        save(value, value.type.includes("parquet") ? "PARQUET" : "CSV");
    } catch (error) {
      if (!request.signal.aborted) setError(error);
    }
  }

  function selectTrendPill(mode: TrendFeatureKey) {
    setTrendMode(mode);
    onSelectFeature?.(mode);
  }

  const showFeatureTrends =
    applied.collection === "RAW" ||
    collection === "RAW" ||
    Boolean(selectedFeature) ||
    aiDegraded ||
    trendSeries.length > 0;

  return (
    <section className="panel history-panel">
      <div className="panel-heading">
        <h2>
          {applied.collection === "PROCESSING"
            ? "Lịch sử xử lý"
            : "Lịch sử dữ liệu cảm biến"}
        </h2>
      </div>
      <p>
        Khoảng thời gian theo giờ trên thiết bị; yêu cầu được chuyển sang UTC.
        Lịch sử xử lý dùng thời gian hoàn thành Processing, dữ liệu cảm biến
        dùng received_at.
      </p>
      {api.defaultRange && (
        <p className="notice">
          Dữ liệu lịch sử mô phỏng từ fixtures; job Parquet cần provider
          PostgreSQL/MinIO.
        </p>
      )}
      <form
        className="history-controls"
        onSubmit={(event) => {
          event.preventDefault();
          apply();
        }}
      >
        <label>
          Nguồn lịch sử
          <select
            value={collection}
            onChange={(event) =>
              setCollection(event.target.value as Collection)
            }
          >
            <option value="PROCESSING">Kết quả xử lý</option>
            <option value="RAW">Dữ liệu cảm biến</option>
          </select>
        </label>
        <label>
          Từ thời điểm
          <input
            type="datetime-local"
            step="0.001"
            required
            value={from}
            onChange={(event) => setFrom(event.target.value)}
          />
        </label>
        <label>
          Đến thời điểm
          <input
            type="datetime-local"
            step="0.001"
            required
            value={to}
            onChange={(event) => setTo(event.target.value)}
          />
        </label>
        {collection === "PROCESSING" && (
          <>
            <label>
              Phiên bản model
              <input
                value={model}
                onChange={(event) => setModel(event.target.value)}
                placeholder="Tất cả"
              />
            </label>
            <label>
              Loại kết quả
              <select
                value={kind}
                onChange={(event) => setKind(event.target.value)}
              >
                <option value="">Tất cả</option>
                <option value="PREDICTION_COMPLETED">Dự đoán hoàn thành</option>
                <option value="DATA_INVALID">Dữ liệu không hợp lệ</option>
                <option value="INFERENCE_UNAVAILABLE">
                  Inference không khả dụng
                </option>
              </select>
            </label>
          </>
        )}
        <button className="button secondary" type="submit" disabled={busy}>
          Áp dụng
        </button>
      </form>
      <ErrorNotice error={error} />
      {busy && <p role="status">Đang tải lịch sử…</p>}
      {!hideTrendChart && showFeatureTrends && (
        <FeatureTrendChart
          series={trendSeries}
          mode={trendMode}
          onSelectMode={selectTrendPill}
          aiLastSeenAt={aiLastSeenAt}
          aiDegraded={aiDegraded}
        />
      )}
      {current && (
        <>
          <p>
            Snapshot <code>{current.snapshot_id}</code> · Chụp lúc{" "}
            {date(current.as_of)} · Hết hạn {date(current.expires_at)} · Trục{" "}
            <code>{current.time_basis}</code>
          </p>
          {current.items.length === 0 ? (
            <p>Không có bản ghi trong khoảng đã chọn.</p>
          ) : (
            <div className="history-table">
              <table>
                <thead>
                  <tr>
                    <th>Bản ghi / nguồn</th>
                    <th>Thời gian</th>
                    <th>Kết quả / bằng chứng</th>
                  </tr>
                </thead>
                <tbody>
                  {current.items.map((item) => (
                    <tr
                      key={"outcome" in item ? item.result_id : item.receipt_id}
                    >
                      <td>
                        {"outcome" in item ? (
                          <>
                            <code>{item.result_id}</code>
                            <p>{item.source_event_id}</p>
                          </>
                        ) : (
                          <>
                            <code>{item.receipt_id}</code>
                            <p>{item.event_id}</p>
                            <p>
                              Sequence {item.sequence_number.toString()} ·
                              Offset {item.kafka.offset.toString()}
                            </p>
                          </>
                        )}
                      </td>
                      <td>
                        {"outcome" in item ? (
                          <>
                            <p>Processing: {date(item.processed_at)}</p>
                            <p>
                              Ingestion nhận:{" "}
                              {date(item.source_server_accepted_at)}
                            </p>
                          </>
                        ) : (
                          <>
                            <p>Cảm biến: {date(item.received_at)}</p>
                            <p>
                              Ingestion nhận: {date(item.server_accepted_at)}
                            </p>
                          </>
                        )}
                      </td>
                      <td>
                        <Badge
                          value={
                            "outcome" in item
                              ? item.outcome.kind
                              : item.validation_status
                          }
                        />
                        <details>
                          <summary>Dữ liệu gốc và kết quả</summary>
                          <pre>{rawText(item)}</pre>
                        </details>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <div className="history-actions">
            <button
              className="button small secondary"
              disabled={index === 0 || busy}
              onClick={() => setIndex(index - 1)}
            >
              Trang trước
            </button>
            <span>Trang {index + 1}</span>
            <button
              className="button small secondary"
              disabled={!current.next_cursor || busy}
              onClick={() => void next()}
            >
              Trang sau
            </button>
          </div>
        </>
      )}
      <div className="history-actions">
        <button
          className="button secondary"
          disabled={exportBusy || busy}
          onClick={() => void exportData("CSV")}
        >
          Xuất CSV
        </button>
        <button
          className="button secondary"
          disabled={exportBusy || busy}
          onClick={() => void exportData("PARQUET")}
        >
          Tạo Parquet job
        </button>
      </div>
      {unknown && intent && (
        <div className="notice">
          <p>
            Yêu cầu {intent.key}: {intent.value.from} → {intent.value.to}. Kết
            quả chưa rõ; không tạo yêu cầu mới tự động.
          </p>
          <button
            className="button secondary"
            disabled={exportBusy}
            onClick={() => void exportData(intent.value.format, true)}
          >
            Kiểm tra lại cùng yêu cầu
          </button>
        </div>
      )}
      <div className="history-controls">
        <label>
          Mã export
          <input
            value={jobId}
            onChange={(event) => setJobId(event.target.value)}
          />
        </label>
        <button
          className="button secondary"
          disabled={!jobId}
          onClick={() => void lookup()}
        >
          Tra cứu export
        </button>
      </div>
      {job && (
        <div className="notice">
          <p>
            Export <code>{job.job_id}</code> · <Badge value={job.status} />
          </p>
          <p>
            Hết hạn tải: {date(job.expires_at)} {job.error_code ?? ""}
          </p>
          {job.status === "SUCCEEDED" && (
            <button
              className="button secondary"
              onClick={() => void download()}
            >
              Tải tệp export
            </button>
          )}
        </div>
      )}
      <h3>Biểu đồ theo thời gian</h3>
      <p>
        Biểu đồ dùng toàn bộ dữ liệu hợp lệ trong khoảng đã áp dụng; bộ lọc
        model/loại kết quả chỉ áp dụng bảng lịch sử.
      </p>
      <div className="history-controls">
        <label>
          Chỉ số
          <select
            value={metric}
            onChange={(event) => setMetric(event.target.value)}
          >
            {[
              "risk_score",
              "failure_probability",
              "air_temperature_k",
              "process_temperature_k",
              "rotational_speed_rpm",
              "torque_nm",
              "tool_wear_min",
            ].map((name) => (
              <option key={name}>{name}</option>
            ))}
          </select>
        </label>
        <label>
          Độ phân giải
          <select
            value={resolution}
            onChange={(event) => setResolution(event.target.value)}
          >
            {["auto", "1s", "10s", "1m", "5m", "1h", "1d"].map((name) => (
              <option key={name}>{name}</option>
            ))}
          </select>
        </label>
        <button className="button secondary" onClick={() => void loadChart()}>
          Xem biểu đồ
        </button>
      </div>
      {chart && (
        <HistoryChart value={chart} more={() => void loadChart(true)} />
      )}
    </section>
  );
}

const TREND_PILLS: { key: TrendFeatureKey; label: string }[] = [
  { key: "ALL", label: "Stacked Overview (All)" },
  { key: "torque_nm", label: "Torque (Nm)" },
  { key: "rotational_speed_rpm", label: "Speed (RPM)" },
  { key: "air_temperature_k", label: "Air Temp (K)" },
  { key: "process_temperature_k", label: "Process Temp (K)" },
  { key: "temp_delta_k", label: "Temp Delta (K)" },
  { key: "tool_wear_min", label: "Tool Wear (min)" },
  { key: "mechanical_power_w", label: "Est. Power (W)" },
];

type LaneLineDef = {
  label: string;
  color: string;
  metricKey: MetricKey;
  getValue: (point: TrendPoint) => number | undefined;
};

type LaneConfig = {
  id: string;
  title: string;
  unit: string;
  min: number;
  max: number;
  warningHigh?: number;
  warningLow?: number;
  criticalHigh?: number;
  criticalLow?: number;
  decimals: number;
  lines: LaneLineDef[];
};

const TEMPERATURES_LANE: LaneConfig = {
  id: "temperatures",
  title: "Temperatures (Air & Process)",
  unit: "K",
  min: FEATURE_METADATA.air_temperature_k.min,
  max: FEATURE_METADATA.process_temperature_k.max,
  warningHigh: FEATURE_METADATA.process_temperature_k.warningHigh,
  criticalHigh: FEATURE_METADATA.process_temperature_k.criticalHigh,
  decimals: 1,
  lines: [
    {
      label: "Process Temp",
      color: "#ea580c",
      metricKey: "process_temperature_k",
      getValue: (p) => p.features.process_temperature_k,
    },
    {
      label: "Air Temp",
      color: "#0284c7",
      metricKey: "air_temperature_k",
      getValue: (p) => p.features.air_temperature_k,
    },
  ],
};

const SINGLE_FEATURE_LANES: Record<
  Exclude<TrendFeatureKey, "ALL">,
  LaneConfig
> = {
  temperatures: TEMPERATURES_LANE,
  air_temperature_k: {
    id: "air_temperature_k",
    title: "Air Temperature",
    unit: FEATURE_METADATA.air_temperature_k.unit,
    min: FEATURE_METADATA.air_temperature_k.min,
    max: FEATURE_METADATA.air_temperature_k.max,
    warningHigh: FEATURE_METADATA.air_temperature_k.warningHigh,
    criticalHigh: FEATURE_METADATA.air_temperature_k.criticalHigh,
    decimals: 1,
    lines: [
      {
        label: "Air Temp",
        color: "#0284c7",
        metricKey: "air_temperature_k",
        getValue: (p) => p.features.air_temperature_k,
      },
    ],
  },
  process_temperature_k: {
    id: "process_temperature_k",
    title: "Process Temperature",
    unit: FEATURE_METADATA.process_temperature_k.unit,
    min: FEATURE_METADATA.process_temperature_k.min,
    max: FEATURE_METADATA.process_temperature_k.max,
    warningHigh: FEATURE_METADATA.process_temperature_k.warningHigh,
    criticalHigh: FEATURE_METADATA.process_temperature_k.criticalHigh,
    decimals: 1,
    lines: [
      {
        label: "Process Temp",
        color: "#ea580c",
        metricKey: "process_temperature_k",
        getValue: (p) => p.features.process_temperature_k,
      },
    ],
  },
  temp_delta_k: {
    id: "temp_delta_k",
    title: "Temp Delta (ΔT)",
    unit: FEATURE_METADATA.temp_delta_k.unit,
    min: FEATURE_METADATA.temp_delta_k.min,
    max: FEATURE_METADATA.temp_delta_k.max,
    warningLow: FEATURE_METADATA.temp_delta_k.warningLow,
    criticalLow: FEATURE_METADATA.temp_delta_k.criticalLow,
    decimals: 1,
    lines: [
      {
        label: "Temp Delta (ΔT)",
        color: "#0891b2",
        metricKey: "temp_delta_k",
        getValue: (p) => p.temp_delta_k ?? undefined,
      },
    ],
  },
  torque_nm: {
    id: "torque_nm",
    title: "Torque",
    unit: FEATURE_METADATA.torque_nm.unit,
    min: FEATURE_METADATA.torque_nm.min,
    max: FEATURE_METADATA.torque_nm.max,
    warningHigh: FEATURE_METADATA.torque_nm.warningHigh,
    criticalHigh: FEATURE_METADATA.torque_nm.criticalHigh,
    decimals: 1,
    lines: [
      {
        label: "Torque",
        color: "#2563eb",
        metricKey: "torque_nm",
        getValue: (p) => p.features.torque_nm,
      },
    ],
  },
  rotational_speed_rpm: {
    id: "rotational_speed_rpm",
    title: "Rotational Speed",
    unit: "RPM",
    min: FEATURE_METADATA.rotational_speed_rpm.min,
    max: FEATURE_METADATA.rotational_speed_rpm.max,
    warningLow: FEATURE_METADATA.rotational_speed_rpm.warningLow,
    warningHigh: FEATURE_METADATA.rotational_speed_rpm.warningHigh,
    criticalLow: FEATURE_METADATA.rotational_speed_rpm.criticalLow,
    criticalHigh: FEATURE_METADATA.rotational_speed_rpm.criticalHigh,
    decimals: 0,
    lines: [
      {
        label: "Speed",
        color: "#168b75",
        metricKey: "rotational_speed_rpm",
        getValue: (p) => p.features.rotational_speed_rpm,
      },
    ],
  },
  tool_wear_min: {
    id: "tool_wear_min",
    title: "Tool Wear",
    unit: "min",
    min: FEATURE_METADATA.tool_wear_min.min,
    max: FEATURE_METADATA.tool_wear_min.max,
    warningHigh: FEATURE_METADATA.tool_wear_min.warningHigh,
    criticalHigh: FEATURE_METADATA.tool_wear_min.criticalHigh,
    decimals: 0,
    lines: [
      {
        label: "Tool Wear",
        color: "#7c3aed",
        metricKey: "tool_wear_min",
        getValue: (p) => p.features.tool_wear_min,
      },
    ],
  },
  mechanical_power_w: {
    id: "mechanical_power_w",
    title: "Est. Mechanical Power (P)",
    unit: FEATURE_METADATA.mechanical_power_w.unit,
    min: FEATURE_METADATA.mechanical_power_w.min,
    max: FEATURE_METADATA.mechanical_power_w.max,
    warningLow: FEATURE_METADATA.mechanical_power_w.warningLow,
    warningHigh: FEATURE_METADATA.mechanical_power_w.warningHigh,
    criticalLow: FEATURE_METADATA.mechanical_power_w.criticalLow,
    criticalHigh: FEATURE_METADATA.mechanical_power_w.criticalHigh,
    decimals: 0,
    lines: [
      {
        label: "Est. Power (P)",
        color: "#4f46e5",
        metricKey: "mechanical_power_w",
        getValue: (p) => p.mechanical_power_w ?? undefined,
      },
    ],
  },
};

const STACKED_LANES: LaneConfig[] = [
  TEMPERATURES_LANE,
  SINGLE_FEATURE_LANES.torque_nm,
  SINGLE_FEATURE_LANES.rotational_speed_rpm,
  SINGLE_FEATURE_LANES.tool_wear_min,
];

function safeX(
  epochMs: number,
  minEpoch: number,
  maxEpoch: number,
  left: number,
  right: number,
): number {
  if (!Number.isFinite(epochMs)) return (left + right) / 2;
  if (maxEpoch <= minEpoch) return (left + right) / 2;
  const ratio = (epochMs - minEpoch) / (maxEpoch - minEpoch);
  const clamped = Math.max(0, Math.min(1, ratio));
  return Number((left + clamped * (right - left)).toFixed(2));
}

function safeY(
  value: number,
  minVal: number,
  maxVal: number,
  top: number,
  bottom: number,
): number {
  if (!Number.isFinite(value)) return (top + bottom) / 2;
  if (maxVal <= minVal) return (top + bottom) / 2;
  const ratio = (value - minVal) / (maxVal - minVal);
  const clamped = Math.max(0, Math.min(1, ratio));
  return Number((bottom - clamped * (bottom - top)).toFixed(2));
}

function formatStatValue(value: number, decimals: number): string {
  if (!Number.isFinite(value)) return "—";
  return decimals === 0
    ? Math.round(value).toString()
    : value.toFixed(decimals);
}

function formatAxisValue(value: number, decimals: number): string {
  if (!Number.isFinite(value)) return "—";
  return decimals === 0
    ? Math.round(value).toString()
    : Number(value.toFixed(decimals)).toString();
}

function formatTimeTick(epochMs: number): string {
  if (!Number.isFinite(epochMs)) return "--:--:--";
  return new Date(epochMs).toLocaleTimeString("en-GB", {
    hour12: false,
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function statusPointColor(
  status: PhysicalStatus,
  fallbackColor: string,
): string {
  if (status === "CRITICAL") return "#dc2626";
  if (status === "WARNING") return "#d97706";
  return fallbackColor;
}

function computeThresholdBands(
  lane: LaneConfig,
  minVal: number,
  maxVal: number,
  laneTop: number,
  laneBottom: number,
): { y: number; height: number; fill: string }[] {
  const bands: { y: number; height: number; fill: string }[] = [];
  if (lane.criticalHigh !== undefined) {
    const yCritHigh = safeY(
      lane.criticalHigh,
      minVal,
      maxVal,
      laneTop,
      laneBottom,
    );
    if (yCritHigh > laneTop) {
      bands.push({
        y: laneTop,
        height: yCritHigh - laneTop,
        fill: "rgba(220, 38, 38, 0.10)",
      });
    }
  }
  if (lane.warningHigh !== undefined) {
    const yWarnHigh = safeY(
      lane.warningHigh,
      minVal,
      maxVal,
      laneTop,
      laneBottom,
    );
    const yUpperBound =
      lane.criticalHigh !== undefined
        ? safeY(lane.criticalHigh, minVal, maxVal, laneTop, laneBottom)
        : laneTop;
    if (yWarnHigh > yUpperBound) {
      bands.push({
        y: yUpperBound,
        height: yWarnHigh - yUpperBound,
        fill: "rgba(217, 119, 6, 0.08)",
      });
    }
  }
  if (lane.criticalLow !== undefined) {
    const yCritLow = safeY(
      lane.criticalLow,
      minVal,
      maxVal,
      laneTop,
      laneBottom,
    );
    if (laneBottom > yCritLow) {
      bands.push({
        y: yCritLow,
        height: laneBottom - yCritLow,
        fill: "rgba(220, 38, 38, 0.10)",
      });
    }
  }
  if (lane.warningLow !== undefined) {
    const yWarnLow = safeY(
      lane.warningLow,
      minVal,
      maxVal,
      laneTop,
      laneBottom,
    );
    const yLowerBound =
      lane.criticalLow !== undefined
        ? safeY(lane.criticalLow, minVal, maxVal, laneTop, laneBottom)
        : laneBottom;
    if (yLowerBound > yWarnLow) {
      bands.push({
        y: yWarnLow,
        height: yLowerBound - yWarnLow,
        fill: "rgba(217, 119, 6, 0.08)",
      });
    }
  }
  return bands;
}

function buildThresholdMarks(
  lane: LaneConfig,
  minVal: number,
  maxVal: number,
  laneTop: number,
  laneBottom: number,
) {
  const rawMarks: {
    key: string;
    threshold: number;
    y: number;
    stroke: string;
    dash: string;
    strokeWidth: string;
    label: string;
    title: string;
  }[] = [];
  const warnVals = [lane.warningHigh, lane.warningLow].filter(
    (v): v is number => v !== undefined,
  );
  const critVals = [lane.criticalHigh, lane.criticalLow].filter(
    (v): v is number => v !== undefined,
  );
  for (const threshold of warnVals) {
    rawMarks.push({
      key: `warn-${threshold}`,
      threshold,
      y: safeY(threshold, minVal, maxVal, laneTop, laneBottom),
      stroke: "#d97706",
      dash: "4 3",
      strokeWidth: "1",
      label: `WARN ${formatAxisValue(threshold, lane.decimals)}`,
      title: `Warning threshold: ${threshold} ${lane.unit}`,
    });
  }
  for (const threshold of critVals) {
    rawMarks.push({
      key: `crit-${threshold}`,
      threshold,
      y: safeY(threshold, minVal, maxVal, laneTop, laneBottom),
      stroke: "#dc2626",
      dash: "3 3",
      strokeWidth: "1.2",
      label: `CRIT ${formatAxisValue(threshold, lane.decimals)}`,
      title: `Critical threshold: ${threshold} ${lane.unit}`,
    });
  }
  rawMarks.sort((a, b) => a.y - b.y);
  let lastLabelY: number | null = null;
  return rawMarks.map((mark) => {
    let labelY = Math.max(laneTop + 8, Math.min(laneBottom - 3, mark.y + 3));
    if (lastLabelY !== null && labelY - lastLabelY < 9) {
      labelY = Math.min(laneBottom - 2, lastLabelY + 9);
    }
    lastLabelY = labelY;
    return { ...mark, labelY };
  });
}

export function FeatureTrendChart({
  series,
  mode,
  onSelectMode,
  aiLastSeenAt = null,
  aiDegraded = false,
}: {
  series: TrendPoint[];
  mode: TrendFeatureKey;
  onSelectMode: (mode: TrendFeatureKey) => void;
  aiLastSeenAt?: string | null;
  aiDegraded?: boolean;
}) {
  const [hoverIndex, setHoverIndex] = useState<number | null>(null);

  const minEpoch = series[0]?.epochMs ?? 0;
  const maxEpoch = series.at(-1)?.epochMs ?? 0;
  const spanMs = Math.max(0, maxEpoch - minEpoch);
  const gapThresholdMs =
    spanMs <= 120_000 ? 5000 : Math.max(5000, Math.round(spanMs * 0.35));

  const aiLastEpoch =
    aiLastSeenAt && Number.isFinite(Date.parse(aiLastSeenAt))
      ? Date.parse(aiLastSeenAt)
      : null;
  const showAiMarker =
    aiLastEpoch !== null &&
    series.length > 1 &&
    aiLastEpoch >= minEpoch &&
    aiLastEpoch <= maxEpoch;

  const activePoint =
    hoverIndex !== null && series[hoverIndex]
      ? series[hoverIndex]
      : (series.at(-1) ?? null);

  const visibleLanes =
    mode === "ALL"
      ? STACKED_LANES
      : [SINGLE_FEATURE_LANES[mode] ?? TEMPERATURES_LANE];
  const isSingleLane = visibleLanes.length === 1;

  const svgWidth = 760;
  const svgHeight = isSingleLane ? 236 : 368;
  const axisY = isSingleLane ? 204 : 336;
  const topLaneTop = isSingleLane ? 20 : 14;
  const leftX = 56;
  const rightX = 696;

  const xTicks = useMemo(() => {
    if (series.length === 0) return [];
    const tickCount =
      maxEpoch <= minEpoch ? 1 : Math.min(5, Math.max(2, series.length));
    if (tickCount === 1) {
      return [
        {
          epochMs: minEpoch,
          x: (leftX + rightX) / 2,
          anchor: "middle" as const,
          label: formatTimeTick(minEpoch),
        },
      ];
    }
    return Array.from({ length: tickCount }, (_, i) => {
      const ratio = i / (tickCount - 1);
      const epochMs = Math.round(minEpoch + ratio * (maxEpoch - minEpoch));
      const anchor =
        i === 0
          ? ("start" as const)
          : i === tickCount - 1
            ? ("end" as const)
            : ("middle" as const);
      return {
        epochMs,
        x: safeX(epochMs, minEpoch, maxEpoch, leftX, rightX),
        anchor,
        label: formatTimeTick(epochMs),
      };
    });
  }, [series.length, minEpoch, maxEpoch]);

  const laneStats = useMemo(
    () =>
      visibleLanes.flatMap((lane) =>
        lane.lines.flatMap((lineDef) => {
          const vals = series
            .map(lineDef.getValue)
            .filter((v): v is number => v !== undefined && Number.isFinite(v));
          if (vals.length === 0) return [];
          const min = Math.min(...vals);
          const max = Math.max(...vals);
          const avg = vals.reduce((sum, v) => sum + v, 0) / vals.length;
          const latest = vals[vals.length - 1];
          const status = evaluateMetricValue(lineDef.metricKey, latest);
          return [
            {
              id: `${lane.id}-${lineDef.label}`,
              label: lineDef.label,
              unit: lane.unit,
              decimals: lane.decimals,
              min,
              avg,
              max,
              latest,
              status,
            },
          ];
        }),
      ),
    [visibleLanes, series],
  );

  return (
    <div className="feature-trend-section">
      <div className="feature-trend-header">
        <div>
          <h3>Physical Feature Trends (Raw Telemetry + Live Stream)</h3>
          <p className="hint">
            Kết hợp dữ liệu lịch sử M6 ({series.filter((p) => !p.isLive).length}{" "}
            điểm) và đệm Live SSE ({series.filter((p) => p.isLive).length} điểm)
            để chẩn đoán xu hướng vật lý ngay cả khi nhánh AI gián đoạn.
          </p>
        </div>
        {aiDegraded && (
          <span className="badge warn">
            <span className="dot" />
            Chế độ chẩn đoán vật lý trực tiếp
          </span>
        )}
      </div>
      <div
        className="feature-pills"
        role="group"
        aria-label="Chọn thông số vật lý"
      >
        {TREND_PILLS.map((pill) => (
          <button
            key={pill.key}
            type="button"
            className={`pill-button ${mode === pill.key ? "active" : ""}`}
            onClick={() => onSelectMode(pill.key)}
          >
            {pill.label}
          </button>
        ))}
      </div>
      {series.length === 0 ? (
        <p className="hint">
          Chưa có mẫu dữ liệu cảm biến hợp lệ để vẽ xu hướng.
        </p>
      ) : (
        <>
          <svg
            className="history-chart feature-trend-svg"
            viewBox={`0 0 ${svgWidth} ${svgHeight}`}
            role="img"
            aria-label="Xu hướng vật lý M6 + Live SSE"
          >
            {visibleLanes.map((lane, laneIdx) => {
              const laneTop = isSingleLane ? 20 : 14 + laneIdx * 86;
              const laneBottom = isSingleLane ? 204 : laneTop + 64;

              // Dynamic range expansion if values exceed nominal bounds
              const laneValues = series.flatMap((p) =>
                lane.lines
                  .map((l) => l.getValue(p))
                  .filter((v): v is number => v !== undefined),
              );
              const minVal = laneValues.length
                ? Math.min(lane.min, ...laneValues)
                : lane.min;
              const maxVal = laneValues.length
                ? Math.max(lane.max, ...laneValues)
                : lane.max;

              const bands = computeThresholdBands(
                lane,
                minVal,
                maxVal,
                laneTop,
                laneBottom,
              );
              const marks = buildThresholdMarks(
                lane,
                minVal,
                maxVal,
                laneTop,
                laneBottom,
              );
              const yTickCount = isSingleLane ? 5 : 3;
              const yTicks = Array.from({ length: yTickCount }, (_, i) => {
                const ratio = i / (yTickCount - 1);
                const val = minVal + ratio * (maxVal - minVal);
                return {
                  val,
                  y: safeY(val, minVal, maxVal, laneTop, laneBottom),
                  isEdge: i === 0 || i === yTickCount - 1,
                };
              });

              return (
                <g key={lane.id}>
                  <rect
                    x={leftX}
                    y={laneTop}
                    width={rightX - leftX}
                    height={laneBottom - laneTop}
                    fill="#f8fafc"
                    stroke="#e2e8f0"
                    rx="4"
                  />
                  {bands.map((band, bandIdx) => (
                    <rect
                      key={`band-${bandIdx}`}
                      x={leftX + 0.5}
                      y={band.y}
                      width={rightX - leftX - 1}
                      height={band.height}
                      fill={band.fill}
                    />
                  ))}
                  {yTicks.map((tick, tickIdx) => (
                    <g key={`ytick-${tickIdx}`}>
                      {!tick.isEdge && (
                        <line
                          x1={leftX}
                          y1={tick.y}
                          x2={rightX}
                          y2={tick.y}
                          stroke="#e2e8f0"
                          strokeWidth="1"
                          strokeDasharray="2 3"
                        />
                      )}
                      <line
                        x1={leftX - 4}
                        y1={tick.y}
                        x2={leftX}
                        y2={tick.y}
                        stroke="#94a3b8"
                        strokeWidth="1"
                      />
                      <text
                        x={leftX - 7}
                        y={tick.y + 3}
                        textAnchor="end"
                        fontSize="9"
                        fill="#64748b"
                      >
                        {formatAxisValue(tick.val, lane.decimals)}
                      </text>
                    </g>
                  ))}
                  <text
                    x={leftX + 6}
                    y={laneTop + 12}
                    fontSize="10"
                    fontWeight="600"
                    fill="#334155"
                  >
                    {lane.title} ({lane.unit})
                  </text>
                  {marks.map((mark) => (
                    <g key={mark.key}>
                      <line
                        x1={leftX}
                        y1={mark.y}
                        x2={rightX}
                        y2={mark.y}
                        stroke={mark.stroke}
                        strokeWidth={mark.strokeWidth}
                        strokeDasharray={mark.dash}
                      >
                        <title>{mark.title}</title>
                      </line>
                      <text
                        x={rightX + 4}
                        y={mark.labelY}
                        fontSize="8.5"
                        fontWeight="600"
                        fill={mark.stroke}
                      >
                        {mark.label}
                      </text>
                    </g>
                  ))}
                  {lane.lines.map((lineDef) => {
                    const coords = series
                      .map((point, idx) => {
                        const v = lineDef.getValue(point);
                        if (v === undefined) return null;
                        const status = evaluateMetricValue(
                          lineDef.metricKey,
                          v,
                        );
                        return {
                          idx,
                          point,
                          v,
                          status,
                          pointColor: statusPointColor(status, lineDef.color),
                          x: safeX(
                            point.epochMs,
                            minEpoch,
                            maxEpoch,
                            leftX,
                            rightX,
                          ),
                          y: safeY(v, minVal, maxVal, laneTop, laneBottom),
                        };
                      })
                      .filter(
                        (
                          item,
                        ): item is {
                          idx: number;
                          point: TrendPoint;
                          v: number;
                          status: PhysicalStatus;
                          pointColor: string;
                          x: number;
                          y: number;
                        } => item !== null,
                      );

                    const segments: (typeof coords)[] = [];
                    for (const coord of coords) {
                      const currentSegment = segments.at(-1);
                      if (
                        !currentSegment ||
                        coord.point.epochMs -
                          currentSegment[currentSegment.length - 1].point
                            .epochMs >
                          gapThresholdMs
                      ) {
                        segments.push([coord]);
                      } else {
                        currentSegment.push(coord);
                      }
                    }

                    const latestCoord = coords.at(-1);
                    return (
                      <g key={lineDef.label}>
                        {segments.map((segment, segIdx) =>
                          segment.length >= 2 ? (
                            <polyline
                              key={`${lineDef.label}-seg-${segIdx}`}
                              points={segment
                                .map((c) => `${c.x},${c.y}`)
                                .join(" ")}
                              fill="none"
                              stroke={lineDef.color}
                              strokeWidth="2"
                            />
                          ) : null,
                        )}
                        {coords.map((c) => (
                          <circle
                            key={`${lineDef.label}-${c.idx}`}
                            cx={c.x}
                            cy={c.y}
                            r={
                              hoverIndex === c.idx
                                ? 4.5
                                : coords.length === 1
                                  ? 4
                                  : 2.5
                            }
                            fill={c.pointColor}
                            stroke="#ffffff"
                            strokeWidth="1"
                            onMouseEnter={() => setHoverIndex(c.idx)}
                          >
                            <title>
                              {`${date(c.point.timestamp)} · ${lineDef.label}: ${c.v} ${lane.unit} (${c.status})`}
                            </title>
                          </circle>
                        ))}
                        {latestCoord && latestCoord.point.isLive && (
                          <circle
                            className="pulsing-dot"
                            cx={latestCoord.x}
                            cy={latestCoord.y}
                            r="5"
                            fill={latestCoord.pointColor}
                          />
                        )}
                      </g>
                    );
                  })}
                </g>
              );
            })}
            <g className="trend-x-axis">
              <line
                x1={leftX}
                y1={axisY}
                x2={rightX}
                y2={axisY}
                stroke="#cbd5e1"
                strokeWidth="1"
              />
              {xTicks.map((tick, tickIdx) => (
                <g key={`xtick-${tickIdx}`}>
                  <line
                    x1={tick.x}
                    y1={axisY}
                    x2={tick.x}
                    y2={axisY + 5}
                    stroke="#94a3b8"
                    strokeWidth="1"
                  />
                  <text
                    x={tick.x}
                    y={axisY + 18}
                    textAnchor={tick.anchor}
                    fontSize="9.5"
                    fill="#64748b"
                  >
                    {tick.label}
                  </text>
                </g>
              ))}
            </g>
            {showAiMarker && aiLastEpoch !== null && (
              <g>
                <line
                  x1={safeX(aiLastEpoch, minEpoch, maxEpoch, leftX, rightX)}
                  y1={topLaneTop}
                  x2={safeX(aiLastEpoch, minEpoch, maxEpoch, leftX, rightX)}
                  y2={axisY}
                  stroke="#b45309"
                  strokeWidth="1.5"
                  strokeDasharray="4 2"
                />
                <text
                  x={Math.min(
                    rightX - 70,
                    safeX(aiLastEpoch, minEpoch, maxEpoch, leftX, rightX) + 4,
                  )}
                  y={topLaneTop - 2}
                  fontSize="9"
                  fontWeight="600"
                  fill="#b45309"
                >
                  AI Last Seen
                </text>
              </g>
            )}
            {activePoint && (
              <line
                x1={safeX(
                  activePoint.epochMs,
                  minEpoch,
                  maxEpoch,
                  leftX,
                  rightX,
                )}
                y1={topLaneTop}
                x2={safeX(
                  activePoint.epochMs,
                  minEpoch,
                  maxEpoch,
                  leftX,
                  rightX,
                )}
                y2={axisY}
                stroke="#64748b"
                strokeWidth="1"
                strokeDasharray="2 2"
              />
            )}
          </svg>
          {laneStats.length > 0 && (
            <div className="feature-trend-stats">
              {isSingleLane ? (
                <>
                  <span>
                    <strong>Min:</strong>{" "}
                    {formatStatValue(laneStats[0].min, laneStats[0].decimals)}{" "}
                    {laneStats[0].unit}
                  </span>
                  <span>
                    <strong>Avg:</strong>{" "}
                    {formatStatValue(laneStats[0].avg, laneStats[0].decimals)}{" "}
                    {laneStats[0].unit}
                  </span>
                  <span>
                    <strong>Max:</strong>{" "}
                    {formatStatValue(laneStats[0].max, laneStats[0].decimals)}{" "}
                    {laneStats[0].unit}
                  </span>
                  <span>
                    <strong>Latest:</strong>{" "}
                    <span
                      className={`stat-status-${laneStats[0].status.toLowerCase()}`}
                    >
                      {formatStatValue(
                        laneStats[0].latest,
                        laneStats[0].decimals,
                      )}{" "}
                      {laneStats[0].unit} ({laneStats[0].status})
                    </span>
                  </span>
                </>
              ) : (
                laneStats.map((stat) => (
                  <span key={stat.id} className="stat-chip">
                    <strong>{stat.label}:</strong>{" "}
                    <span
                      className={`stat-status-${stat.status.toLowerCase()}`}
                    >
                      {formatStatValue(stat.latest, stat.decimals)} {stat.unit}
                    </span>{" "}
                    (Min {formatStatValue(stat.min, stat.decimals)} · Avg{" "}
                    {formatStatValue(stat.avg, stat.decimals)} · Max{" "}
                    {formatStatValue(stat.max, stat.decimals)})
                  </span>
                ))
              )}
            </div>
          )}
          {activePoint && (
            <div className="trend-readout" aria-live="polite">
              <span>
                <strong>Thời điểm:</strong> {date(activePoint.timestamp)}{" "}
                {activePoint.isLive ? "(Live SSE)" : "(M6 Archive)"}
              </span>
              <span>
                <strong>Torque:</strong>{" "}
                {activePoint.features.torque_nm !== undefined
                  ? `${activePoint.features.torque_nm.toFixed(1)} Nm`
                  : "—"}
              </span>
              <span>
                <strong>Speed:</strong>{" "}
                {activePoint.features.rotational_speed_rpm !== undefined
                  ? `${Math.round(activePoint.features.rotational_speed_rpm)} RPM`
                  : "—"}
              </span>
              <span>
                <strong>Temp (Air/Proc/ΔT):</strong>{" "}
                {activePoint.features.air_temperature_k !== undefined
                  ? activePoint.features.air_temperature_k.toFixed(1)
                  : "—"}
                {" / "}
                {activePoint.features.process_temperature_k !== undefined
                  ? activePoint.features.process_temperature_k.toFixed(1)
                  : "—"}
                {" / "}
                {activePoint.temp_delta_k !== null
                  ? `${activePoint.temp_delta_k.toFixed(1)} K`
                  : "—"}
              </span>
              <span>
                <strong>Wear:</strong>{" "}
                {activePoint.features.tool_wear_min !== undefined
                  ? `${Math.round(activePoint.features.tool_wear_min)} min`
                  : "—"}
              </span>
              <span>
                <strong>Power:</strong>{" "}
                {activePoint.mechanical_power_w !== null
                  ? `${Math.round(activePoint.mechanical_power_w)} W`
                  : "—"}
              </span>
            </div>
          )}
        </>
      )}
    </div>
  );
}

function HistoryChart({ value, more }: { value: Chart; more: () => void }) {
  const buckets = value.buckets;
  const samples = buckets.flatMap((item) =>
    item.min === null || item.max === null ? [] : [item.min, item.max],
  );
  const minimum = samples.reduce((a, b) => Math.min(a, b), Infinity),
    maximum = samples.reduce((a, b) => Math.max(a, b), -Infinity);
  const scale = Math.max(Math.abs(minimum), Math.abs(maximum), 1);
  const y = (v: number) =>
    minimum === maximum
      ? 90
      : 170 -
        150 *
          ((v / scale - minimum / scale) / (maximum / scale - minimum / scale));
  const start = Date.parse(buckets[0]?.start ?? value.as_of),
    end = Date.parse(buckets.at(-1)?.end ?? value.as_of);
  const x = (v: string) =>
    20 + (660 * (Date.parse(v) - start)) / Math.max(end - start, 1);
  const lines: string[][] = [[]];
  for (const item of buckets) {
    if (item.avg === null) {
      if (lines.at(-1)!.length) lines.push([]);
    } else
      lines.at(-1)!.push(`${(x(item.start) + x(item.end)) / 2},${y(item.avg)}`);
  }
  return (
    <>
      <p>
        {value.metric} · {value.unit} · Trục <code>{value.time_basis}</code> ·{" "}
        {value.resolution}. Đoạn trống giữ nguyên; cột thể hiện min/max, đường
        thể hiện trung bình.
      </p>
      <svg
        className="history-chart"
        viewBox="0 0 720 205"
        role="img"
        aria-label={`Biểu đồ ${value.metric}`}
      >
        <line x1="20" y1="180" x2="700" y2="180" stroke="currentColor" />
        {buckets.map(
          (item) =>
            item.min !== null &&
            item.max !== null && (
              <line
                key={item.start}
                x1={(x(item.start) + x(item.end)) / 2}
                x2={(x(item.start) + x(item.end)) / 2}
                y1={y(item.min)}
                y2={y(item.max)}
                stroke="#94a3b8"
                strokeWidth="3"
              >
                <title>
                  {date(item.start)}: min {item.min}, max {item.max}, count{" "}
                  {item.count}, invalid {item.invalid_count}
                </title>
              </line>
            ),
        )}
        {lines
          .filter((line) => line.length)
          .map((line, index) => (
            <polyline
              key={index}
              points={line.join(" ")}
              fill="none"
              stroke="#2563eb"
              strokeWidth="2"
            />
          ))}
        {value.markers
          .filter(
            (marker) =>
              marker.position_at !== null &&
              Date.parse(marker.position_at) >= start &&
              Date.parse(marker.position_at) < end,
          )
          .map((marker) => (
            <circle
              key={marker.kind + marker.reference_id}
              cx={x(marker.position_at!)}
              cy="190"
              r="4"
              fill={marker.kind === "ALERT" ? "#dc2626" : "#2563eb"}
            >
              <title>
                {marker.kind}: {marker.reference_id}
              </title>
            </circle>
          ))}
      </svg>
      {!samples.length && (
        <p>Không có mẫu số hợp lệ; không suy thành giá trị 0.</p>
      )}
      <p>
        {buckets.reduce((sum, item) => sum + item.count, 0)} mẫu số hợp lệ ·{" "}
        {buckets.reduce((sum, item) => sum + item.invalid_count, 0)} mẫu không
        hợp lệ
      </p>
      <details>
        <summary>Markers ({value.markers.length})</summary>
        <ul>
          {value.markers.map((marker) => (
            <li key={marker.kind + marker.reference_id}>
              <strong>{marker.kind}</strong> <code>{marker.reference_id}</code>
              <p>
                Vị trí:{" "}
                {marker.position_at
                  ? date(marker.position_at)
                  : "Chưa có thời gian đúng trục"}{" "}
                · Nguồn: {marker.source_event_id}
              </p>
              <p>
                Cảm biến: {date(marker.source_received_at)} · Ingestion:{" "}
                {date(marker.source_server_accepted_at)} · Processing:{" "}
                {date(marker.processed_at)}
              </p>
            </li>
          ))}
        </ul>
      </details>
      {value.markers_next_cursor && (
        <button className="button secondary" onClick={more}>
          Thêm markers
        </button>
      )}
    </>
  );
}
