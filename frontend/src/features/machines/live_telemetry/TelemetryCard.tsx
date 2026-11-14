import { rawText } from "../../../api/codec";
import type { Telemetry } from "../../../types/live";
import { age, Badge, Empty, Panel, Source } from "../../../shared/ui";
import { freshness } from "../../../realtime/freshness";
import {
  evaluatePhysicalHealth,
  FEATURE_METADATA,
  type MetricKey,
  type PhysicalStatus,
  type TrendFeatureKey,
} from "../../../shared/physical-guardrails";

const fields: Record<string, [string, string, MetricKey?]> = {
  product_type: ["Loại sản phẩm", ""],
  air_temperature_k: ["Nhiệt độ không khí", "K", "air_temperature_k"],
  process_temperature_k: ["Nhiệt độ quá trình", "K", "process_temperature_k"],
  rotational_speed_rpm: ["Tốc độ quay", "rpm", "rotational_speed_rpm"],
  torque_nm: ["Mô-men xoắn", "Nm", "torque_nm"],
  tool_wear_min: ["Độ mòn dụng cụ", "phút", "tool_wear_min"],
};

function barColor(status: PhysicalStatus): string {
  return status === "CRITICAL"
    ? "#dc2626"
    : status === "WARNING"
      ? "#d97706"
      : "#168b75";
}

function ThresholdBar({
  percent,
  status,
}: {
  percent: number;
  status: PhysicalStatus;
}) {
  return (
    <svg
      className="threshold-bar"
      viewBox="0 0 100 6"
      preserveAspectRatio="none"
      aria-hidden="true"
    >
      <rect x="0" y="0" width="70" height="6" fill="#e6f6ef" rx="2" />
      <rect x="70" y="0" width="18" height="6" fill="#fff4de" />
      <rect x="88" y="0" width="12" height="6" fill="#fcebed" rx="2" />
      <rect
        x="0"
        y="0"
        width={Math.max(4, Math.min(100, percent))}
        height="6"
        fill={barColor(status)}
        rx="2"
      />
    </svg>
  );
}

export function TelemetryCard({
  value,
  now,
  freshMs,
  staleMs,
  onSelectFeature,
}: {
  value: Telemetry | null;
  now: number;
  freshMs: number;
  staleMs: number;
  onSelectFeature?: (feature: TrendFeatureKey) => void;
}) {
  const status = freshness(value?.server_accepted_at, now, freshMs, staleMs);
  const evaluation = value ? evaluatePhysicalHealth(value.features_raw) : null;
  const physicalTone =
    evaluation?.status === "CRITICAL"
      ? "bad"
      : evaluation?.status === "WARNING"
        ? "warn"
        : "good";
  return (
    <Panel
      title={value && status !== "FRESH" ? "Last Telemetry" : "Live Telemetry"}
      eyebrow="01 / LATEST RECEIVED READING"
      action={
        value && (
          <div className="panel-badges">
            <Badge value={status} />
            <Badge value={value.validation_status} />
            {evaluation && (
              <span className={`badge ${physicalTone}`}>
                <span className="dot" />
                {evaluation.primaryBadge}
              </span>
            )}
          </div>
        )
      }
    >
      {!value ? (
        <Empty>Chưa nhận telemetry.</Empty>
      ) : (
        <>
          <div className="sensor-grid">
            {Object.entries(fields).map(([key, [label, unit, metricKey]]) => {
              const metric = metricKey
                ? evaluation?.metrics[metricKey]
                : undefined;
              const meta = metricKey ? FEATURE_METADATA[metricKey] : undefined;
              return (
                <div
                  className={`sensor-value ${metric?.status === "CRITICAL" ? "sensor-critical" : metric?.status === "WARNING" ? "sensor-warning" : ""}`}
                  key={key}
                >
                  <div className="sensor-header-row">
                    <span>{label}</span>
                    {meta && onSelectFeature && (
                      <button
                        type="button"
                        className="trend-link-btn"
                        onClick={() => onSelectFeature(meta.trendKey)}
                      >
                        📈 View Trend
                      </button>
                    )}
                  </div>
                  <strong>
                    {key in value.features_raw
                      ? typeof value.features_raw[key] === "string"
                        ? String(value.features_raw[key])
                        : rawText(value.features_raw[key])
                      : "—"}
                    <small>{unit}</small>
                  </strong>
                  {metric && (
                    <ThresholdBar
                      percent={metric.percent}
                      status={metric.status}
                    />
                  )}
                </div>
              );
            })}
            {evaluation && (
              <>
                <div
                  className={`sensor-value derived-sensor ${evaluation.metrics.temp_delta_k?.status === "CRITICAL" ? "sensor-critical" : evaluation.metrics.temp_delta_k?.status === "WARNING" ? "sensor-warning" : ""}`}
                >
                  <div className="sensor-header-row">
                    <span>Temp Delta (ΔT)</span>
                    {onSelectFeature && (
                      <button
                        type="button"
                        className="trend-link-btn"
                        onClick={() => onSelectFeature("temperatures")}
                      >
                        📈 View Trend
                      </button>
                    )}
                  </div>
                  <strong>
                    {evaluation.derived.temp_delta_k !== null
                      ? evaluation.derived.temp_delta_k.toFixed(1)
                      : "—"}
                    <small>K</small>
                  </strong>
                  {evaluation.metrics.temp_delta_k && (
                    <ThresholdBar
                      percent={evaluation.metrics.temp_delta_k.percent}
                      status={evaluation.metrics.temp_delta_k.status}
                    />
                  )}
                </div>
                <div
                  className={`sensor-value derived-sensor ${evaluation.metrics.mechanical_power_w?.status === "CRITICAL" ? "sensor-critical" : evaluation.metrics.mechanical_power_w?.status === "WARNING" ? "sensor-warning" : ""}`}
                >
                  <div className="sensor-header-row">
                    <span>Est. Power (P)</span>
                    {onSelectFeature && (
                      <button
                        type="button"
                        className="trend-link-btn"
                        onClick={() => onSelectFeature("torque_nm")}
                      >
                        📈 View Trend
                      </button>
                    )}
                  </div>
                  <strong>
                    {evaluation.derived.mechanical_power_w !== null
                      ? Math.round(
                          evaluation.derived.mechanical_power_w,
                        ).toString()
                      : "—"}
                    <small>W</small>
                  </strong>
                  {evaluation.metrics.mechanical_power_w && (
                    <ThresholdBar
                      percent={evaluation.metrics.mechanical_power_w.percent}
                      status={evaluation.metrics.mechanical_power_w.status}
                    />
                  )}
                </div>
              </>
            )}
          </div>
          <div className="panel-footer">
            {evaluation && evaluation.triggeredReasons.length > 0 && (
              <div className="physical-alert-summary">
                <strong>Cảnh báo ngưỡng vật lý:</strong>{" "}
                {evaluation.triggeredReasons.join(" · ")}
              </div>
            )}
            <Source label="Event nguồn" value={value.event_id} />
            <Source label="Sequence" value={value.sequence_number.toString()} />
            <p className="age-label">
              Nhận {age(value.server_accepted_at, now)}
            </p>
            {value.validation_status !== "VALID" && (
              <p className="hint">Dữ liệu thô chưa được xác nhận hợp lệ.</p>
            )}
            <details>
              <summary>Xem raw features</summary>
              <pre>{rawText(value.features_raw)}</pre>
            </details>
          </div>
        </>
      )}
    </Panel>
  );
}
