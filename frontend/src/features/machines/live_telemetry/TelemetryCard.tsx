import { rawText } from "../../../api/codec";
import type { Telemetry } from "../../../types/live";
import { age, Badge, Empty, Panel, Source } from "../../../shared/ui";
import { freshness } from "../../../realtime/freshness";

const fields: Record<string, [string, string]> = {
  product_type: ["Loại sản phẩm", ""],
  air_temperature_k: ["Nhiệt độ không khí", "K"],
  process_temperature_k: ["Nhiệt độ quá trình", "K"],
  rotational_speed_rpm: ["Tốc độ quay", "rpm"],
  torque_nm: ["Mô-men xoắn", "Nm"],
  tool_wear_min: ["Độ mòn dụng cụ", "phút"],
};
export function TelemetryCard({
  value,
  now,
  freshMs,
  staleMs,
}: {
  value: Telemetry | null;
  now: number;
  freshMs: number;
  staleMs: number;
}) {
  const status = freshness(value?.server_accepted_at, now, freshMs, staleMs);
  return (
    <Panel
      title={value && status !== "FRESH" ? "Last Telemetry" : "Live Telemetry"}
      eyebrow="01 / LATEST RECEIVED READING"
      action={
        value && (
          <>
            <Badge value={status} />
            <Badge value={value.validation_status} />
          </>
        )
      }
    >
      {!value ? (
        <Empty>Chưa nhận telemetry.</Empty>
      ) : (
        <>
          <div className="sensor-grid">
            {Object.entries(fields).map(([key, [label, unit]]) => (
              <div className="sensor-value" key={key}>
                <span>{label}</span>
                <strong>
                  {key in value.features_raw
                    ? typeof value.features_raw[key] === "string"
                      ? String(value.features_raw[key])
                      : rawText(value.features_raw[key])
                    : "—"}
                  <small>{unit}</small>
                </strong>
              </div>
            ))}
          </div>
          <div className="panel-footer">
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
