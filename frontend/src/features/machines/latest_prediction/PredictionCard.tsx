import type { Prediction, Telemetry } from "../../../types/live";
import { freshness } from "../../../realtime/freshness";
import { age, Badge, date, Empty, Panel, Source } from "../../../shared/ui";

export function PredictionCard({
  value,
  telemetry,
  now,
  freshMs,
  staleMs,
}: {
  value: Prediction | null;
  telemetry: Telemetry | null;
  now: number;
  freshMs: number;
  staleMs: number;
}) {
  const historical =
    !!value && !!telemetry && value.source_event_id !== telemetry.event_id;
  return (
    <Panel
      title="Latest Valid Prediction"
      eyebrow="02 / LATEST COMPLETED RESULT"
      action={
        <Badge
          value={freshness(
            value?.source_server_accepted_at,
            now,
            freshMs,
            staleMs,
          )}
        />
      }
    >
      {!value ? (
        <Empty>Chưa có prediction hợp lệ. NOT_AVAILABLE</Empty>
      ) : (
        <>
          <div className="prediction-result">
            <Badge value={value.prediction} />
            <div className="probability">
              {(value.failure_probability * 100).toFixed(1)}
              <span>%</span>
            </div>
            <p>Xác suất lỗi từ model · không phải risk score</p>
          </div>
          {historical && (
            <div className="historical">
              Based on older data · Prediction thuộc reading trước.
            </div>
          )}
          <div className="panel-footer">
            <Source label="Dựa trên event" value={value.source_event_id} />
            <Source label="Model" value={value.model_version} />
            <Source label="Risk policy" value={value.policy_version} />
            <Source
              label="Trạng thái tại prediction"
              value={value.machine_state_at_prediction}
            />
            <p className="age-label">
              Tuổi dữ liệu nguồn: {age(value.source_server_accepted_at, now)}
            </p>
            <p className="hint">Hoàn tất: {date(value.completed_at)}</p>
          </div>
        </>
      )}
    </Panel>
  );
}
