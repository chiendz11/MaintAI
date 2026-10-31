import { useAlertRefresh } from "../../alerts/useRefresh";
import { useEffect, useState } from "react";
import { ArrowLeft, RefreshCw } from "lucide-react";
import type { MachineApi } from "../../../api/client";
import type { MachineDetail } from "../../../types/live";
import { emptyLive } from "../../../realtime/reducers";
import { watchMachine, type Connection } from "../../../realtime/sse";
import type { RuntimeConfig } from "../../../app/config";
import { Badge, ErrorNotice } from "../../../shared/ui";
import { TelemetryCard } from "../live_telemetry/TelemetryCard";
import { PredictionCard } from "../latest_prediction/PredictionCard";
import { ProcessingCard } from "../processing_status/ProcessingCard";
import { HistoryPanel } from "./HistoryPanel";

export function MachinePage({
  machine,
  api,
  config,
  back,
  connection,
}: {
  machine: string;
  api: MachineApi;
  config: RuntimeConfig;
  back: () => void;
  connection: (value: Connection) => void;
}) {
  const [value, setValue] = useState(() => emptyLive(machine)),
    [detail, setDetail] = useState<MachineDetail | null>(null),
    [error, setError] = useState<unknown>(null),
    [metadataError, setMetadataError] = useState(false),
    [revision, setRevision] = useState(0),
    [now, setNow] = useState(Date.now());
  const [alertRevision] = useAlertRefresh();
  const [historyOpen, setHistoryOpen] = useState(false);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  useEffect(() => {
    setValue(emptyLive(machine));
    setError(null);
    setDetail(null);
    setMetadataError(false);
    const stop = watchMachine(api, machine, {
      state: setValue,
      connection,
      error: setError,
    });
    return () => {
      stop();
    };
  }, [api, machine, revision, connection]);
  useEffect(() => {
    const request = new AbortController();
    api
      .detail(machine, request.signal)
      .then((result) => {
        if (!request.signal.aborted) setDetail(result);
      })
      .catch(() => {
        if (!request.signal.aborted) setMetadataError(true);
      });
    return () => request.abort();
  }, [api, machine, revision, alertRevision]);

  return (
    <>
      <button className="back-link" onClick={back}>
        <ArrowLeft size={15} />
        Danh sách thiết bị
      </button>
      <div className="page-heading">
        <div>
          <p className="eyebrow">THIẾT BỊ / CHI TIẾT</p>
          <h1>{detail?.display_name ?? machine}</h1>
          <p className="subtitle">Ba nguồn dữ liệu. Một góc nhìn rõ ràng.</p>
        </div>
        <button
          className="button secondary"
          onClick={() => setRevision((count) => count + 1)}
        >
          <RefreshCw size={16} />
          Đồng bộ lại
        </button>
      </div>
      <div className="machine-context">
        <span className="machine-id">{machine}</span>
        {detail && <Badge value={detail.state} />}
        <span>
          {detail
            ? `${detail.active_alerts.length} active alerts`
            : metadataError
              ? "Thông tin bổ sung chưa khả dụng"
              : "Đang tải thông tin bổ sung…"}
        </span>
      </div>
      <ErrorNotice
        error={error}
        retry={() => setRevision((count) => count + 1)}
      />
      <div className="live-grid">
        <TelemetryCard
          value={value.telemetry}
          now={now}
          freshMs={config.freshMs}
          staleMs={config.staleMs}
        />
        <PredictionCard
          value={value.latest_valid_prediction}
          telemetry={value.telemetry}
          now={now}
          freshMs={config.freshMs}
          staleMs={config.staleMs}
        />
      </div>
      <ProcessingCard value={value.latest_processing} />
      <button
        className="button secondary"
        onClick={() => setHistoryOpen((open) => !open)}
      >
        Lịch sử
      </button>
      {historyOpen && api.history && (
        <HistoryPanel key={machine} api={api.history} machine={machine} />
      )}
      <p className="snapshot-note">
        REST snapshot:{" "}
        {value.as_of
          ? new Date(value.as_of).toLocaleString("vi-VN")
          : "Chưa đồng bộ"}{" "}
        · Mỗi vùng giữ source event và tuổi dữ liệu riêng.
      </p>
    </>
  );
}
