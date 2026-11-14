import { useAlertRefresh } from "../../alerts/useRefresh";
import { useCallback, useEffect, useState } from "react";
import { ArrowLeft, RefreshCw } from "lucide-react";
import type { MachineApi } from "../../../api/client";
import type { MachineDetail, Telemetry } from "../../../types/live";
import { emptyLive } from "../../../realtime/reducers";
import { watchMachine, type Connection } from "../../../realtime/sse";
import { freshness } from "../../../realtime/freshness";
import type { RuntimeConfig } from "../../../app/config";
import type { TrendFeatureKey } from "../../../shared/physical-guardrails";
import { Badge, ErrorNotice } from "../../../shared/ui";
import { TelemetryCard } from "../live_telemetry/TelemetryCard";
import { PredictionCard } from "../latest_prediction/PredictionCard";
import { ProcessingCard } from "../processing_status/ProcessingCard";
import { HistoryPanel } from "./HistoryPanel";

const MAX_LIVE_BUFFER = 30;

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
    [liveBuffer, setLiveBuffer] = useState<Telemetry[]>([]),
    [selectedFeature, setSelectedFeature] = useState<TrendFeatureKey | null>(
      null,
    ),
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
    setLiveBuffer([]);
    setSelectedFeature(null);
    setError(null);
    setDetail(null);
    setMetadataError(false);
    const stop = watchMachine(api, machine, {
      state: (snapshot) => {
        setValue(snapshot);
        if (snapshot.telemetry) {
          const incoming = snapshot.telemetry;
          setLiveBuffer((prev) => {
            const filtered = prev.filter(
              (item) => item.event_id !== incoming.event_id,
            );
            const next = [...filtered, incoming].sort(
              (a, b) =>
                Date.parse(a.received_at || a.server_accepted_at) -
                Date.parse(b.received_at || b.server_accepted_at),
            );
            return next.slice(-MAX_LIVE_BUFFER);
          });
        }
      },
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

  const handleSelectFeature = useCallback((feature: TrendFeatureKey) => {
    setSelectedFeature(feature);
    setHistoryOpen(true);
  }, []);

  const predictionStatus = freshness(
    value.latest_valid_prediction?.source_server_accepted_at,
    now,
    config.freshMs,
    config.staleMs,
  );
  const aiDegraded =
    value.latest_processing?.processing_status === "FAILED" ||
    predictionStatus === "STALE" ||
    (Boolean(value.latest_valid_prediction) &&
      Boolean(value.telemetry) &&
      value.latest_valid_prediction!.source_event_id !==
        value.telemetry!.event_id);
  const aiLastSeenAt =
    value.latest_valid_prediction?.completed_at ??
    value.latest_processing?.updated_at ??
    null;

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
          onSelectFeature={handleSelectFeature}
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
        <HistoryPanel
          key={machine}
          api={api.history}
          machine={machine}
          liveTelemetry={liveBuffer}
          selectedFeature={selectedFeature}
          onSelectFeature={setSelectedFeature}
          aiLastSeenAt={aiLastSeenAt}
          aiDegraded={aiDegraded}
        />
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
