import { useEffect, useRef, useState } from "react";
import { ArrowLeft, RefreshCw } from "lucide-react";
import { ApiError } from "../../api/client";
import { rawText } from "../../api/codec";
import {
  allowedActions,
  invalidateAlerts,
  UnknownOutcome,
} from "../../realtime/alerts";
import type {
  AlertApi,
  AlertAction,
  AlertDetail,
  Feedback,
} from "../../types/alerts";
import { Badge, date, ErrorNotice, Panel, Source } from "../../shared/ui";
import { useAlertRefresh } from "./useRefresh";

const labels: Record<AlertAction, string> = {
  ack: "Tiếp nhận",
  start: "Bắt đầu xử lý",
  escalate: "Chuyển cấp",
  resolve: "Hoàn tất xử lý",
  "false-alert": "Cảnh báo không xác nhận",
};
export function AlertDetailPage({
  api,
  id,
  back,
  actor,
  canManage,
}: {
  api: AlertApi;
  id: string;
  back: () => void;
  actor: string;
  canManage: boolean;
}) {
  const [detail, setDetail] = useState<AlertDetail | null>(null),
    [error, setError] = useState<unknown>(null),
    [notice, setNotice] = useState("");
  const [pending, setPending] = useState(false),
    [selected, setSelected] = useState<AlertAction>("ack"),
    [note, setNote] = useState(""),
    [reason, setReason] = useState("");
  const [verdict, setVerdict] =
      useState<Feedback["technician_verdict"]>("INCONCLUSIVE"),
    [recommendation, setRecommendation] =
      useState<Feedback["recommendation_feedback"]>("NOT_AVAILABLE");
  const [revision, refresh] = useAlertRefresh();
  const lifetime = useRef(new AbortController());
  useEffect(() => {
    const request = new AbortController();
    lifetime.current = request;
    return () => request.abort();
  }, [id, api]);
  useEffect(() => {
    const request = new AbortController();
    setError(null);
    api
      .alert(id, request.signal)
      .then((value) => {
        if (!request.signal.aborted) setDetail(value);
      })
      .catch((caught) => {
        if (!request.signal.aborted) {
          setError(caught);
          setDetail(null);
        }
      });
    return () => request.abort();
  }, [api, id, revision]);
  const actions = detail ? allowedActions[detail.alert.status] : [];
  const action = actions.includes(selected) ? selected : actions[0];
  async function perform(event: React.FormEvent, feedback = false) {
    event.preventDefault();
    if (pending || !detail || !canManage) return;
    setPending(true);
    setError(null);
    setNotice("");
    const signal = lifetime.current.signal;
    try {
      if (feedback) {
        await api.feedback(
          id,
          {
            technician_verdict: verdict,
            recommendation_feedback: recommendation,
            note,
          },
          signal,
        );
        if (!signal.aborted) setNotice("Đã ghi nhận feedback.");
      } else if (action) {
        await api.action(
          id,
          action,
          {
            actor_id: actor,
            note,
            ...(action === "resolve"
              ? { resolution: reason }
              : action === "false-alert"
                ? { reason }
                : {}),
          },
          signal,
        );
        if (!signal.aborted) setNotice("Đã lưu thao tác và lịch sử xử lý.");
      }
      if (!signal.aborted) {
        invalidateAlerts();
        refresh();
      }
    } catch (caught) {
      if (signal.aborted) return;
      if (caught instanceof UnknownOutcome) {
        setNotice(caught.message);
        if (!caught.feedback) refresh();
      } else if (caught instanceof ApiError && caught.status === 409) {
        setNotice(
          "Trạng thái đã thay đổi. Đang tải lại cảnh báo; thao tác chưa được áp dụng.",
        );
        refresh();
      } else setError(caught);
    } finally {
      if (!signal.aborted) setPending(false);
    }
  }
  return (
    <>
      <button className="back-link" onClick={back}>
        <ArrowLeft size={15} />
        Danh sách cảnh báo
      </button>
      <div className="page-heading">
        <div>
          <p className="eyebrow">ALERT / EVIDENCE & LIFECYCLE</p>
          <h1>Chi tiết cảnh báo</h1>
          <p className="subtitle">{id}</p>
        </div>
        <button className="button secondary" onClick={refresh}>
          <RefreshCw size={16} />
          Làm mới
        </button>
      </div>
      <ErrorNotice error={error} retry={refresh} />
      {notice && (
        <p className="notice" role="status">
          {notice}
        </p>
      )}
      {!detail && !error && <p role="status">Đang tải cảnh báo…</p>}
      {detail && (
        <>
          <div className="machine-context">
            <strong>{detail.alert.machine_id}</strong>
            <Badge value={detail.alert.severity} />
            <Badge value={detail.alert.status} />
            <span>
              Risk lúc tạo: {(detail.alert.risk_score * 100).toFixed(1)}%
            </span>
          </div>
          <div className="alert-grid">
            <Panel title="Evidence gốc" eyebrow="IMMUTABLE SOURCE">
              <Source
                label="Source event"
                value={detail.sensor_event.event_id}
              />
              <Source
                label="Prediction"
                value={detail.prediction.prediction_id}
              />
              <Source
                label="Model version"
                value={detail.alert.model_version}
              />
              <Source
                label="Policy version"
                value={detail.alert.policy_version}
              />
              <p>Được tạo: {date(detail.alert.created_at)}</p>
              <p>
                Prediction: {detail.prediction.prediction} · Probability:{" "}
                {(detail.prediction.failure_probability * 100).toFixed(1)}%
              </p>
              <pre className="evidence-json">
                {rawText(detail.sensor_event.features)}
              </pre>
            </Panel>
            <Panel title="Technician Copilot" eyebrow="ASSISTANCE">
              <Badge value={detail.copilot.status} />
              <p className="subtitle">
                Khuyến nghị chưa khả dụng. Bạn vẫn có thể kiểm tra evidence và
                xử lý cảnh báo.
              </p>
            </Panel>
          </div>
          <Panel title="Lịch sử xử lý" eyebrow="AUDIT TIMELINE">
            <ol className="alert-timeline">
              {detail.history.map((event) => (
                <li key={event.alert_event_id}>
                  <Badge value={event.to_status} />
                  <div>
                    <strong>{event.event_type}</strong>
                    <p>
                      {date(event.timestamp)} · {event.actor_id ?? "Hệ thống"}
                    </p>
                    {event.note && <p>{event.note}</p>}
                  </div>
                </li>
              ))}
            </ol>
          </Panel>
          {canManage ? (
            <div className="alert-grid">
              <Panel title="Cập nhật trạng thái" eyebrow="LIFECYCLE ACTION">
                {actions.length ? (
                  <form
                    className="alert-form"
                    onSubmit={(event) => void perform(event)}
                  >
                    <label>
                      Thao tác
                      <select
                        aria-label="Thao tác cảnh báo"
                        value={action}
                        disabled={pending}
                        onChange={(e) => {
                          setSelected(e.target.value as AlertAction);
                          setReason("");
                        }}
                      >
                        {actions.map((v) => (
                          <option key={v} value={v}>
                            {labels[v]}
                          </option>
                        ))}
                      </select>
                    </label>
                    {(action === "resolve" || action === "false-alert") && (
                      <label>
                        {action === "resolve" ? "Kết quả xử lý" : "Lý do"}
                        <textarea
                          aria-label={
                            action === "resolve" ? "Kết quả xử lý" : "Lý do"
                          }
                          value={reason}
                          onChange={(e) => setReason(e.target.value)}
                          required
                          disabled={pending}
                        />
                      </label>
                    )}
                    <label>
                      Ghi chú
                      <textarea
                        aria-label="Ghi chú thao tác"
                        value={note}
                        disabled={pending}
                        onChange={(e) => setNote(e.target.value)}
                      />
                    </label>
                    <button
                      className="button primary"
                      disabled={pending}
                      type="submit"
                    >
                      {pending ? "Đang gửi…" : labels[action]}
                    </button>
                  </form>
                ) : (
                  <p className="empty">
                    Cảnh báo đã đóng. Không thể chuyển trạng thái thêm.
                  </p>
                )}
              </Panel>
              <Panel
                title="Ghi nhận kết quả kiểm tra"
                eyebrow="TECHNICIAN FEEDBACK"
              >
                <form
                  className="alert-form"
                  onSubmit={(event) => void perform(event, true)}
                >
                  <label>
                    Kết luận
                    <select
                      aria-label="Kết luận kiểm tra"
                      value={verdict}
                      disabled={pending}
                      onChange={(e) =>
                        setVerdict(
                          e.target.value as Feedback["technician_verdict"],
                        )
                      }
                    >
                      {["CONFIRMED_ISSUE", "FALSE_ALERT", "INCONCLUSIVE"].map(
                        (v) => (
                          <option key={v}>{v}</option>
                        ),
                      )}
                    </select>
                  </label>
                  <label>
                    Đánh giá khuyến nghị
                    <select
                      aria-label="Đánh giá khuyến nghị"
                      value={recommendation}
                      disabled={pending}
                      onChange={(e) =>
                        setRecommendation(
                          e.target.value as Feedback["recommendation_feedback"],
                        )
                      }
                    >
                      {[
                        "NOT_AVAILABLE",
                        "USEFUL",
                        "PARTIALLY_USEFUL",
                        "NOT_USEFUL",
                      ].map((v) => (
                        <option key={v}>{v}</option>
                      ))}
                    </select>
                  </label>
                  <button
                    className="button secondary"
                    disabled={pending}
                    type="submit"
                  >
                    Gửi feedback
                  </button>
                </form>
              </Panel>
            </div>
          ) : (
            <p className="notice">
              Bạn có quyền xem cảnh báo. Thao tác xử lý dành cho kỹ thuật viên
              hoặc quản trị viên.
            </p>
          )}
        </>
      )}
    </>
  );
}
