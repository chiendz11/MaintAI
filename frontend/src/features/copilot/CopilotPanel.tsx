import { useEffect, useRef, useState } from "react";
import {
  BriefUnknown,
  type Copilot,
  type Intent,
  type M7Client,
} from "../../api/m7-client";
import { Badge, Panel } from "../../shared/ui";
import { changes } from "../../realtime/alerts";

const labels: Record<Intent, string> = {
  MAINTENANCE_BRIEF: "Tóm tắt bảo trì",
  TECHNICIAN_CHECKLIST: "Checklist kỹ thuật",
  ALERT_EXPLANATION: "Giải thích cảnh báo",
};
export function CopilotPanel({
  api,
  alert,
  canManage,
}: {
  api?: M7Client;
  alert: string;
  canManage: boolean;
}) {
  const [value, setValue] = useState<Copilot | null>(null),
    [intent, setIntent] = useState<Intent>("MAINTENANCE_BRIEF");
  const [busy, setBusy] = useState(false),
    [unknown, setUnknown] = useState(false),
    [message, setMessage] = useState("");
  const [revision, setRevision] = useState(0),
    [job, setJob] = useState<string | null>(null);
  const request = useRef<{ key: string; intent: Intent } | null>(null);
  const lifetime = useRef(new AbortController());
  useEffect(() => {
    const controller = new AbortController();
    lifetime.current = controller;
    return () => controller.abort();
  }, [api, alert]);
  useEffect(() => {
    const refresh = () => setRevision((v) => v + 1);
    changes.addEventListener("change", refresh);
    return () => changes.removeEventListener("change", refresh);
  }, []);
  useEffect(() => {
    if (!api) return;
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    let count = 0;
    const read = async () => {
      try {
        const result = await api.brief(alert, controller.signal);
        const state = job ? await api.job(job, controller.signal) : null;
        if (controller.signal.aborted) return;
        setValue(result);
        const pending = state
          ? ["QUEUED", "RUNNING"].includes(state.status)
          : result.copilot_status === "PENDING";
        if (pending && count++ < 100)
          timer = setTimeout(() => void read(), 3000);
        if (!pending && job) {
          setJob(null);
          request.current = null;
        }
        if (pending && count >= 100)
          setMessage(
            "Đã dừng chờ tự động. Chọn đọc lại để kiểm tra trạng thái.",
          );
      } catch {
        if (!controller.signal.aborted)
          setMessage(
            "Chưa đọc được trạng thái Copilot. Các thao tác cảnh báo vẫn khả dụng.",
          );
      }
    };
    void read();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [api, alert, revision, job]);
  async function generate() {
    if (!api || busy) return;
    const frozen = request.current ?? { key: crypto.randomUUID(), intent };
    request.current = frozen;
    setBusy(true);
    setMessage("");
    try {
      const accepted = await api.generate(
        alert,
        frozen.intent,
        frozen.key,
        lifetime.current.signal,
      );
      if (lifetime.current.signal.aborted) return;
      setUnknown(false);
      setJob(accepted.job_id);
      setValue({ copilot_status: "PENDING", message: "PENDING", brief: null });
      setRevision((v) => v + 1);
    } catch (error) {
      if (!lifetime.current.signal.aborted) {
        setUnknown(error instanceof BriefUnknown);
        setMessage(
          error instanceof Error ? error.message : "Không gửi được yêu cầu.",
        );
        if (!(error instanceof BriefUnknown)) request.current = null;
        setRevision((v) => v + 1);
      }
    } finally {
      if (!lifetime.current.signal.aborted) setBusy(false);
    }
  }
  return (
    <Panel title="Technician Copilot" eyebrow="ASSISTANCE">
      <Badge
        value={
          api
            ? (value?.brief?.grounded_status ??
              value?.copilot_status ??
              "Đang tải")
            : "UNAVAILABLE"
        }
      />
      {!api && <p>Copilot chưa khả dụng trong nguồn dữ liệu này.</p>}
      {message && <p role="status">{message}</p>}
      {value?.brief && (
        <>
          <p>{value.brief.summary}</p>
          <p className="subtitle">{value.brief.uncertainty}</p>
          {value.brief.grounded_status === "INSUFFICIENT_EVIDENCE" && (
            <p>Chưa đủ bằng chứng để đưa ra checklist.</p>
          )}
          {value.brief.grounded_status === "VALIDATION_FAILED" && (
            <p>Khuyến nghị không vượt qua kiểm tra nguồn trích dẫn.</p>
          )}
          <ol>
            {value.brief.checklist.map((step) => (
              <li key={step.step}>
                {step.action} <small>{step.citation_ids.join(", ")}</small>
              </li>
            ))}
          </ol>
          {value.brief.citations.length > 0 && (
            <>
              <h3>Nguồn trích dẫn</h3>
              <ul>
                {value.brief.citations.map((citation) => (
                  <li key={citation.citation_id}>
                    {citation.citation_id}: {citation.document_id} · phiên bản{" "}
                    {citation.document_version} · chunk {citation.chunk_id}
                    {citation.page != null && ` · trang ${citation.page}`}
                    {citation.section && ` · ${citation.section}`}
                  </li>
                ))}
              </ul>
            </>
          )}
        </>
      )}
      {api && (
        <div className="alert-form">
          {canManage && (
            <>
              <label>
                Loại yêu cầu
                <select
                  aria-label="Loại yêu cầu Copilot"
                  value={intent}
                  disabled={busy || unknown || !!job}
                  onChange={(e) => setIntent(e.target.value as Intent)}
                >
                  {Object.entries(labels).map(([key, label]) => (
                    <option key={key} value={key}>
                      {label}
                    </option>
                  ))}
                </select>
              </label>
              <button
                className="button primary"
                disabled={busy || !!job}
                onClick={() => void generate()}
              >
                {busy
                  ? "Đang gửi…"
                  : unknown
                    ? "Thử lại cùng yêu cầu"
                    : "Tạo khuyến nghị"}
              </button>
            </>
          )}
          <button
            className="button secondary"
            onClick={() => setRevision((v) => v + 1)}
          >
            Đọc lại Copilot
          </button>
        </div>
      )}
    </Panel>
  );
}
