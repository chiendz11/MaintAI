import { useEffect, useState } from "react";
import {
  type DriftPage,
  type DriftReport,
  type M7Client,
  type Monitoring,
} from "../../api/m7-client";
import { Badge, date, Panel } from "../../shared/ui";
import { changes } from "../../realtime/alerts";

export function MonitoringPage({ api }: { api?: M7Client }) {
  const [summary, setSummary] = useState<Monitoring | null>(null),
    [page, setPage] = useState<DriftPage | null>(null);
  const [report, setReport] = useState<DriftReport | null>(null),
    [selected, setSelected] = useState<string | null>(null);
  const [model, setModel] = useState(""),
    [status, setStatus] = useState(""),
    [cursor, setCursor] = useState<string | undefined>();
  const [error, setError] = useState(""),
    [revision, setRevision] = useState(0);
  useEffect(() => {
    const refresh = () => setRevision((v) => v + 1);
    changes.addEventListener("change", refresh);
    const timer = setInterval(refresh, 60000);
    return () => {
      changes.removeEventListener("change", refresh);
      clearInterval(timer);
    };
  }, []);
  useEffect(() => {
    if (!api) return;
    const controller = new AbortController();
    setError("");
    void api
      .summary(controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setSummary(value);
      })
      .catch(() => {
        if (!controller.signal.aborted)
          setError("Không đọc được Monitoring summary.");
      });
    void api
      .reports(
        { model_version: model, overall_status: status, cursor },
        controller.signal,
      )
      .then((value) => {
        if (!controller.signal.aborted) setPage(value);
      })
      .catch(() => {
        if (!controller.signal.aborted) {
          setPage(null);
          setError(
            "Catalog Drift chưa khả dụng hoặc phiên phân trang đã hết hạn.",
          );
        }
      });
    return () => controller.abort();
  }, [api, model, status, cursor, revision]);
  useEffect(() => {
    setReport(null);
    if (!api || !selected) return;
    const controller = new AbortController();
    void api
      .report(selected, controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setReport(value);
      })
      .catch(() => {
        if (!controller.signal.aborted)
          setError(
            "Chưa đọc được report đúng phiên bản hoặc không xác minh được checksum.",
          );
      });
    return () => controller.abort();
  }, [api, selected, revision]);
  return (
    <>
      <h1>Monitoring</h1>
      <p className="subtitle">Tình trạng dịch vụ và báo cáo Drift</p>
      {!api && <p>Monitoring chưa khả dụng trong nguồn dữ liệu này.</p>}
      {error && <p role="alert">{error}</p>}
      <button
        className="button secondary"
        onClick={() => {
          setCursor(undefined);
          setRevision((v) => v + 1);
        }}
      >
        Tải lại Monitoring
      </button>
      {summary && (
        <Panel title="Tình trạng dịch vụ" eyebrow="SERVICES">
          <p>Quan sát lúc {date(summary.observed_at)}</p>
          {Object.entries(summary.services).map(([name, service]) => (
            <p key={name}>
              {name}: <Badge value={service.status} /> {service.message}
            </p>
          ))}
          <p>
            Drift worker: <Badge value={summary.drift.worker_status} />
          </p>
          <p>
            Checkpoint thành công:{" "}
            {summary.drift.last_success_at
              ? date(summary.drift.last_success_at)
              : "Chưa có nguồn heartbeat"}
          </p>
          <p>Ngưỡng heartbeat cũ: {summary.drift.stale_after_seconds} giây</p>
          {summary.drift.latest_report && (
            <p>
              Model được cấu hình: {summary.drift.latest_report.model_version} ·
              Cửa sổ dữ liệu kết thúc:{" "}
              {date(summary.drift.latest_report.window_end_at)}
            </p>
          )}
        </Panel>
      )}
      <Panel title="Báo cáo Drift" eyebrow="CATALOG">
        <div className="alert-form">
          <label>
            Model version
            <input
              aria-label="Lọc model version"
              value={model}
              onChange={(e) => {
                setModel(e.target.value);
                setCursor(undefined);
              }}
            />
          </label>
          <label>
            Trạng thái Drift
            <select
              aria-label="Lọc trạng thái Drift"
              value={status}
              onChange={(e) => {
                setStatus(e.target.value);
                setCursor(undefined);
              }}
            >
              <option value="">Tất cả</option>
              {[
                "STABLE",
                "WARNING",
                "CRITICAL",
                "ERROR",
                "INSUFFICIENT_DATA",
              ].map((s) => (
                <option key={s}>{s}</option>
              ))}
            </select>
          </label>
        </div>
        {page && (
          <>
            <p>
              Catalog quan sát lúc {date(page.catalog_as_of)} ·{" "}
              {page.stale ? "Dữ liệu catalog cũ" : "Catalog đã đồng bộ"}
            </p>
            {!page.items.length && (
              <p>Chưa có báo cáo trong catalog đã quan sát.</p>
            )}
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th>Report</th>
                    <th>Model</th>
                    <th>Trạng thái</th>
                    <th>Tính toán</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {page.items.map((item) => (
                    <tr key={item.report_id}>
                      <td>{item.report_id}</td>
                      <td>{item.model_version}</td>
                      <td>
                        <Badge value={item.overall_status} />
                      </td>
                      <td>{date(item.computed_at)}</td>
                      <td>
                        <button
                          className="button secondary"
                          onClick={() => setSelected(item.report_id)}
                        >
                          Xem report
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <button
              className="button secondary"
              disabled={!page.next_cursor}
              onClick={() => setCursor(page.next_cursor ?? undefined)}
            >
              Trang tiếp
            </button>
          </>
        )}
      </Panel>
      {report && (
        <Panel title={report.report_id} eyebrow="DRIFT REPORT">
          <Badge value={report.overall_status} />
          <p>
            Model {report.reference.model_version} · Dataset{" "}
            {report.reference.dataset_version}
          </p>
          <p>
            Cửa sổ {date(report.current_window.start_at)} —{" "}
            {date(report.current_window.end_at)} ·{" "}
            {report.current_window.sample_count} mẫu
          </p>
          {report.reason && <p>{report.reason}</p>}
          <pre className="evidence-json">{JSON.stringify(report, null, 2)}</pre>
        </Panel>
      )}
    </>
  );
}
