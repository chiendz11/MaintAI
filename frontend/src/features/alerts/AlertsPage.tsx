import { useEffect, useState } from "react";
import { ArrowRight, RefreshCw } from "lucide-react";
import type { AlertApi, AlertPage } from "../../types/alerts";
import { Badge, date, ErrorNotice } from "../../shared/ui";
import { useAlertRefresh } from "./useRefresh";

export function AlertsPage({
  api,
  open,
}: {
  api: AlertApi;
  open: (id: string) => void;
}) {
  const [status, setStatus] = useState(""),
    [severity, setSeverity] = useState(""),
    [machine, setMachine] = useState(""),
    [offset, setOffset] = useState(0);
  const [page, setPage] = useState<AlertPage | null>(null),
    [error, setError] = useState<unknown>(null),
    [loading, setLoading] = useState(true);
  const [revision, refresh] = useAlertRefresh();
  useEffect(() => {
    const request = new AbortController();
    setLoading(true);
    setError(null);
    api
      .alerts({ status, severity, machine_id: machine, offset }, request.signal)
      .then((value) => {
        if (!request.signal.aborted) setPage(value);
      })
      .catch((caught) => {
        if (!request.signal.aborted) {
          setError(caught);
          setPage(null);
        }
      })
      .finally(() => {
        if (!request.signal.aborted) setLoading(false);
      });
    return () => request.abort();
  }, [api, status, severity, machine, offset, revision]);
  return (
    <>
      <div className="page-heading">
        <div>
          <p className="eyebrow">ALERTS / CẢNH BÁO</p>
          <h1>Cảnh báo bảo trì</h1>
          <p className="subtitle">
            Theo dõi, tiếp nhận và ghi nhận kết quả xử lý theo evidence nguồn.
          </p>
        </div>
        <button className="button secondary" onClick={refresh}>
          <RefreshCw size={16} />
          Làm mới
        </button>
      </div>
      <div className="alert-filters panel">
        <label>
          Trạng thái
          <select
            aria-label="Trạng thái cảnh báo"
            value={status}
            onChange={(e) => {
              setStatus(e.target.value);
              setOffset(0);
            }}
          >
            <option value="">Tất cả trạng thái</option>
            {[
              "OPEN",
              "ACKNOWLEDGED",
              "IN_PROGRESS",
              "ESCALATED",
              "RESOLVED",
              "FALSE_ALERT",
            ].map((v) => (
              <option key={v}>{v}</option>
            ))}
          </select>
        </label>
        <label>
          Mức độ
          <select
            aria-label="Mức độ cảnh báo"
            value={severity}
            onChange={(e) => {
              setSeverity(e.target.value);
              setOffset(0);
            }}
          >
            <option value="">Tất cả mức độ</option>
            {["WATCH", "HIGH", "CRITICAL"].map((v) => (
              <option key={v}>{v}</option>
            ))}
          </select>
        </label>
        <label>
          Máy
          <input
            aria-label="Lọc theo ID máy"
            value={machine}
            onChange={(e) => {
              setMachine(e.target.value);
              setOffset(0);
            }}
            placeholder="Tất cả máy được cấp quyền"
          />
        </label>
      </div>
      <ErrorNotice error={error} retry={refresh} />
      {loading && <p role="status">Đang đồng bộ cảnh báo…</p>}
      <section className="panel">
        <div className="panel-heading">
          <h2>Danh sách cảnh báo</h2>
          <span>{page?.total ?? "—"} kết quả</span>
        </div>
        {page?.items.length === 0 && !loading && (
          <p className="empty">Không có cảnh báo khớp bộ lọc.</p>
        )}
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Cảnh báo / Thiết bị</th>
                <th>Mức độ</th>
                <th>Trạng thái</th>
                <th>Risk</th>
                <th>Được tạo</th>
                <th>Thao tác</th>
              </tr>
            </thead>
            <tbody>
              {page?.items.map((v) => (
                <tr key={v.alert_id}>
                  <td>
                    <strong>{v.machine_id}</strong>
                    <small className="alert-id">{v.alert_id}</small>
                  </td>
                  <td>
                    <Badge value={v.severity} />
                  </td>
                  <td>
                    <Badge value={v.status} />
                  </td>
                  <td>{(v.risk_score * 100).toFixed(1)}%</td>
                  <td>{date(v.created_at)}</td>
                  <td>
                    <button
                      className="button small secondary"
                      onClick={() => open(v.alert_id)}
                    >
                      Chi tiết
                      <ArrowRight size={14} />
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="pagination">
          <button
            className="button small secondary"
            disabled={offset === 0 || loading}
            onClick={() => setOffset((v) => Math.max(0, v - 50))}
          >
            Trang trước
          </button>
          <span>
            {offset + 1}–{offset + (page?.items.length ?? 0)} /{" "}
            {page?.total ?? "—"}
          </span>
          <button
            className="button small secondary"
            disabled={!page || offset + 50 >= page.total || loading}
            onClick={() => setOffset((v) => v + 50)}
          >
            Trang sau
          </button>
        </div>
      </section>
    </>
  );
}
