import { useAlertRefresh } from "../alerts/useRefresh";
import { useEffect, useMemo, useState } from "react";
import { AlertTriangle, ArrowRight, RefreshCw, Search } from "lucide-react";
import type { MachineApi } from "../../api/client";
import { pathSegment } from "../../api/client";
import type { MachineItem, MachinePage } from "../../types/live";
import {
  getFleetMachineHealth,
  subscribeFleetHealth,
  type FleetMachineHealth,
} from "../../realtime/reducers";
import type { PhysicalStatus } from "../../shared/physical-guardrails";
import { Badge, date, ErrorNotice } from "../../shared/ui";

type SortMode = "default" | "fallback";

const PHYSICAL_RANK: Record<PhysicalStatus, number> = {
  CRITICAL: 3,
  WARNING: 2,
  NORMAL: 1,
};

function isMachineAiDegraded(
  item: MachineItem,
  health?: FleetMachineHealth,
): boolean {
  if (item.state === "UNKNOWN" || item.latest_risk_score === null) return true;
  if (
    health?.processing_status &&
    ["FAILED", "RETRYING"].includes(health.processing_status)
  ) {
    return true;
  }
  if (health?.telemetry_accepted_at && health?.prediction_accepted_at) {
    const lagMs =
      Date.parse(health.telemetry_accepted_at) -
      Date.parse(health.prediction_accepted_at);
    if (Number.isFinite(lagMs) && lagMs > 15000) return true;
  }
  return false;
}

export function FleetPage({
  api,
  open,
}: {
  api: MachineApi;
  open: (machine: string) => void;
}) {
  const [state, setState] = useState(""),
    [sortMode, setSortMode] = useState<SortMode>("fallback"),
    [offset, setOffset] = useState(0),
    [revision, setRevision] = useState(0),
    [fleetTick, setFleetTick] = useState(0);
  const [page, setPage] = useState<MachinePage | null>(null),
    [error, setError] = useState<unknown>(null),
    [loading, setLoading] = useState(true),
    [machine, setMachine] = useState("");
  const [alertRevision] = useAlertRefresh();

  useEffect(() => {
    return subscribeFleetHealth(() => {
      setFleetTick((value) => value + 1);
    });
  }, []);

  useEffect(() => {
    const request = new AbortController();
    setLoading(true);
    setError(null);
    setPage(null);
    api
      .fleet(state, offset, request.signal)
      .then((value) => {
        if (!request.signal.aborted) setPage(value);
      })
      .catch((caught) => {
        if (!request.signal.aborted) setError(caught);
      })
      .finally(() => {
        if (!request.signal.aborted) setLoading(false);
      });
    return () => request.abort();
  }, [api, state, offset, revision, alertRevision]);

  const degradedCount = useMemo(() => {
    void fleetTick;
    if (!page) return 0;
    return page.items.filter((item) =>
      isMachineAiDegraded(item, getFleetMachineHealth(item.machine_id)),
    ).length;
  }, [page, fleetTick]);

  const sortedItems = useMemo(() => {
    void fleetTick;
    if (!page) return [];
    if (sortMode === "default") return page.items;
    return [...page.items].sort((a, b) => {
      const healthA = getFleetMachineHealth(a.machine_id);
      const healthB = getFleetMachineHealth(b.machine_id);
      const rankA = healthA ? PHYSICAL_RANK[healthA.physicalHealth.status] : 0;
      const rankB = healthB ? PHYSICAL_RANK[healthB.physicalHealth.status] : 0;
      // Physical WARNING / CRITICAL violations jump to the top in fallback mode
      const alertRankA = rankA >= 2 ? rankA : 0;
      const alertRankB = rankB >= 2 ? rankB : 0;
      if (alertRankA !== alertRankB) return alertRankB - alertRankA;

      const scoreA = a.latest_risk_score ?? -1;
      const scoreB = b.latest_risk_score ?? -1;
      if (scoreA !== scoreB) return scoreB - scoreA;
      return a.machine_id.localeCompare(b.machine_id);
    });
  }, [page, sortMode, fleetTick]);

  function submit(event: React.FormEvent) {
    event.preventDefault();
    if (!machine.trim()) return;
    try {
      pathSegment(machine);
      open(machine);
    } catch (caught) {
      setError(caught);
    }
  }
  return (
    <>
      <div className="page-heading">
        <div>
          <p className="eyebrow">FLEET / THIẾT BỊ</p>
          <h1>Tổng quan thiết bị</h1>
          <p className="subtitle">
            Theo dõi trạng thái vận hành từ dữ liệu đã được xử lý.
          </p>
        </div>
        <button
          className="button secondary"
          onClick={() => setRevision((value) => value + 1)}
        >
          <RefreshCw size={16} />
          Làm mới
        </button>
      </div>
      <div className="overview">
        <div>
          <span>THIẾT BỊ KHỚP BỘ LỌC</span>
          <strong>{page?.total ?? "—"}</strong>
        </div>
        <div>
          <span>TRẠNG THÁI DỮ LIỆU</span>
          <strong className="overview-small">
            {loading ? "Đang tải" : error ? "Chưa khả dụng" : "Đã đồng bộ"}
          </strong>
        </div>
        <div>
          <span>NGUỒN THÔNG TIN</span>
          <strong className="overview-small">Business snapshot</strong>
        </div>
      </div>
      {degradedCount > 0 && (
        <div className="notice warn degraded-banner" role="status">
          <div className="degraded-banner-content">
            <AlertTriangle size={16} />
            <span>
              ⚠️ Nhánh suy luận AI đang gián đoạn hoặc trễ trên một số máy (
              {degradedCount} thiết bị). Hệ thống đã kích hoạt bộ lọc Cảnh báo
              Vật lý Trực tiếp (Physical Guardrails) từ luồng Telemetry.
            </span>
          </div>
        </div>
      )}
      <form className="quick-open" onSubmit={submit}>
        <div>
          <strong>Mở trực tiếp thiết bị</strong>
          <p>Nhập ID máy để xem telemetry và kết quả xử lý realtime.</p>
        </div>
        <div className="input-action">
          <label className="sr-only" htmlFor="machine-id">
            ID máy
          </label>
          <Search size={17} />
          <input
            id="machine-id"
            value={machine}
            onChange={(event) => setMachine(event.target.value)}
            placeholder="Ví dụ: M-001"
            required
          />
          <button className="button primary" type="submit">
            Mở thiết bị
            <ArrowRight size={16} />
          </button>
        </div>
      </form>
      <ErrorNotice
        error={error}
        retry={() => setRevision((value) => value + 1)}
      />
      <section className="panel fleet-panel">
        <div className="table-heading">
          <div>
            <h2>Danh sách thiết bị</h2>
            <p>Dữ liệu theo phạm vi được cấp quyền</p>
          </div>
          <div className="fleet-controls">
            <label>
              Sắp xếp
              <select
                aria-label="Chế độ sắp xếp"
                value={sortMode}
                onChange={(event) =>
                  setSortMode(event.target.value as SortMode)
                }
              >
                <option value="fallback">
                  AI Risk &amp; Physical Guardrail (Fallback)
                </option>
                <option value="default">Theo mã thiết bị (Mặc định)</option>
              </select>
            </label>
            <label>
              Trạng thái
              <select
                aria-label="Lọc trạng thái"
                value={state}
                onChange={(event) => {
                  setState(event.target.value);
                  setOffset(0);
                }}
              >
                {[
                  "",
                  "NORMAL",
                  "WATCH",
                  "HIGH",
                  "CRITICAL",
                  "INVALID",
                  "UNKNOWN",
                ].map((value) => (
                  <option key={value} value={value}>
                    {value || "Tất cả trạng thái"}
                  </option>
                ))}
              </select>
            </label>
          </div>
        </div>
        <div className="table-scroll">
          <table>
            <thead>
              <tr>
                <th>Thiết bị</th>
                <th>Trạng thái</th>
                <th>Physical Guardrail</th>
                <th>Risk score</th>
                <th>Active alerts</th>
                <th>Lần nhận gần nhất</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {sortedItems.map((value) => {
                const health = getFleetMachineHealth(value.machine_id);
                const physical = health?.physicalHealth;
                const tone =
                  physical?.status === "CRITICAL"
                    ? "bad"
                    : physical?.status === "WARNING"
                      ? "warn"
                      : physical?.status === "NORMAL"
                        ? "good"
                        : "neutral";
                return (
                  <tr key={value.machine_id}>
                    <td>
                      <strong>{value.display_name}</strong>
                      <code>{value.machine_id}</code>
                    </td>
                    <td>
                      <Badge value={value.state} />
                      <small>{value.data_status ?? "Chưa có kết quả DQ"}</small>
                    </td>
                    <td>
                      <span
                        className={`badge ${tone}`}
                        title={
                          physical?.triggeredReasons.length
                            ? physical.triggeredReasons.join(" · ")
                            : "Các chỉ số vật lý trong ngưỡng an toàn"
                        }
                      >
                        <span className="dot" />
                        {physical ? physical.primaryBadge : "Chờ telemetry"}
                      </span>
                      {physical && physical.triggeredReasons.length > 0 && (
                        <small>{physical.triggeredReasons[0]}</small>
                      )}
                    </td>
                    <td>
                      {value.latest_risk_score === null
                        ? "—"
                        : value.latest_risk_score.toFixed(2)}
                    </td>
                    <td>{value.active_alert_count}</td>
                    <td>{date(value.last_seen_at)}</td>
                    <td>
                      <a
                        className="table-link"
                        href={"/machines/" + pathSegment(value.machine_id)}
                        aria-label={"Xem " + value.machine_id}
                        onClick={(event) => {
                          if (!event.metaKey && !event.ctrlKey) {
                            event.preventDefault();
                            open(value.machine_id);
                          }
                        }}
                      >
                        Xem
                        <ArrowRight size={15} />
                      </a>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
        {loading && (
          <div className="empty" role="status">
            Đang tải danh sách thiết bị…
          </div>
        )}
        {!loading && !error && page?.items.length === 0 && (
          <div className="empty">Không có thiết bị khớp bộ lọc.</div>
        )}
        <div className="pagination">
          <span>
            {page
              ? `${page.total} thiết bị · Trang ${Math.floor(offset / 50) + 1}`
              : "Chưa có snapshot danh sách"}
          </span>
          <div>
            <button
              className="button small secondary"
              disabled={loading || offset === 0}
              onClick={() => setOffset((value) => Math.max(0, value - 50))}
            >
              Trước
            </button>
            <button
              className="button small secondary"
              disabled={loading || !page || offset + 50 >= page.total}
              onClick={() => setOffset((value) => value + 50)}
            >
              Sau
            </button>
          </div>
        </div>
      </section>
    </>
  );
}
