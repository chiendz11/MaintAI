import { AlertsPage } from "../features/alerts/AlertsPage";
import { AlertDetailPage } from "../features/alerts/AlertDetailPage";
import { watchAlerts } from "../realtime/alerts";
import { useCallback, useEffect, useMemo, useState } from "react";
import {
  Activity,
  ArrowUpRight,
  Bell,
  Boxes,
  ChevronRight,
  CircleGauge,
  LogOut,
  Radio,
  ShieldCheck,
} from "lucide-react";
import { BusinessApi, pathSegment } from "../api/client";
import { DemoApi } from "../api/demo";
import type { RuntimeConfig } from "./config";
import type { Session } from "./session";
import { FleetPage } from "../features/fleet/FleetPage";
import { MachinePage } from "../features/machines/detail/MachinePage";
import { Badge } from "../shared/ui";
import type { Connection } from "../realtime/sse";

function selectedMachine(): string | null {
  return location.pathname.startsWith("/machines/")
    ? decodeURIComponent(location.pathname.slice("/machines/".length))
    : null;
}
export function App({
  config,
  session,
}: {
  config: RuntimeConfig;
  session: Session;
}) {
  const [machine, setMachine] = useState(selectedMachine),
    [status, setStatus] = useState<Connection>("OFFLINE");
  const [alertRoute, setAlertRoute] = useState(() =>
    location.pathname.startsWith("/alerts"),
  );
  const [alertId, setAlertId] = useState(() =>
    location.pathname.startsWith("/alerts/")
      ? decodeURIComponent(location.pathname.slice(8))
      : null,
  );
  const [expired, setExpired] = useState(false);
  const unauthorized = useCallback(() => setExpired(true), []);
  const api = useMemo(
    () =>
      config.mode === "demo"
        ? new DemoApi()
        : new BusinessApi(
            config.apiBaseUrl,
            session.token,
            undefined,
            unauthorized,
          ),
    [config, session, unauthorized],
  );
  useEffect(() => {
    const pop = () => {
      setMachine(selectedMachine());
      setAlertRoute(location.pathname.startsWith("/alerts"));
      setAlertId(
        location.pathname.startsWith("/alerts/")
          ? decodeURIComponent(location.pathname.slice(8))
          : null,
      );
    };
    window.addEventListener("popstate", pop);
    return () => window.removeEventListener("popstate", pop);
  }, []);
  const navigate = useCallback((machine: string | null) => {
    history.pushState(
      null,
      "",
      machine === null ? "/machines" : "/machines/" + pathSegment(machine),
    );
    setMachine(machine);
    setAlertRoute(false);
    setAlertId(null);
    window.scrollTo(0, 0);
  }, []);
  useEffect(() => {
    if (session.authenticated && !expired && !machine) return watchAlerts(api);
  }, [api, session.authenticated, expired, machine]);
  const openAlert = (id: string | null) => {
    history.pushState(
      null,
      "",
      id === null ? "/alerts" : "/alerts/" + pathSegment(id),
    );
    setMachine(null);
    setAlertRoute(true);
    setAlertId(id);
    window.scrollTo(0, 0);
  };
  const connected = useCallback((value: Connection) => setStatus(value), []);
  if (!session.authenticated || expired)
    return (
      <main className="login">
        <div className="brand-mark">
          <Activity />
        </div>
        <p className="eyebrow">MAINTAI / BUSINESS</p>
        <h1>Đăng nhập để theo dõi thiết bị</h1>
        <p>Dữ liệu vận hành theo đúng phạm vi được cấp quyền.</p>
        <button className="button primary" onClick={() => void session.login()}>
          Đăng nhập
          <ArrowUpRight size={17} />
        </button>
      </main>
    );
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <div className="brand-mark">
            <Activity size={23} />
          </div>
          <span>
            Maint<span className="brand-accent">AI</span>
            <small>OPERATIONS WORKSPACE</small>
          </span>
        </div>
        <p className="nav-label">KHÔNG GIAN LÀM VIỆC</p>
        <nav>
          <button
            className={!machine && !alertRoute ? "nav-item active" : "nav-item"}
            onClick={() => navigate(null)}
          >
            <CircleGauge size={19} />
            Tổng quan
            <ChevronRight size={15} />
          </button>
          <button
            className={machine ? "nav-item active" : "nav-item"}
            onClick={() => navigate(null)}
          >
            <Boxes size={19} />
            Thiết bị
          </button>
          <button
            className={alertRoute ? "nav-item active" : "nav-item"}
            onClick={() => openAlert(null)}
          >
            <Bell size={19} />
            Cảnh báo
            <ChevronRight size={15} />
          </button>
          <div className="nav-item disabled" title="Chức năng chưa khả dụng">
            <Radio size={19} />
            Monitoring<span>Sắp có</span>
          </div>
        </nav>
        <div className="sidebar-bottom">
          <ShieldCheck size={18} />
          <div>
            Business workspace<small>Theo quyền · Theo machine scope</small>
          </div>
        </div>
      </aside>
      <div className="workspace">
        <header className="topbar">
          <div className="breadcrumb">
            Workspace
            <ChevronRight size={13} />
            <strong>
              {alertRoute
                ? "Alerts"
                : machine
                  ? "Machine detail"
                  : "Fleet overview"}
            </strong>
          </div>
          <div className="topbar-right">
            {config.mode === "demo" && (
              <span className="demo-label">Dữ liệu mô phỏng</span>
            )}
            {machine ? (
              <Badge value={status} />
            ) : (
              <span className="rest-label">REST snapshot</span>
            )}
            <div className="user">
              <span className="avatar">{session.name.slice(0, 1)}</span>
              <span>
                {session.name}
                <small>{session.role}</small>
              </span>
            </div>
            {config.mode === "live" && (
              <button
                className="icon-button"
                aria-label="Đăng xuất"
                onClick={() => void session.logout()}
              >
                <LogOut size={17} />
              </button>
            )}
          </div>
        </header>
        <main className="content">
          {alertRoute ? (
            alertId ? (
              <AlertDetailPage
                key={alertId}
                api={api}
                id={alertId}
                back={() => openAlert(null)}
                actor={session.subject}
                canManage={session.canManage}
              />
            ) : (
              <AlertsPage api={api} open={openAlert} />
            )
          ) : machine ? (
            <MachinePage
              key={machine}
              machine={machine}
              api={api}
              config={config}
              back={() => navigate(null)}
              connection={connected}
            />
          ) : (
            <FleetPage api={api} open={navigate} />
          )}
          <footer className="page-footer">
            <span>MaintAI · Predictive maintenance</span>
            <span>Telemetry · Prediction · Processing</span>
          </footer>
        </main>
      </div>
    </div>
  );
}
