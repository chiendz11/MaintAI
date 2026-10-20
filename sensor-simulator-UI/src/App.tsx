import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import {
  Activity,
  ArrowDownRight,
  ArrowUpRight,
  BellOff,
  Check,
  CheckCheck,
  ChevronDown,
  ChevronRight,
  CircleHelp,
  Clock3,
  Cpu,
  ExternalLink,
  Gauge,
  LayoutDashboard,
  LoaderCircle,
  LogIn,
  LogOut,
  Menu,
  Pause,
  Play,
  Radio,
  RefreshCw,
  Search,
  Settings2,
  ShieldCheck,
  SlidersHorizontal,
  Terminal,
  Trash2,
  TriangleAlert,
  X,
} from "lucide-react";
import type { RuntimeConfig } from "./config";
import type { Session } from "./auth";
import type { SimulatorController } from "./controller";
import { nextPreview, type Preview } from "./demo";
import { PreviewPanel } from "./PreviewPanel";
import { scenarioLabel, scenarios, type CommandEntry } from "./types";

const time = (v: string | number) =>
  new Date(v).toLocaleTimeString("vi-VN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
const statusLabels = {
  pending: "Đang gửi",
  confirmed: "Đã xác nhận",
  unknown: "Chưa rõ kết quả",
  conflict: "Xung đột",
  rejected: "Bị từ chối",
};
const actionLabel = (entry: CommandEntry) =>
  entry.action === "pause"
    ? "Pause"
    : entry.action === "resume"
      ? "Resume"
      : `Scenario → ${scenarioLabel(entry.body.scenario ?? "")}`;
const scenarioIcons = {
  normal: Check,
  degradation: ArrowUpRight,
  critical: TriangleAlert,
  recovery: ArrowDownRight,
};

interface DemoReading {
  id: string;
  time: string;
  machineId: string;
  rpm: number;
  scenario: string;
}
export function App({
  config,
  session,
  controller,
}: {
  config: RuntimeConfig;
  session: Session;
  controller: SimulatorController;
}) {
  const state = useSyncExternalStore(
    controller.subscribe,
    controller.getSnapshot,
  );
  const [now, setNow] = useState(Date.now());
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState("ALL");
  const [mobileNav, setMobileNav] = useState(false);
  const [rate, setRate] = useState(1);
  const [preview, setPreview] = useState<Record<string, Preview>>({});
  const [readings, setReadings] = useState<DemoReading[]>([]);
  const [tab, setTab] = useState<"commands" | "readings">("commands");
  const [authError, setAuthError] = useState("");
  const settings = useRef<HTMLDialogElement>(null);
  const previews = useRef<Record<string, Preview>>({});
  const demo = config.mode === "demo";
  const selected = state.machines.find(
    (m) => m.machine_id === state.selectedId,
  );
  const unresolved = state.journal.find(
    (x) =>
      x.machineId === state.selectedId &&
      ["pending", "unknown"].includes(x.status),
  );
  const fresh =
    !!selected &&
    now - (state.observedAt[selected.machine_id] ?? 0) < config.staleAfterMs &&
    state.notice?.kind !== "error";
  const disabled = !fresh || !!unresolved || !session.authenticated;
  const running = state.machines.filter(
    (m) => m.stream_state === "RUNNING",
  ).length;
  const confirmed = state.journal.filter(
    (x) => x.status === "confirmed",
  ).length;
  const visibleMachines = state.machines.filter(
    (m) =>
      m.machine_id.toLowerCase().includes(query.toLowerCase()) &&
      (filter === "ALL" || m.stream_state === filter),
  );
  const hasSnapshot = state.lastSync !== null;
  const stale =
    hasSnapshot &&
    (now - state.lastSync! >= config.staleAfterMs ||
      state.notice?.kind === "error");

  useEffect(() => {
    if (!session.authenticated) return;
    let stopped = false;
    let timeout: ReturnType<typeof setTimeout>;
    let backoff = config.pollIntervalMs;
    const poll = async () => {
      if (document.visibilityState !== "hidden") {
        await controller.refresh();
        backoff =
          controller.getSnapshot().notice?.kind === "error"
            ? Math.min(backoff * 2, 60000)
            : config.pollIntervalMs;
      }
      if (!stopped)
        timeout = setTimeout(() => {
          void poll();
        }, backoff);
    };
    const resync = () => {
      if (!stopped && document.visibilityState !== "hidden")
        void controller.refresh();
    };
    void poll();
    window.addEventListener("online", resync);
    document.addEventListener("visibilitychange", resync);
    return () => {
      stopped = true;
      clearTimeout(timeout);
      window.removeEventListener("online", resync);
      document.removeEventListener("visibilitychange", resync);
    };
  }, [config.pollIntervalMs, controller, session.authenticated]);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, []);
  useEffect(() => {
    if (!demo) return;
    const timer = setInterval(() => {
      const machines = controller.getSnapshot().machines;
      const next = { ...previews.current };
      const batch: DemoReading[] = [];
      for (const machine of machines) {
        next[machine.machine_id] = nextPreview(
          next[machine.machine_id],
          machine,
        );
        if (machine.stream_state === "RUNNING")
          batch.push({
            id: `DEMO-${machine.machine_id}-${next[machine.machine_id].seq}`,
            time: new Date().toISOString(),
            machineId: machine.machine_id,
            rpm: next[machine.machine_id].rpm,
            scenario: machine.scenario,
          });
      }
      previews.current = next;
      setPreview(next);
      setReadings((old) => [...batch.reverse(), ...old].slice(0, 30));
    }, 1000 / rate);
    return () => clearInterval(timer);
  }, [demo, rate, controller]);

  const authenticate = async (logout = false) => {
    try {
      setAuthError("");
      await (logout ? session.logout() : session.login());
    } catch {
      setAuthError(
        "Không thể mở phiên đăng nhập. Kiểm tra kết nối tới nhà cung cấp danh tính.",
      );
    }
  };
  const links = [
    {
      href: config.links.business,
      label: "Business UI",
      icon: LayoutDashboard,
    },
    { href: config.links.grafana, label: "System & Drift", icon: Gauge },
    { href: config.links.mlflow, label: "MLflow", icon: Cpu },
  ];
  return (
    <div className="app-shell">
      <a className="skip-link" href="#main">
        Đi tới nội dung chính
      </a>
      {mobileNav && (
        <button
          className="nav-backdrop"
          onClick={() => setMobileNav(false)}
          aria-label="Đóng menu"
        />
      )}
      <aside className={`sidebar ${mobileNav ? "open" : ""}`}>
        <a className="brand" href="#main">
          <div className="brand-mark">
            <Activity size={23} />
          </div>
          <div>
            <strong>
              MaintAI<span> / LAB</span>
            </strong>
            <small>SENSOR SIMULATOR</small>
          </div>
        </a>
        <div className="workspace">
          <span className="workspace-icon">
            <SlidersHorizontal size={17} />
          </span>
          <div>
            <strong>Simulator Workspace</strong>
            <small>{demo ? "Môi trường demo cục bộ" : "Sensor Control"}</small>
          </div>
          <ChevronDown size={14} />
        </div>
        <div className="nav-caption">ĐIỀU KHIỂN</div>
        <nav aria-label="Simulator">
          <a
            className="nav-item active"
            href="#control"
            onClick={() => setMobileNav(false)}
          >
            <SlidersHorizontal size={18} />
            <span>Bảng điều khiển</span>
            <span className="nav-active-dot" />
          </a>
          <a
            className="nav-item"
            href="#machines"
            onClick={() => setMobileNav(false)}
          >
            <Cpu size={18} />
            <span>Máy mô phỏng</span>
            <span className="nav-count">{state.machines.length}</span>
          </a>
          <a
            className="nav-item"
            href="#activity"
            onClick={() => setMobileNav(false)}
          >
            <Terminal size={18} />
            <span>Nhật ký thao tác</span>
          </a>
        </nav>
        <div className="nav-caption">KHÔNG GIAN LÀM VIỆC</div>
        <nav aria-label="Ứng dụng liên quan">
          {links.map(({ href, label, icon: Icon }) =>
            href ? (
              <a
                className="nav-item"
                href={href}
                target="_blank"
                rel="noopener noreferrer"
                key={label}
              >
                <Icon size={18} />
                <span>{label}</span>
                <ExternalLink size={13} />
              </a>
            ) : (
              <span
                className="nav-item unavailable-link"
                title="Chưa cấu hình liên kết"
                key={label}
              >
                <Icon size={18} />
                <span>{label}</span>
                <ExternalLink size={13} />
              </span>
            ),
          )}
        </nav>
        <div className="sidebar-bottom">
          <div className="sandbox-note">
            <ShieldCheck size={18} />
            <strong>Không gian mô phỏng</strong>
            <p>
              Điều khiển luồng dữ liệu giả lập. Không kết nối máy vật lý hoặc
              PLC.
            </p>
          </div>
          <span className="sidebar-version">
            SIMULATOR UI <b>v0.1.0</b>
          </span>
        </div>
      </aside>
      <div className="app-main">
        <header className="topbar">
          <div className="breadcrumb">
            <button
              className="icon-button menu-button"
              onClick={() => setMobileNav(!mobileNav)}
              aria-label="Mở menu"
            >
              <Menu size={19} />
            </button>
            <span>Workspace</span>
            <ChevronRight size={13} />
            <strong>Sensor Simulator</strong>
          </div>
          <div className="topbar-actions">
            <span className={`environment ${demo ? "demo" : "live"}`}>
              <i />
              {demo ? "DEMO LOCAL" : "LIVE API"}
            </span>
            <div className="topbar-divider" />
            <button
              className="icon-button"
              aria-label="Cấu hình kết nối"
              onClick={() => settings.current?.showModal()}
            >
              <Settings2 size={18} />
            </button>
            <span className="avatar" title={session.name}>
              OP
            </span>
          </div>
        </header>
        <main className="page" id="main">
          <div className="page-heading" id="control">
            <div>
              <div className="eyebrow">
                MAINTAI LAB <span>/</span> SENSOR CONTROL
              </div>
              <h1>
                Sensor Simulator<span className="title-dot">.</span>
              </h1>
              <p>
                Thiết lập kịch bản và điều khiển luồng dữ liệu cho từng máy mô
                phỏng.
              </p>
            </div>
            <button
              className="button refresh-button"
              onClick={() => {
                void controller.refresh();
              }}
              disabled={state.syncing || !session.authenticated}
            >
              <RefreshCw size={15} className={state.syncing ? "spin" : ""} />
              Đồng bộ trạng thái
            </button>
          </div>
          <div
            className={`mode-banner ${demo ? "demo-banner" : "live-banner"}`}
          >
            <span className="banner-icon">
              {demo ? <FlaskIcon /> : <ShieldCheck size={20} />}
            </span>
            <div>
              <strong>
                {demo ? "Bạn đang ở chế độ demo" : "Kết nối Sensor Control API"}
              </strong>
              <p>
                {demo
                  ? "Dữ liệu preview được mô phỏng ngay trong trình duyệt. Không có reading hay lệnh nào được gửi tới Sensor, Ingestion hoặc Kafka thật."
                  : "Thao tác chỉ có hiệu lực sau ACK từ API. Chức năng điều khiển không thay đổi cảnh báo hoặc trạng thái bảo trì."}
              </p>
            </div>
            <span className="banner-tag">
              {demo ? "BROWSER ONLY" : "CONTROL PLANE"}
            </span>
          </div>
          {!session.authenticated && (
            <div className="notice warning" role="status">
              <LogIn size={18} />
              <div>
                <strong>Đăng nhập để truy cập simulator</strong>
                <p>Dùng tài khoản được cấp quyền điều khiển máy.</p>
              </div>
              <button
                className="button primary"
                onClick={() => {
                  void authenticate();
                }}
              >
                Đăng nhập
              </button>
            </div>
          )}
          {authError && (
            <div className="notice error" role="alert">
              {authError}
            </div>
          )}
          {state.notice && (
            <div className={`notice ${state.notice.kind}`} role="status">
              <TriangleAlert size={17} />
              <span>{state.notice.text}</span>
            </div>
          )}
          <section className="stats-grid" aria-label="Tổng quan simulator">
            <Metric
              label="Máy đang phát"
              value={hasSnapshot ? String(running).padStart(2, "0") : "—"}
              suffix={hasSnapshot ? `/ ${state.machines.length}` : ""}
              caption={
                stale ? "Trạng thái đã cũ" : "Điều khiển độc lập từng máy"
              }
              icon={<Radio size={19} />}
              tone="teal"
            />
            <Metric
              label="Đang tạm dừng"
              value={
                hasSnapshot
                  ? String(state.machines.length - running).padStart(2, "0")
                  : "—"
              }
              caption="Chỉ dừng tạo reading mới"
              icon={<Pause size={19} />}
              tone="amber"
            />
            <Metric
              label="Lệnh đã xác nhận"
              value={String(confirmed).padStart(2, "0")}
              caption={
                demo
                  ? "ACK demo trong phiên này"
                  : "ACK từ Control API trong phiên"
              }
              icon={<CheckCheck size={19} />}
              tone="blue"
            />
            <Metric
              label={demo ? "Tốc độ preview" : "Nguồn trạng thái"}
              value={
                demo
                  ? (running * rate).toLocaleString("vi-VN")
                  : hasSnapshot
                    ? stale
                      ? "STALE"
                      : "FRESH"
                    : "—"
              }
              suffix={demo ? "reading/s" : ""}
              caption={
                demo
                  ? "Tổng tốc độ mô phỏng trình duyệt"
                  : "Độ mới lần đọc, không phải engine health"
              }
              icon={<Activity size={19} />}
              tone="violet"
            />
          </section>
          <section
            className="card machines-panel"
            id="machines"
            aria-labelledby="machines-title"
          >
            <div className="section-bar">
              <div>
                <h2 id="machines-title">
                  Máy mô phỏng{" "}
                  <span className="count-badge">{state.machines.length}</span>
                </h2>
                <p>Chọn một máy để xem trạng thái và điều khiển.</p>
              </div>
              <div className="machine-tools">
                <label className="search-box">
                  <Search size={15} />
                  <input
                    aria-label="Tìm máy"
                    placeholder="Tìm machine ID…"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                  />
                </label>
                <select
                  aria-label="Lọc trạng thái máy"
                  value={filter}
                  onChange={(e) => setFilter(e.target.value)}
                >
                  <option value="ALL">Tất cả trạng thái</option>
                  <option value="RUNNING">RUNNING</option>
                  <option value="PAUSED">PAUSED</option>
                </select>
              </div>
            </div>
            <div className="machine-grid">
              {visibleMachines.map((machine, i) => (
                <button
                  className={`machine-card ${state.selectedId === machine.machine_id ? "selected" : ""}`}
                  aria-pressed={state.selectedId === machine.machine_id}
                  key={machine.machine_id}
                  onClick={() => controller.select(machine.machine_id)}
                >
                  <div className="machine-card-top">
                    <span className="machine-icon">
                      <Cpu size={19} />
                    </span>
                    <span
                      className={`small-state ${machine.stream_state === "PAUSED" ? "paused" : ""}`}
                    >
                      <i />
                      {machine.stream_state}
                    </span>
                  </div>
                  <strong>{machine.machine_id}</strong>
                  <span className="machine-caption">
                    {demo
                      ? `Máy giả lập ${String(i + 1).padStart(2, "0")}`
                      : "Sensor Simulator"}
                  </span>
                  <div className="machine-card-bottom">
                    <span>{scenarioLabel(machine.scenario)}</span>
                    {state.selectedId === machine.machine_id ? (
                      <Check size={14} />
                    ) : (
                      <ChevronRight size={14} />
                    )}
                  </div>
                </button>
              ))}
            </div>
            {!visibleMachines.length && (
              <div className="empty-state">
                <Cpu size={26} />
                <strong>
                  {state.syncing
                    ? "Đang tải danh sách máy…"
                    : hasSnapshot
                      ? "Không có máy phù hợp"
                      : "Chưa đọc được danh sách máy"}
                </strong>
                <p>
                  {hasSnapshot
                    ? "Thử thay đổi bộ lọc hoặc từ khóa tìm kiếm."
                    : "Đồng bộ lại sau khi kết nối hoặc đăng nhập."}
                </p>
              </div>
            )}
          </section>
          <div className="control-layout">
            <section
              className="card controls-panel"
              aria-labelledby="control-title"
            >
              <div className="card-heading">
                <div>
                  <div className="eyebrow">01 / STREAM CONTROL</div>
                  <h2 id="control-title">Điều khiển luồng</h2>
                </div>
                <span className="card-heading-icon">
                  <SlidersHorizontal size={19} />
                </span>
              </div>
              <label className="field-label" htmlFor="machine-select">
                Máy đang chọn
              </label>
              <div className="select-wrap">
                <select
                  id="machine-select"
                  value={state.selectedId}
                  onChange={(e) => controller.select(e.target.value)}
                  disabled={!state.machines.length}
                >
                  {!selected && <option value="">Chưa có máy</option>}
                  {state.machines.map((machine) => (
                    <option key={machine.machine_id} value={machine.machine_id}>
                      {machine.machine_id} · {machine.stream_state}
                    </option>
                  ))}
                </select>
                <ChevronDown size={16} />
              </div>
              <div className="control-state-row">
                <span
                  className={`badge ${selected?.stream_state === "RUNNING" ? "teal" : selected ? "amber" : "neutral"}`}
                >
                  <i className="status-dot" />
                  {selected?.stream_state ?? "UNAVAILABLE"}
                </span>
                <span className={`freshness ${fresh ? "fresh" : "stale"}`}>
                  <Clock3 size={12} />
                  {selected
                    ? fresh
                      ? "Vừa đồng bộ"
                      : "Trạng thái cũ"
                    : "Chưa có dữ liệu"}
                </span>
                <span className="control-version">
                  v{selected?.control_version ?? "—"}
                </span>
              </div>
              <div className="control-buttons">
                <button
                  className="button primary"
                  disabled={disabled || selected?.stream_state === "RUNNING"}
                  onClick={() => {
                    if (selected)
                      void controller.command(selected.machine_id, "resume");
                  }}
                >
                  <Play size={16} fill="currentColor" />
                  Resume
                </button>
                <button
                  className="button pause-button"
                  disabled={disabled || selected?.stream_state === "PAUSED"}
                  onClick={() => {
                    if (selected)
                      void controller.command(selected.machine_id, "pause");
                  }}
                >
                  <Pause size={16} fill="currentColor" />
                  Pause
                </button>
              </div>
              {unresolved && (
                <div className="pending-box" role="status">
                  {unresolved.status === "pending" ? (
                    <LoaderCircle className="spin" size={16} />
                  ) : (
                    <TriangleAlert size={16} />
                  )}
                  <span>
                    {unresolved.status === "pending"
                      ? "Đang chờ xác nhận…"
                      : "Lệnh chưa rõ kết quả. Xem nhật ký để đối chiếu hoặc thử lại."}
                  </span>
                </div>
              )}
              <div className="control-meta">
                <Clock3 size={13} />
                <span>
                  Cập nhật control: {selected ? time(selected.updated_at) : "—"}
                  {selected && <> · Bởi {selected.last_controlled_by}</>}
                </span>
              </div>
              <div className="panel-divider" />
              <div className="subheading">
                <h3>02 / Kịch bản dữ liệu</h3>
                <span>Áp dụng cho readings tiếp theo</span>
              </div>
              <div className="scenario-grid">
                {scenarios.map((scenario) => {
                  const Icon = scenarioIcons[scenario.symbol];
                  const active = selected?.scenario === scenario.id;
                  return (
                    <button
                      key={scenario.id}
                      className={`scenario-card ${active ? "selected" : ""}`}
                      aria-label={`Kịch bản ${scenario.label}`}
                      aria-pressed={active}
                      disabled={disabled || active}
                      onClick={() => {
                        if (selected)
                          void controller.command(
                            selected.machine_id,
                            "scenario",
                            scenario.id,
                          );
                      }}
                    >
                      <div>
                        <span className={`scenario-symbol ${scenario.id}`}>
                          <Icon size={16} />
                        </span>
                        {active && <Check size={14} />}
                      </div>
                      <strong>{scenario.label}</strong>
                      <small>{scenario.caption}</small>
                    </button>
                  );
                })}
              </div>
              {selected &&
                !scenarios.some((s) => s.id === selected.scenario) && (
                  <p className="unsupported-scenario">
                    Scenario từ server: {selected.scenario}. Chưa có mô tả trong
                    UI.
                  </p>
                )}
              {demo ? (
                <div className="demo-rate">
                  <div>
                    <label htmlFor="rate">Tốc độ preview demo</label>
                    <strong>{rate} reading/s/máy</strong>
                  </div>
                  <input
                    id="rate"
                    type="range"
                    min="0.5"
                    max="4"
                    step="0.5"
                    value={rate}
                    onChange={(e) => setRate(Number(e.target.value))}
                  />
                  <p>Chỉ thay tốc độ preview trong trình duyệt.</p>
                </div>
              ) : (
                <div className="capability-note">
                  <CircleHelp size={15} />
                  <span>
                    API hiện chưa hỗ trợ thay tốc độ, counters hoặc preview.
                  </span>
                </div>
              )}
              <div className="control-note">
                <BellOff size={16} />
                <p>
                  Pause chỉ dừng reading mới. Dữ liệu trong hàng chờ vẫn được xử
                  lý; cảnh báo không tự đóng.
                </p>
              </div>
            </section>
            <PreviewPanel
              preview={selected ? preview[selected.machine_id] : undefined}
              machineId={state.selectedId}
              demo={demo}
              paused={selected?.stream_state === "PAUSED"}
            />
          </div>
          <section
            className="card activity-panel"
            id="activity"
            aria-labelledby="activity-title"
          >
            <div className="section-bar">
              <div>
                <div className="eyebrow">ACTIVITY LOG</div>
                <h2 id="activity-title">Nhật ký thao tác</h2>
              </div>
              <button
                className="button quiet"
                onClick={() => {
                  if (tab === "readings") setReadings([]);
                  else controller.clearJournal();
                }}
              >
                <Trash2 size={14} />
                Xóa log hiển thị
              </button>
            </div>
            <div
              className="activity-tabs"
              role="tablist"
              aria-label="Loại nhật ký"
            >
              <button
                role="tab"
                id="commands-tab"
                aria-controls="commands-panel"
                aria-selected={tab === "commands"}
                onClick={() => setTab("commands")}
              >
                Lệnh điều khiển <span>{state.journal.length}</span>
              </button>
              {demo && (
                <button
                  role="tab"
                  id="readings-tab"
                  aria-controls="readings-panel"
                  aria-selected={tab === "readings"}
                  onClick={() => setTab("readings")}
                >
                  Readings demo <span>{readings.length}</span>
                </button>
              )}
            </div>
            <div
              className="table-scroll"
              role="tabpanel"
              id={`${tab}-panel`}
              aria-labelledby={`${tab}-tab`}
              tabIndex={0}
            >
              {tab === "commands" ? (
                <table>
                  <thead>
                    <tr>
                      <th>Thời gian</th>
                      <th>Máy</th>
                      <th>Thao tác</th>
                      <th>Kết quả</th>
                      <th>Đối chiếu</th>
                    </tr>
                  </thead>
                  <tbody>
                    {state.journal.map((entry) => (
                      <tr key={entry.id}>
                        <td className="mono muted">{time(entry.createdAt)}</td>
                        <td>
                          <strong className="mono">{entry.machineId}</strong>
                        </td>
                        <td>
                          {actionLabel(entry)}
                          <small
                            className="table-subline mono"
                            title={entry.id}
                          >
                            {entry.id.slice(0, 8)} · expected v
                            {entry.body.expected_control_version}
                          </small>
                        </td>
                        <td>
                          <span
                            className={`badge ${entry.status === "confirmed" ? "teal" : entry.status === "pending" ? "blue" : "amber"}`}
                          >
                            {entry.status === "pending" && (
                              <LoaderCircle size={11} className="spin" />
                            )}
                            {statusLabels[entry.status]}
                          </span>
                          <small className="table-subline">
                            {entry.message}
                          </small>
                        </td>
                        <td>
                          {entry.status === "unknown" ? (
                            <div className="retry-actions">
                              <button
                                className="text-button"
                                onClick={() => {
                                  void controller.reconcile(entry.machineId);
                                }}
                              >
                                Đọc lại trạng thái
                              </button>
                              <button
                                className="text-button"
                                onClick={() => {
                                  void controller.retry(entry.id);
                                }}
                              >
                                Thử lại cùng mã lệnh
                              </button>
                            </div>
                          ) : (
                            <span className="muted">—</span>
                          )}
                        </td>
                      </tr>
                    ))}
                    {!state.journal.length && (
                      <tr>
                        <td colSpan={5}>
                          <div className="log-empty">
                            <Terminal size={21} />
                            <span>
                              Chưa có thao tác. Lệnh điều khiển và kết quả sẽ
                              xuất hiện tại đây.
                            </span>
                          </div>
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              ) : (
                <table>
                  <thead>
                    <tr>
                      <th>Thời gian</th>
                      <th>Reading demo</th>
                      <th>Máy</th>
                      <th>RPM</th>
                      <th>Scenario</th>
                      <th>Nguồn</th>
                    </tr>
                  </thead>
                  <tbody>
                    {readings.map((reading) => (
                      <tr key={reading.id}>
                        <td className="mono muted">{time(reading.time)}</td>
                        <td className="mono">{reading.id}</td>
                        <td className="mono">{reading.machineId}</td>
                        <td>{reading.rpm.toLocaleString("vi-VN")}</td>
                        <td>{scenarioLabel(reading.scenario)}</td>
                        <td>
                          <span className="badge neutral">Browser demo</span>
                        </td>
                      </tr>
                    ))}
                    {!readings.length && (
                      <tr>
                        <td colSpan={6}>
                          <div className="log-empty">
                            Chưa có reading demo trong phiên này.
                          </div>
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              )}
            </div>
            <div className="log-footer">
              <span>
                <i className={`status-dot ${stale ? "amber" : ""}`} />
                {state.lastSync
                  ? `Đọc thành công lúc ${time(state.lastSync)}`
                  : "Chưa có lần đọc thành công"}
              </span>
              <span>
                {demo
                  ? "Demo state lưu trong tab · preview không phải dữ liệu thật"
                  : "Nhật ký UI không thay thế audit log tại Sensor"}
              </span>
            </div>
          </section>
          <section className="pipeline-strip">
            <div>
              <div className="eyebrow">DATA FLOW</div>
              <h3>Luồng dữ liệu mục tiêu</h3>
            </div>
            <div className="pipeline-steps">
              {[
                "Sensor",
                "Ingestion",
                "Kafka",
                "Processing",
                "Business UI",
              ].map((step, i) => (
                <span key={step}>
                  <b>{step}</b>
                  {i < 4 && <ChevronRight size={14} />}
                </span>
              ))}
            </div>
            <p>
              <span className="mono">202</span> là broker ACK. Kết quả ML và
              cảnh báo được xử lý bất đồng bộ.
            </p>
          </section>
          <footer className="page-footer">
            <span>
              MaintAI <b>Sensor Simulator</b>{" "}
              <span className="footer-separator">/</span> Môi trường giả lập
            </span>
            <span>
              <ShieldCheck size={12} />
              Không điều khiển thiết bị vật lý
            </span>
          </footer>
        </main>
      </div>
      <dialog
        ref={settings}
        className="settings-dialog"
        aria-labelledby="settings-title"
      >
        <div className="dialog-heading">
          <div>
            <div className="eyebrow">WORKSPACE</div>
            <h2 id="settings-title">Kết nối & phiên truy cập</h2>
          </div>
          <button
            className="icon-button"
            aria-label="Đóng cấu hình"
            onClick={() => settings.current?.close()}
          >
            <X size={19} />
          </button>
        </div>
        <dl>
          <div>
            <dt>Chế độ</dt>
            <dd>
              {demo
                ? "Demo cục bộ — dữ liệu trình duyệt"
                : "Live — Sensor Control API"}
            </dd>
          </div>
          <div>
            <dt>Nguồn dữ liệu</dt>
            <dd>
              {demo
                ? "Không gọi API bên ngoài"
                : config.apiBaseUrl || "Cùng origin với Simulator UI"}
            </dd>
          </div>
          <div>
            <dt>Đồng bộ nền</dt>
            <dd>{config.pollIntervalMs / 1000} giây · dừng khi tab bị ẩn</dd>
          </div>
          <div>
            <dt>Phiên truy cập</dt>
            <dd>{session.name}</dd>
          </div>
          <div>
            <dt>Xác thực</dt>
            <dd>
              {demo
                ? "Demo Operator"
                : config.auth.mode === "oidc"
                  ? "OIDC · Authorization Code + PKCE"
                  : "Anonymous localhost — chỉ phát triển"}
            </dd>
          </div>
        </dl>
        <div className="control-note">
          <CircleHelp size={16} />
          <p>
            {demo
              ? "Mở chế độ live bằng runtime config của ứng dụng. Demo không thể điều khiển Sensor đang chạy."
              : "Quyền và phạm vi máy do Sensor API kiểm tra. Thiếu quyền sẽ trả 403."}
          </p>
        </div>
        {!demo && config.auth.mode === "oidc" && (
          <button
            className="button primary full-width"
            onClick={() => {
              void authenticate(session.authenticated);
            }}
          >
            {session.authenticated ? <LogOut size={16} /> : <LogIn size={16} />}
            {session.authenticated
              ? "Đăng xuất khỏi Simulator UI"
              : "Đăng nhập"}
          </button>
        )}
        {!demo && config.auth.mode === "oidc" && session.authenticated && (
          <button
            className="button full-width"
            onClick={() => {
              void authenticate();
            }}
          >
            <LogIn size={16} /> Đăng nhập lại
          </button>
        )}
        <button
          className="button full-width"
          onClick={() => settings.current?.close()}
        >
          Đóng
        </button>
      </dialog>
    </div>
  );
}

function Metric({
  label,
  value,
  suffix,
  caption,
  icon,
  tone,
}: {
  label: string;
  value: string;
  suffix?: string;
  caption: string;
  icon: React.ReactNode;
  tone: string;
}) {
  return (
    <div className="card metric">
      <div className="metric-top">
        <span>{label}</span>
        <span className={`metric-icon ${tone}`}>{icon}</span>
      </div>
      <div className="metric-value">
        {value}
        <small>{suffix}</small>
      </div>
      <p>{caption}</p>
    </div>
  );
}
function FlaskIcon() {
  return (
    <svg
      width="20"
      height="20"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.7"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M9 3h6M10 3v6l-6 10a1.5 1.5 0 0 0 1.3 2h13.4a1.5 1.5 0 0 0 1.3-2L14 9V3M8 14h8" />
      <path d="m9 17 1 1m4-1 1 1" />
    </svg>
  );
}
