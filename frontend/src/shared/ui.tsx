import type { ReactNode } from "react";

export function Badge({ value }: { value: string }) {
  const tone = ["NORMAL", "VALID", "FRESH", "CONNECTED", "COMPLETED"].includes(
    value,
  )
    ? "good"
    : ["INVALID", "FAILED", "CRITICAL", "OFFLINE"].includes(value)
      ? "bad"
      : ["WATCH", "HIGH", "DELAYED", "STALE", "RECONNECTING"].includes(value)
        ? "warn"
        : "neutral";
  return (
    <span className={`badge ${tone}`}>
      <span className="dot" />
      {value}
    </span>
  );
}
export function Panel({
  title,
  eyebrow,
  children,
  action,
}: {
  title: string;
  eyebrow: string;
  children: ReactNode;
  action?: ReactNode;
}) {
  return (
    <section className="panel">
      <div className="panel-heading">
        <div>
          <p className="eyebrow">{eyebrow}</p>
          <h2>{title}</h2>
        </div>
        {action}
      </div>
      {children}
    </section>
  );
}
export function Empty({ children }: { children: ReactNode }) {
  return <div className="empty">{children}</div>;
}
export function Source({
  label,
  value,
}: {
  label: string;
  value: string | null | undefined;
}) {
  return (
    <div className="source">
      <span>{label}</span>
      <code>{value || "—"}</code>
    </div>
  );
}
export function ErrorNotice({
  error,
  retry,
}: {
  error: unknown;
  retry?: () => void;
}) {
  if (!error) return null;
  return (
    <div className="notice error" role="alert">
      <span>
        {error instanceof Error ? error.message : "Không thể tải dữ liệu."}
      </span>
      {retry && (
        <button className="button small secondary" onClick={retry}>
          Thử lại
        </button>
      )}
    </div>
  );
}
export function date(value: string | null | undefined): string {
  return value && Number.isFinite(Date.parse(value))
    ? new Date(value).toLocaleString("vi-VN")
    : "—";
}
export function age(value: string | null | undefined, now: number): string {
  if (!value) return "Chưa có dữ liệu";
  const seconds = Math.max(0, Math.floor((now - Date.parse(value)) / 1000));
  return Number.isFinite(seconds)
    ? seconds < 60
      ? `${seconds} giây trước`
      : `${Math.floor(seconds / 60)} phút trước`
    : "Không rõ tuổi dữ liệu";
}
