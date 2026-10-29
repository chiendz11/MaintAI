import { ApiError } from "../api/client";
import type { AlertApi, AlertAction, AlertStatus } from "../types/alerts";

export const changes = new EventTarget();
export const invalidateAlerts = () =>
  changes.dispatchEvent(new Event("change"));
export const allowedActions: Record<AlertStatus, AlertAction[]> = {
  OPEN: ["ack", "escalate", "false-alert"],
  ACKNOWLEDGED: ["start", "escalate", "resolve", "false-alert"],
  IN_PROGRESS: ["escalate", "resolve", "false-alert"],
  ESCALATED: ["start", "resolve"],
  RESOLVED: [],
  FALSE_ALERT: [],
};
export class UnknownOutcome extends Error {
  constructor(readonly feedback = false) {
    super(
      feedback
        ? "Kết quả gửi feedback chưa xác định. Hệ thống không tự gửi lại; lịch sử Alert không xác nhận được feedback."
        : "Kết quả thao tác chưa xác định. Đã yêu cầu tải lại trạng thái và lịch sử để đối soát; hệ thống không tự gửi lại thao tác.",
    );
  }
}
export function watchAlerts(api: AlertApi): () => void {
  const controller = new AbortController();
  let timer: ReturnType<typeof setTimeout>;
  void (async () => {
    let attempt = 0;
    while (!controller.signal.aborted) {
      try {
        await api.alertStream(
          controller.signal,
          invalidateAlerts,
          invalidateAlerts,
        );
      } catch (error) {
        if (error instanceof ApiError && [401, 403, 404].includes(error.status))
          break;
      }
      if (controller.signal.aborted) break;
      await new Promise<void>((resolve) => {
        const done = () => {
          clearTimeout(timer);
          controller.signal.removeEventListener("abort", done);
          resolve();
        };
        controller.signal.addEventListener("abort", done, { once: true });
        timer = setTimeout(
          done,
          Math.min(30000, 1000 * 2 ** Math.min(attempt++, 5)),
        );
      });
    }
  })();
  return () => {
    controller.abort();
    clearTimeout(timer);
  };
}
