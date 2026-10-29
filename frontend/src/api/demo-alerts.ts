import { ApiError } from "./client";
import { invalidateAlerts, allowedActions } from "../realtime/alerts";
import type {
  AlertAction,
  AlertApi,
  AlertDetail,
  AlertPage,
  Feedback,
  FeedbackCreated,
  Transition,
} from "../types/alerts";

export class DemoAlerts implements AlertApi {
  private values = new Map<string, AlertDetail>([
    [
      "demo-alert-1",
      {
        alert: {
          alert_id: "demo-alert-1",
          machine_id: "M-001",
          severity: "HIGH",
          status: "OPEN",
          risk_score: 0.72,
          model_version: "demo-model",
          policy_version: "demo-policy",
          created_at: new Date().toISOString(),
        },
        sensor_event: {
          event_id: "demo-source-1",
          validation_status: "VALID",
          features: {
            product_type: "M",
            air_temperature_k: 298.1,
            process_temperature_k: 308.6,
            rotational_speed_rpm: 1500,
            torque_nm: 42.8,
            tool_wear_min: 108,
          },
        },
        prediction: {
          prediction_id: "demo-prediction-1",
          prediction: "FAILURE",
          failure_probability: 0.8,
        },
        copilot: {
          status: "UNAVAILABLE",
          brief_id: null,
          grounded_status: null,
        },
        history: [
          {
            alert_event_id: "demo-created",
            event_type: "CREATED",
            from_status: null,
            to_status: "OPEN",
            actor_id: null,
            note: null,
            timestamp: new Date().toISOString(),
          },
        ],
      },
    ],
  ]);
  async alerts(
    filters: {
      status?: string;
      severity?: string;
      machine_id?: string;
      offset?: number;
    },
    _signal: AbortSignal,
  ): Promise<AlertPage> {
    const all = [...this.values.values()]
      .map((v) => v.alert)
      .filter(
        (v) =>
          (!filters.status || v.status === filters.status) &&
          (!filters.severity || v.severity === filters.severity) &&
          (!filters.machine_id || v.machine_id === filters.machine_id),
      );
    return structuredClone({
      total: all.length,
      items: all
        .slice(filters.offset ?? 0, (filters.offset ?? 0) + 50)
        .map(
          ({
            alert_id,
            machine_id,
            severity,
            status,
            risk_score,
            created_at,
          }) => ({
            alert_id,
            machine_id,
            severity,
            status,
            risk_score,
            created_at,
          }),
        ),
    });
  }
  async alert(id: string, _signal: AbortSignal) {
    const value = this.values.get(id);
    if (!value) throw new ApiError(404, "ALERT_NOT_FOUND");
    return structuredClone(value);
  }
  async action(
    id: string,
    action: AlertAction,
    body: Record<string, string>,
    signal: AbortSignal,
  ): Promise<Transition> {
    const value = await this.alert(id, signal);
    if (!allowedActions[value.alert.status].includes(action))
      throw new ApiError(409, "INVALID_ALERT_TRANSITION");
    if (
      (action === "resolve" && !body.resolution) ||
      (action === "false-alert" && !body.reason)
    )
      throw new ApiError(422, "VALIDATION_FAILED");
    const next = {
      ack: "ACKNOWLEDGED",
      start: "IN_PROGRESS",
      escalate: "ESCALATED",
      resolve: "RESOLVED",
      "false-alert": "FALSE_ALERT",
    } as const;
    const before = value.alert.status;
    value.alert.status = next[action];
    const timestamp = new Date().toISOString();
    value.history.push({
      alert_event_id: crypto.randomUUID(),
      event_type: next[action],
      from_status: before,
      to_status: next[action],
      actor_id: body.actor_id,
      note: body.note || null,
      timestamp,
    });
    this.values.set(id, value);
    invalidateAlerts();
    return {
      alert_id: id,
      previous_status: before,
      status: next[action],
      updated_at: timestamp,
    };
  }
  async feedback(
    id: string,
    _body: Feedback,
    signal: AbortSignal,
  ): Promise<FeedbackCreated> {
    await this.alert(id, signal);
    return {
      feedback_id: "demo-feedback-" + crypto.randomUUID(),
      alert_id: id,
      recorded_at: new Date().toISOString(),
    };
  }
  async alertStream(signal: AbortSignal, ready: () => void): Promise<void> {
    ready();
    if (signal.aborted) return;
    await new Promise<void>((resolve) =>
      signal.addEventListener("abort", () => resolve(), { once: true }),
    );
  }
}
