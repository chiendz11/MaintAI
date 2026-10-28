export type AlertStatus =
  | "OPEN"
  | "ACKNOWLEDGED"
  | "IN_PROGRESS"
  | "ESCALATED"
  | "RESOLVED"
  | "FALSE_ALERT";
export type AlertItem = {
  alert_id: string;
  machine_id: string;
  severity: string;
  status: AlertStatus;
  risk_score: number;
  created_at: string;
};
export type AlertPage = { items: AlertItem[]; total: number };
export type AlertDetail = {
  alert: AlertItem & { model_version: string; policy_version: string };
  sensor_event: {
    event_id: string;
    validation_status: "VALID";
    features: Record<string, unknown>;
  };
  prediction: {
    prediction_id: string;
    prediction: string;
    failure_probability: number;
  };
  copilot: {
    status: string;
    brief_id: string | null;
    grounded_status: string | null;
  };
  history: {
    alert_event_id: string;
    event_type: string;
    from_status: string | null;
    to_status: string;
    actor_id: string | null;
    note: string | null;
    timestamp: string;
  }[];
};
export type AlertEvent = {
  schema_version: "1.0.0";
  type: "alert.created" | "alert.updated";
  machine_id: string;
  occurred_at: string;
  data: {
    alert_id: string;
    machine_id: string;
    severity: string;
    status: AlertStatus;
    updated_at: string;
  };
};
export type AlertAction =
  "ack" | "start" | "escalate" | "resolve" | "false-alert";
export type Transition = {
  alert_id: string;
  previous_status: AlertStatus;
  status: AlertStatus;
  updated_at: string;
};
export type Feedback = {
  technician_verdict: "CONFIRMED_ISSUE" | "FALSE_ALERT" | "INCONCLUSIVE";
  recommendation_feedback:
    "USEFUL" | "PARTIALLY_USEFUL" | "NOT_USEFUL" | "NOT_AVAILABLE";
  note?: string;
};
export type FeedbackCreated = {
  feedback_id: string;
  alert_id: string;
  recorded_at: string;
};
export interface AlertApi {
  alerts(
    filters: {
      status?: string;
      severity?: string;
      machine_id?: string;
      offset?: number;
    },
    signal: AbortSignal,
  ): Promise<AlertPage>;
  alert(id: string, signal: AbortSignal): Promise<AlertDetail>;
  action(
    id: string,
    action: AlertAction,
    body: Record<string, string>,
    signal: AbortSignal,
  ): Promise<Transition>;
  feedback(
    id: string,
    body: Feedback,
    signal: AbortSignal,
  ): Promise<FeedbackCreated>;
  alertStream(
    signal: AbortSignal,
    ready: () => void,
    event: (event: AlertEvent) => void,
  ): Promise<void>;
}
