export type Validation = "UNVERIFIED" | "VALID" | "INVALID";
export type ProcessingStatus =
  "PENDING" | "PROCESSING" | "RETRYING" | "COMPLETED" | "INVALID" | "FAILED";
export type Cursor = { stream_epoch: string; sequence_number: bigint };
export type Telemetry = Cursor & {
  event_id: string;
  server_accepted_at: string;
  received_at: string;
  features_raw: Record<string, unknown>;
  validation_status: Validation;
};
export type Prediction = Cursor & {
  source_event_id: string;
  source_server_accepted_at: string;
  prediction_id: string;
  prediction: "NORMAL" | "FAILURE";
  failure_probability: number;
  machine_state_at_prediction: "NORMAL" | "WATCH" | "HIGH" | "CRITICAL";
  model_version: string;
  preprocessor_version: string;
  dataset_version: string;
  policy_version: string;
  completed_at: string;
};
export type Processing = Cursor & {
  source_event_id: string;
  processing_status: ProcessingStatus;
  validation_status: "VALID" | "INVALID" | null;
  updated_at: string;
  failure_code: string | null;
};
export type LiveSnapshot = {
  machine_id: string;
  telemetry: Telemetry | null;
  latest_valid_prediction: Prediction | null;
  latest_processing: Processing | null;
  as_of: string;
};
export type LiveEvent = {
  schema_version: "1.0.0";
  type:
    "telemetry.received" | "prediction.completed" | "processing.status_changed";
  machine_id: string;
  data: Telemetry | Prediction | Processing;
  occurred_at: string;
};
export type MachineItem = {
  machine_id: string;
  display_name: string;
  state: string;
  data_status: string | null;
  latest_risk_score: number | null;
  active_alert_count: number;
  last_seen_at: string | null;
};
export type MachinePage = { items: MachineItem[]; total: number };
export type MachineDetail = {
  machine_id: string;
  display_name: string;
  state: string;
  data_status: string | null;
  last_seen_at: string | null;
  latest_sensor: unknown;
  latest_prediction: unknown;
  active_alerts: string[];
};
