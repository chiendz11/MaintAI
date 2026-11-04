export type Collection = "PROCESSING" | "RAW";
export type HistoryQuery = Record<string, string>;
export type ProcessingRecord = {
  result_id: string;
  source_event_id: string;
  machine_id: string;
  processed_at: string;
  source_server_accepted_at: string;
  features_raw: Record<string, unknown>;
  outcome: Record<string, unknown> & {
    kind: string;
    sequence_number: bigint;
    source_offset: bigint;
  };
  risk_snapshot: {
    risk_score: number;
    machine_state: string;
    policy_version: string;
  } | null;
};
export type RawRecord = {
  event_id: string;
  machine_id: string;
  stream_epoch: string;
  sequence_number: bigint;
  received_at: string;
  server_accepted_at: string;
  features_raw: Record<string, unknown>;
  validation_status: string;
  result_id: string | null;
  receipt_id: string;
  kafka: { topic: string; partition: number; offset: bigint };
};
export type HistoryPage = {
  schema_version: string;
  profile: string;
  snapshot_id: string;
  as_of: string;
  expires_at: string;
  coverage: string;
  collection: Collection;
  time_basis: string;
  next_cursor: string | null;
  items: (ProcessingRecord | RawRecord)[];
  catalog_generation?: string | null;
  partition_coverage?: unknown[];
};
export type Marker = {
  kind: "PREDICTION" | "ALERT";
  reference_id: string;
  source_event_id: string;
  position_at: string | null;
  source_received_at: string | null;
  source_server_accepted_at: string | null;
  processed_at: string | null;
};
export type Chart = Omit<HistoryPage, "items"> & {
  metric: string;
  unit: string;
  resolution: string;
  buckets: {
    start: string;
    end: string;
    min: number | null;
    max: number | null;
    avg: number | null;
    count: number;
    invalid_count: number;
  }[];
  markers: Marker[];
  markers_next_cursor: string | null;
};
export type ExportRequest = {
  profile: "M6-HISTORY-01/1.0.0";
  machine_ids: string[];
  from: string;
  to: string;
  resource: Collection;
  format: "CSV" | "PARQUET";
  delivery?: "AUTO" | "JOB";
  snapshot_id?: string;
  model_version?: string;
  result_kind?: string;
};
export type ExportJob = {
  job_id: string;
  status: "QUEUED" | "RUNNING" | "SUCCEEDED" | "FAILED" | "EXPIRED";
  created_at: string;
  updated_at: string;
  expires_at: string | null;
  error_code: string | null;
  artifact_ref: unknown;
};
export type ExportAccepted =
  { file: Blob; job?: never } | { job: ExportJob; file?: never };
export interface HistoryApi {
  readonly defaultRange?: { from: string; to: string };
  page(
    machine: string,
    collection: Collection,
    query: HistoryQuery,
    signal: AbortSignal,
  ): Promise<HistoryPage>;
  chart(
    machine: string,
    query: HistoryQuery,
    signal: AbortSignal,
  ): Promise<Chart>;
  export(
    value: ExportRequest,
    key: string,
    signal: AbortSignal,
  ): Promise<ExportAccepted>;
  job(id: string, signal: AbortSignal): Promise<ExportJob>;
  download(id: string, signal: AbortSignal): Promise<Blob>;
}
