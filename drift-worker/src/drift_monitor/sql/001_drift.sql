CREATE TABLE IF NOT EXISTS drift_reports (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    report_id text NOT NULL UNIQUE,
    computed_at timestamptz NOT NULL,
    detector_version text NOT NULL,
    reference_dataset_version text NOT NULL,
    model_version text NOT NULL,
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    sample_count integer NOT NULL CHECK (sample_count >= 0),
    overall_status text NOT NULL CHECK (overall_status IN
        ('STABLE', 'WARNING', 'CRITICAL', 'INSUFFICIENT_DATA', 'ERROR')),
    drifted_feature_count integer NOT NULL,
    warning_feature_count integer NOT NULL,
    critical_feature_count integer NOT NULL,
    report_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS drift_reports_model_time_idx
    ON drift_reports (model_version, computed_at DESC);

CREATE TABLE IF NOT EXISTS drift_checkpoints (
    worker_name text PRIMARY KEY,
    last_processed_event_at timestamptz,
    last_processed_event_id text,
    last_success_at timestamptz NOT NULL,
    last_report_id text NOT NULL REFERENCES drift_reports(report_id),
    updated_at timestamptz NOT NULL DEFAULT now()
);
