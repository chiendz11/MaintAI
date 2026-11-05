"""Add generation serving and durable recovery metadata; retain all legacy data."""

from alembic import op

revision = "business_0007_rebuild"
down_revision = "business_0006_drift_catalog"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    connection.exec_driver_sql("""
    CREATE TABLE projection_generations (
        generation_id TEXT PRIMARY KEY,
        state TEXT NOT NULL CHECK(state IN ('BUILDING','ACTIVE','FAILED')),
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        activated_at TIMESTAMPTZ, deactivated_at TIMESTAMPTZ
    );
    CREATE TABLE projection_control (
        singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
        active_generation TEXT REFERENCES projection_generations,
        building_job TEXT
    );
    INSERT INTO projection_control(singleton) VALUES(TRUE);
    CREATE TABLE projection_rebuild_jobs (
        job_id TEXT PRIMARY KEY,
        generation_id TEXT NOT NULL UNIQUE REFERENCES projection_generations,
        expected_active TEXT NOT NULL REFERENCES projection_generations,
        status TEXT NOT NULL CHECK(status IN
            ('QUEUED','RUNNING','VERIFIED','SUCCEEDED','FAILED','ABORTED')),
        input BYTEA NOT NULL,
        registry_sha256 TEXT NOT NULL,
        deadline TIMESTAMPTZ NOT NULL,
        journal_budget BIGINT NOT NULL CHECK(journal_budget>0),
        journal_bytes BIGINT NOT NULL DEFAULT 0,
        journal_count BIGINT NOT NULL DEFAULT 0,
        applied_count BIGINT NOT NULL DEFAULT 0,
        checkpoint BYTEA NOT NULL,
        baseline_done BOOLEAN NOT NULL DEFAULT FALSE,
        applied_journal BIGINT NOT NULL DEFAULT 0,
        lease_owner TEXT, lease_until TIMESTAMPTZ,
        fencing_token BIGINT NOT NULL DEFAULT 0,
        last_error TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
    );
    ALTER TABLE projection_control ADD CONSTRAINT projection_building_job
        FOREIGN KEY(building_job) REFERENCES projection_rebuild_jobs(job_id);
    CREATE UNIQUE INDEX projection_one_rebuild ON projection_rebuild_jobs((TRUE))
        WHERE status IN ('QUEUED','RUNNING','VERIFIED');
    CREATE FUNCTION projection_rebuild_input_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
    BEGIN
        IF (OLD.job_id,OLD.generation_id,OLD.expected_active,OLD.input,OLD.registry_sha256,
            OLD.deadline,OLD.journal_budget,OLD.created_at) IS DISTINCT FROM
           (NEW.job_id,NEW.generation_id,NEW.expected_active,NEW.input,NEW.registry_sha256,
            NEW.deadline,NEW.journal_budget,NEW.created_at) THEN
            RAISE EXCEPTION 'Projection rebuild input is immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END $$;
    CREATE TRIGGER projection_rebuild_input_immutable BEFORE UPDATE ON projection_rebuild_jobs
        FOR EACH ROW EXECUTE FUNCTION projection_rebuild_input_immutable();
    CREATE TABLE projection_members (
        job_id TEXT NOT NULL REFERENCES projection_rebuild_jobs,
        consumer_group TEXT NOT NULL, topic TEXT NOT NULL, message_id TEXT NOT NULL,
        identity_sha256 TEXT NOT NULL, source_partition INTEGER NOT NULL,
        source_offset BIGINT NOT NULL, schema_version TEXT NOT NULL,
        applied BOOLEAN NOT NULL DEFAULT FALSE, source_ref BYTEA,
        PRIMARY KEY(job_id,consumer_group,topic,message_id)
    );
    CREATE INDEX projection_members_pending ON projection_members
        (job_id,consumer_group,topic,message_id) WHERE NOT applied;
    CREATE TABLE projection_journal (
        journal_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        job_id TEXT NOT NULL REFERENCES projection_rebuild_jobs,
        topic TEXT NOT NULL, message_id TEXT NOT NULL,
        identity_sha256 TEXT NOT NULL, wire_sha256 TEXT NOT NULL,
        wire BYTEA NOT NULL, source_partition INTEGER NOT NULL,
        source_offset BIGINT NOT NULL,
        UNIQUE(job_id,topic,message_id)
    );
    CREATE INDEX projection_journal_job ON projection_journal(job_id,journal_id);
    CREATE TABLE projection_row_checksums (
        generation_id TEXT NOT NULL REFERENCES projection_generations,
        axis TEXT NOT NULL CHECK(axis IN
            ('latest_telemetry','latest_processing','latest_valid_prediction')),
        machine_id TEXT NOT NULL, sha256 TEXT NOT NULL,
        PRIMARY KEY(generation_id,axis,machine_id)
    );
    CREATE TABLE projection_catalog_seen (
        job_id TEXT NOT NULL REFERENCES projection_rebuild_jobs,
        generation_id TEXT NOT NULL, walk_id TEXT NOT NULL, PRIMARY KEY(job_id,generation_id)
    );
    CREATE TABLE projection_raw_entries (
        job_id TEXT NOT NULL REFERENCES projection_rebuild_jobs,
        topic TEXT NOT NULL, source_partition INTEGER NOT NULL, source_offset BIGINT NOT NULL,
        entry BYTEA NOT NULL, catalog_ref BYTEA NOT NULL,
        PRIMARY KEY(job_id,topic,source_partition,source_offset)
    );
    CREATE TABLE projection_source_inbox (
        generation_id TEXT NOT NULL REFERENCES projection_generations,
        machine_id TEXT NOT NULL REFERENCES machines,
        sequence_number NUMERIC NOT NULL CHECK(sequence_number>0
            AND sequence_number<'Infinity'::numeric AND sequence_number=trunc(sequence_number)),
        source_event_id TEXT NOT NULL, stream_epoch TEXT NOT NULL,
        reading_sha256 TEXT, outcome_sha256 TEXT,
        validation_status TEXT CHECK(validation_status IN ('VALID','INVALID')),
        PRIMARY KEY(generation_id,machine_id,sequence_number),
        UNIQUE(generation_id,source_event_id)
    );
    """)
    for axis in ("latest_telemetry", "latest_processing", "latest_valid_prediction"):
        connection.exec_driver_sql(f"""CREATE TABLE projection_{axis} (
            generation_id TEXT NOT NULL REFERENCES projection_generations,
            machine_id TEXT NOT NULL REFERENCES machines,
            source_event_id TEXT NOT NULL, stream_epoch TEXT NOT NULL,
            sequence_number NUMERIC NOT NULL CHECK(sequence_number>0
                AND sequence_number<'Infinity'::numeric AND sequence_number=trunc(sequence_number)),
            identity_sha256 TEXT NOT NULL CHECK(identity_sha256 ~ '^[0-9a-f]{{64}}$'),
            snapshot BYTEA NOT NULL, metadata BYTEA,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY(generation_id,machine_id)
        )""")


def downgrade():
    raise RuntimeError("Rebuild history requires an explicit reviewed retention migration")
