import { DemoAlerts } from "./demo-alerts";
import { DemoHistory } from "./demo-history";
import type { MachineApi } from "./client";
import { ApiError } from "./client";
import { parseLiveSnapshot } from "./codec";
import type { LiveEvent, MachineDetail, MachinePage } from "../types/live";

// Explicit offline fixtures. This driver is never selected after a live failure.
export class DemoApi extends DemoAlerts implements MachineApi {
  readonly history = new DemoHistory();
  private sequence = 0;
  private readings = new Map<string, number>();
  private snapshots = new Map<string, ReturnType<typeof parseLiveSnapshot>>();
  private sample(machine: string) {
    if (!["M-001", "M-002", "M-003"].includes(machine))
      throw new ApiError(404, "MACHINE_NOT_FOUND");
    const sequence = ++this.sequence,
      timestamp = new Date().toISOString();
    const previous = this.snapshots.get(machine);
    const count = (this.readings.get(machine) ?? 0) + 1;
    this.readings.set(machine, count);
    const warming = machine === "M-003" && count <= 2;
    const invalid = !warming && sequence % 5 === 0,
      failed = !warming && sequence % 7 === 0;
    const value = parseLiveSnapshot(
      JSON.stringify({
        machine_id: machine,
        as_of: timestamp,
        telemetry: {
          event_id: `demo-${sequence}`,
          stream_epoch: "demo",
          sequence_number: sequence,
          received_at: timestamp,
          server_accepted_at: timestamp,
          features_raw: invalid
            ? { rotational_speed_rpm: "invalid fixture" }
            : {
                product_type: "M",
                air_temperature_k: 298.1,
                process_temperature_k: 308.6,
                rotational_speed_rpm: 1500 + (sequence % 30),
                torque_nm: 42.8,
                tool_wear_min: 108,
              },
          validation_status: invalid ? "INVALID" : "VALID",
        },
        latest_valid_prediction:
          invalid || failed || warming
            ? null
            : {
                source_event_id: `demo-${sequence}`,
                stream_epoch: "demo",
                sequence_number: sequence,
                source_server_accepted_at: timestamp,
                prediction_id: `demo-pred-${sequence}`,
                failure_probability: 0.12,
                prediction: "NORMAL",
                machine_state_at_prediction: "NORMAL",
                model_version: "demo-model",
                preprocessor_version: "demo-preprocessor",
                dataset_version: "demo-dataset",
                policy_version: "demo-policy",
                completed_at: timestamp,
              },
        latest_processing: {
          source_event_id: `demo-${sequence}`,
          stream_epoch: "demo",
          sequence_number: sequence,
          processing_status: warming
            ? "PENDING"
            : invalid
              ? "INVALID"
              : failed
                ? "FAILED"
                : "COMPLETED",
          validation_status: invalid ? "INVALID" : "VALID",
          updated_at: timestamp,
          failure_code: failed ? "ML_UNAVAILABLE" : null,
        },
      }),
    );
    if (!value.latest_valid_prediction && previous)
      value.latest_valid_prediction = previous.latest_valid_prediction;
    this.snapshots.set(machine, value);
    return value;
  }
  async live(machine: string, _signal: AbortSignal) {
    return this.snapshots.get(machine) ?? this.sample(machine);
  }
  async detail(machine: string, signal: AbortSignal): Promise<MachineDetail> {
    const value = await this.live(machine, signal);
    return {
      machine_id: machine,
      display_name: {
        "M-001": "Máy ép thủy lực",
        "M-002": "Trạm gia công",
        "M-003": "Băng tải đóng gói",
      }[machine]!,
      state:
        value.latest_processing?.processing_status === "INVALID"
          ? "INVALID"
          : value.latest_processing?.processing_status === "FAILED"
            ? "UNKNOWN"
            : "NORMAL",
      data_status: value.telemetry?.validation_status ?? null,
      last_seen_at: value.telemetry?.server_accepted_at ?? null,
      latest_sensor: null,
      latest_prediction: null,
      active_alerts: [],
    };
  }
  async fleet(
    state: string,
    offset: number,
    signal: AbortSignal,
  ): Promise<MachinePage> {
    const values = await Promise.all(
      ["M-001", "M-002", "M-003"].map((machine) =>
        this.detail(machine, signal),
      ),
    );
    const filtered = values.filter((value) => !state || value.state === state);
    return {
      items: filtered.slice(offset, offset + 50).map((value) => ({
        machine_id: value.machine_id,
        display_name: value.display_name,
        state: value.state,
        data_status: value.data_status,
        last_seen_at: value.last_seen_at,
        latest_risk_score: value.state === "NORMAL" ? 0.2 : null,
        active_alert_count: 0,
      })),
      total: filtered.length,
    };
  }
  async stream(
    machine: string,
    signal: AbortSignal,
    ready: () => void,
    event: (value: LiveEvent) => void,
  ): Promise<void> {
    this.sample(machine);
    ready();
    await new Promise<void>((resolve) => {
      const timer = setInterval(() => {
        const value = this.sample(machine);
        for (const [type, key] of [
          ["telemetry.received", "telemetry"],
          ["prediction.completed", "latest_valid_prediction"],
          ["processing.status_changed", "latest_processing"],
        ] as const) {
          if (value[key])
            event({
              schema_version: "1.0.0",
              type,
              machine_id: machine,
              data: value[key]!,
              occurred_at: new Date().toISOString(),
            });
        }
      }, 1000);
      const stop = () => {
        clearInterval(timer);
        signal.removeEventListener("abort", stop);
        resolve();
      };
      signal.addEventListener("abort", stop, { once: true });
      if (signal.aborted) stop();
    });
  }
}
