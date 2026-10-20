import { ApiError, parseControl } from "./api";
import type { Command, ControlApi, ControlList, ControlState } from "./types";

const seed = (): ControlState[] =>
  ["degradation", "normal", "critical", "recovery", "normal"].map(
    (scenario, i) => ({
      machine_id: `M-00${i + 1}`,
      stream_state: i === 3 ? "PAUSED" : "RUNNING",
      scenario,
      control_version: 1,
      updated_at: new Date().toISOString(),
      last_controlled_by: "demo-operator",
      source: "SIMULATOR_CONTROL",
    }),
  );
export class DemoControlApi implements ControlApi {
  private machines: ControlState[];
  private receipts: Record<string, { command: string; state: ControlState }> =
    {};
  constructor(private readonly storage: Storage) {
    try {
      const previous = JSON.parse(
        storage.getItem("maintai-simulator-demo-v1") ?? "null",
      );
      this.machines = Array.isArray(previous?.machines)
        ? previous.machines.map(parseControl)
        : seed();
      this.receipts = previous?.receipts ?? {};
    } catch {
      this.machines = seed();
    }
  }
  private wait() {
    return new Promise((resolve) => setTimeout(resolve, 180));
  }
  async list(): Promise<ControlList> {
    await this.wait();
    return {
      items: structuredClone(this.machines),
      as_of: new Date().toISOString(),
    };
  }
  async get(machineId: string) {
    await this.wait();
    const state = this.machines.find((m) => m.machine_id === machineId);
    if (!state)
      throw new ApiError(404, "NOT_FOUND", "Không tìm thấy máy demo.");
    return structuredClone(state);
  }
  async execute(command: Command) {
    await this.wait();
    const identity = JSON.stringify([
      command.machineId,
      command.action,
      command.body,
    ]);
    const receipt = this.receipts[command.id];
    if (receipt) {
      if (receipt.command !== identity)
        throw new ApiError(
          409,
          "IDEMPOTENCY_CONFLICT",
          "Mã lệnh đã được dùng.",
        );
      return structuredClone(receipt.state);
    }
    const previous = this.machines.find(
      (m) => m.machine_id === command.machineId,
    );
    if (!previous)
      throw new ApiError(404, "NOT_FOUND", "Không tìm thấy máy demo.");
    if (previous.control_version !== command.body.expected_control_version)
      throw new ApiError(
        409,
        "CONTROL_VERSION_CONFLICT",
        "Version đã thay đổi.",
      );
    const state: ControlState = {
      ...previous,
      control_version: previous.control_version + 1,
      updated_at: new Date().toISOString(),
    };
    if (command.action === "scenario") state.scenario = command.body.scenario!;
    else state.stream_state = command.action === "pause" ? "PAUSED" : "RUNNING";
    const machines = this.machines.map((m) =>
      m.machine_id === state.machine_id ? state : m,
    );
    const receipts = {
      ...this.receipts,
      [command.id]: { command: identity, state },
    };
    // Demo durability is tab-scoped. No Sensor process or external pipeline is involved.
    this.storage.setItem(
      "maintai-simulator-demo-v1",
      JSON.stringify({ machines, receipts }),
    );
    this.machines = machines;
    this.receipts = receipts;
    return structuredClone(state);
  }
}

export interface Preview {
  seq: number;
  rpm: number;
  torque: number;
  wear: number;
  air: number;
  process: number;
  type: string;
  history: number[];
}
export function nextPreview(
  previous: Preview | undefined,
  machine: ControlState,
): Preview {
  const base: Preview = previous ?? {
    seq: 0,
    rpm: 1520,
    torque: 39,
    wear: 80,
    air: 298.5,
    process: 308.2,
    type: "M",
    history: [1520],
  };
  if (machine.stream_state === "PAUSED") return base;
  const direction =
    machine.scenario === "degradation"
      ? 11
      : machine.scenario === "critical"
        ? 20
        : machine.scenario === "recovery"
          ? -15
          : 0;
  const rpm = Math.max(
    1420,
    Math.min(1980, base.rpm + direction + Math.sin(base.seq * 1.7) * 8),
  );
  return {
    ...base,
    seq: base.seq + 1,
    rpm: Math.round(rpm),
    torque: +(39 + (rpm - 1520) * 0.055).toFixed(1),
    wear: +(base.wear + 0.1).toFixed(1),
    process: +(308.2 + (rpm - 1520) * 0.008).toFixed(1),
    history: [...base.history, rpm].slice(-30),
  };
}
