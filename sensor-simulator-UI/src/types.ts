export type StreamState = "RUNNING" | "PAUSED";
export interface ControlState {
  machine_id: string;
  stream_state: StreamState;
  scenario: string;
  control_version: number;
  updated_at: string;
  last_controlled_by: string;
  source: "SIMULATOR_CONTROL";
}
export interface ControlList {
  items: ControlState[];
  as_of: string;
}
export type Action = "pause" | "resume" | "scenario";
export interface Command {
  id: string;
  machineId: string;
  action: Action;
  body: { expected_control_version: number; scenario?: string };
  createdAt: string;
}
export type CommandStatus =
  "pending" | "confirmed" | "unknown" | "conflict" | "rejected";
export interface CommandEntry extends Command {
  status: CommandStatus;
  message: string;
}
export interface ControlApi {
  list(): Promise<ControlList>;
  get(machineId: string): Promise<ControlState>;
  execute(command: Command): Promise<ControlState>;
}
export const scenarios = [
  {
    id: "normal",
    label: "Bình thường",
    caption: "Dao động quanh mức vận hành ổn định.",
    symbol: "normal",
  },
  {
    id: "degradation",
    label: "Suy giảm",
    caption: "Mô phỏng thông số tăng dần theo thời gian.",
    symbol: "degradation",
  },
  {
    id: "critical",
    label: "Critical",
    caption: "Mô phỏng điều kiện có nguy cơ cao.",
    symbol: "critical",
  },
  {
    id: "recovery",
    label: "Hồi phục",
    caption: "Đưa thông số dần về vùng bình thường.",
    symbol: "recovery",
  },
] as const;
export const scenarioLabel = (id: string) =>
  scenarios.find((s) => s.id === id)?.label ?? id;
