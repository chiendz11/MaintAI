import { ApiError } from "./api";
import type { Action, CommandEntry, ControlApi, ControlState } from "./types";

export interface Snapshot {
  machines: ControlState[];
  observedAt: Record<string, number>;
  selectedId: string;
  syncing: boolean;
  lastSync: number | null;
  journal: CommandEntry[];
  notice: { kind: "error" | "warning" | "info"; text: string } | null;
}
export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
}

function restore(storage: StorageLike, key: string): CommandEntry[] {
  try {
    const raw: unknown = JSON.parse(storage.getItem(key) ?? "[]");
    if (!Array.isArray(raw)) return [];
    return raw
      .filter(
        (x): x is CommandEntry =>
          x &&
          typeof x.id === "string" &&
          typeof x.machineId === "string" &&
          ["pause", "resume", "scenario"].includes(x.action) &&
          ["pending", "unknown"].includes(x.status) &&
          Number.isSafeInteger(x.body?.expected_control_version) &&
          x.body.expected_control_version >= 0 &&
          (x.action !== "scenario" || typeof x.body.scenario === "string") &&
          typeof x.createdAt === "string",
      )
      .slice(0, 100)
      .map((x) => ({
        id: x.id,
        machineId: x.machineId,
        action: x.action,
        body: {
          expected_control_version: x.body.expected_control_version,
          ...(x.action === "scenario" ? { scenario: x.body.scenario } : {}),
        },
        createdAt: x.createdAt,
        status: "unknown",
        message:
          "Phiên trước chưa có ACK. Đọc lại trạng thái rồi thử lại cùng mã lệnh.",
      }));
  } catch {
    return [];
  }
}

export class SimulatorController {
  private listeners = new Set<() => void>();
  private active = new Set<string>();
  private blockedUntil = 0;
  private readPromise?: Promise<void>;
  private state: Snapshot;
  constructor(
    private readonly api: ControlApi,
    private readonly storage: StorageLike,
    private readonly storageKey: string,
    private readonly staleAfterMs: number,
  ) {
    this.state = {
      machines: [],
      observedAt: {},
      selectedId: "",
      syncing: false,
      lastSync: null,
      journal: restore(storage, storageKey),
      notice: null,
    };
  }
  getSnapshot = () => this.state;
  subscribe = (listener: () => void) => {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  };
  private update(patch: Partial<Snapshot>) {
    this.state = { ...this.state, ...patch };
    this.listeners.forEach((listener) => listener());
  }
  private persist(entries: CommandEntry[]) {
    // Write ahead of HTTP. Never persist bearer tokens or retry with a new identity.
    this.storage.setItem(
      this.storageKey,
      JSON.stringify(
        entries.filter((x) => ["pending", "unknown"].includes(x.status)),
      ),
    );
  }
  select(id: string) {
    this.update({ selectedId: id });
  }
  clearJournal() {
    this.update({
      journal: this.state.journal.filter((x) =>
        ["pending", "unknown"].includes(x.status),
      ),
    });
  }
  private merge(machine: ControlState) {
    const previous = this.state.machines.find(
      (x) => x.machine_id === machine.machine_id,
    );
    const machines = previous
      ? this.state.machines.map((x) =>
          x.machine_id === machine.machine_id &&
          x.control_version <= machine.control_version
            ? machine
            : x,
        )
      : [...this.state.machines, machine];
    this.update({
      machines,
      observedAt: {
        ...this.state.observedAt,
        [machine.machine_id]: Date.now(),
      },
    });
  }
  private errorNotice(error: unknown) {
    const e =
      error instanceof ApiError
        ? error
        : new ApiError(0, "UNAVAILABLE", "Không đọc được trạng thái máy.");
    if (e.status === 401)
      return "Phiên đăng nhập không hợp lệ hoặc đã hết hạn. Hãy đăng nhập lại.";
    if (e.status === 403)
      return "Bạn chưa có quyền điều khiển máy này. API cần SIMULATOR_OPERATOR/ADMIN và machine scope phù hợp.";
    if (e.status === 429)
      return "Control API đang giới hạn tần suất. Chờ một lúc trước khi đồng bộ lại.";
    return `${e.message} Dữ liệu trước đó chỉ là trạng thái được ghi nhận gần nhất.`;
  }
  refresh = (): Promise<void> => {
    if (this.readPromise) return this.readPromise;
    if (Date.now() < this.blockedUntil) return Promise.resolve();
    this.readPromise = this.readList().finally(() => {
      this.readPromise = undefined;
    });
    return this.readPromise;
  };
  private async readList() {
    this.update({ syncing: true });
    try {
      const list = await this.api.list();
      const now = Date.now();
      const asOf = Date.parse(list.as_of);
      if (asOf > now + 60000 || now - asOf > this.staleAfterMs)
        throw new ApiError(
          0,
          "STALE_SNAPSHOT",
          "Control API trả snapshot quá cũ hoặc clock lệch.",
        );
      const previous = new Map(
        this.state.machines.map((m) => [m.machine_id, m]),
      );
      const machines = list.items.map((m) => {
        const old = previous.get(m.machine_id);
        return old && old.control_version > m.control_version ? old : m;
      });
      const selectedId = machines.some(
        (m) => m.machine_id === this.state.selectedId,
      )
        ? this.state.selectedId
        : (machines[0]?.machine_id ?? "");
      this.update({
        machines,
        selectedId,
        observedAt: Object.fromEntries(
          machines.map((m) => [m.machine_id, now]),
        ),
        lastSync: now,
        notice: null,
      });
    } catch (error) {
      if (error instanceof ApiError && error.status === 429)
        this.blockedUntil =
          Date.now() + Math.min(300000, error.retryAfterMs || 15000);
      this.update({ notice: { kind: "error", text: this.errorNotice(error) } });
    } finally {
      this.update({ syncing: false });
    }
  }
  async reconcile(machineId: string) {
    try {
      this.merge(await this.api.get(machineId));
      this.update({
        notice: {
          kind: "info",
          text: "Đã đọc lại trạng thái. GET không xác nhận riêng lệnh bị timeout; dùng Thử lại cùng mã lệnh để nhận ACK.",
        },
      });
    } catch (error) {
      this.update({ notice: { kind: "error", text: this.errorNotice(error) } });
    }
  }
  async command(machineId: string, action: Action, scenario?: string) {
    const machine = this.state.machines.find((m) => m.machine_id === machineId);
    if (
      !machine ||
      this.active.has(machineId) ||
      this.state.journal.some(
        (x) =>
          x.machineId === machineId &&
          ["pending", "unknown"].includes(x.status),
      )
    )
      return;
    if (
      Date.now() - (this.state.observedAt[machineId] ?? 0) >
        this.staleAfterMs ||
      this.state.notice?.kind === "error"
    ) {
      this.update({
        notice: {
          kind: "warning",
          text: "Cần đồng bộ trạng thái mới trước khi gửi lệnh.",
        },
      });
      return;
    }
    const entry: CommandEntry = {
      id: crypto.randomUUID(),
      machineId,
      action,
      body: {
        expected_control_version: machine.control_version,
        ...(action === "scenario" ? { scenario } : {}),
      },
      createdAt: new Date().toISOString(),
      status: "pending",
      message: "Đang chờ Control API xác nhận…",
    };
    const journal = [entry, ...this.state.journal].slice(0, 100);
    try {
      this.persist(journal);
    } catch {
      this.update({
        notice: {
          kind: "error",
          text: "Trình duyệt không lưu được mã lệnh. Chưa gửi lệnh; hãy bật session storage rồi thử lại.",
        },
      });
      return;
    }
    this.update({ journal });
    await this.send(entry);
  }
  async retry(id: string) {
    const entry = this.state.journal.find(
      (x) => x.id === id && x.status === "unknown",
    );
    if (entry && !this.active.has(entry.machineId)) await this.send(entry);
  }
  private async send(command: CommandEntry) {
    this.active.add(command.machineId);
    this.setCommand(command.id, "pending", "Đang chờ Control API xác nhận…");
    try {
      const result = await this.api.execute(command);
      this.merge(result);
      this.setCommand(
        command.id,
        "confirmed",
        `Đã nhận ACK · control version ${result.control_version}`,
      );
      this.update({
        notice: {
          kind: "info",
          text: `${command.machineId}: Control API đã xác nhận lệnh. Pause không xóa backlog hoặc đóng cảnh báo.`,
        },
      });
    } catch (error) {
      const e =
        error instanceof ApiError
          ? error
          : new ApiError(0, "UNAVAILABLE", "Kết quả lệnh chưa xác định.");
      if (e.status === 409) {
        this.update({
          observedAt: { ...this.state.observedAt, [command.machineId]: 0 },
        });
        this.setCommand(
          command.id,
          "conflict",
          "Xung đột phiên bản hoặc mã lệnh. Đang đọc lại trạng thái máy.",
        );
        await this.reconcile(command.machineId);
        this.update({
          notice: {
            kind: "warning",
            text: "Có thay đổi từ phiên khác (409). Đã yêu cầu đọc lại trạng thái; kiểm tra rồi chọn thao tác mới.",
          },
        });
      } else if (e.uncertain || command.status === "unknown") {
        this.update({
          observedAt: { ...this.state.observedAt, [command.machineId]: 0 },
        });
        this.setCommand(
          command.id,
          "unknown",
          "Chưa xác định kết quả. Giữ nguyên mã lệnh và payload khi thử lại.",
        );
        await this.reconcile(command.machineId);
        this.update({
          notice: {
            kind: "warning",
            text: `${command.machineId}: chưa có ACK chắc chắn. Không gửi lệnh mới cho máy này cho tới khi đối chiếu lệnh cũ.`,
          },
        });
      } else {
        this.setCommand(command.id, "rejected", this.errorNotice(e));
        this.update({ notice: { kind: "error", text: this.errorNotice(e) } });
      }
    } finally {
      this.active.delete(command.machineId);
    }
  }
  private setCommand(
    id: string,
    status: CommandEntry["status"],
    message: string,
  ) {
    const journal = this.state.journal.map((x) =>
      x.id === id ? { ...x, status, message } : x,
    );
    try {
      this.persist(journal);
    } catch {
      this.update({
        notice: {
          kind: "warning",
          text: "Không cập nhật được bộ nhớ phiên. Giữ tab này để đối chiếu lệnh; tải lại có thể hiện lệnh cũ là chưa rõ kết quả.",
        },
      });
    }
    this.update({ journal });
  }
}
