import { readFileSync } from "node:fs";
import Ajv from "ajv/dist/2020.js";
import addFormats from "ajv-formats";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, HttpControlApi, parseControl, parseList } from "../src/api";
import { parseConfig } from "../src/config";
import { SimulatorController, type StorageLike } from "../src/controller";
import type { Command, ControlApi, ControlState } from "../src/types";

const fixture: ControlState = JSON.parse(
  readFileSync(
    new URL("../../contracts/v1.3/examples/control.json", import.meta.url),
    "utf8",
  ),
);
const schema = JSON.parse(
  readFileSync(
    new URL(
      "../../contracts/v1.3/schemas/runtime.schema.json",
      import.meta.url,
    ),
    "utf8",
  ),
);
const ajv = new Ajv({ strict: false });
addFormats(ajv);
function validate(name: string, value: unknown) {
  const check = ajv.compile({
    ...schema,
    $id: undefined,
    $ref: `#/$defs/${name}`,
  });
  expect(check(value), JSON.stringify(check.errors)).toBe(true);
}
const memory = (): StorageLike => {
  const data = new Map<string, string>();
  return {
    getItem: (key) => data.get(key) ?? null,
    setItem: (key, value) => {
      data.set(key, value);
    },
  };
};
function mockApi(): ControlApi {
  return {
    list: async () => ({
      items: [{ ...fixture }],
      as_of: new Date().toISOString(),
    }),
    get: async () => ({ ...fixture }),
    execute: async () => ({
      ...fixture,
      stream_state: "RUNNING",
      control_version: 3,
    }),
  };
}
afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

describe("Frozen Control API", () => {
  it("accepts the actual repository fixture and closed list schema", () => {
    expect(parseControl(fixture)).toEqual(fixture);
    const list = { items: [fixture], as_of: new Date().toISOString() };
    validate("ControlState", fixture);
    validate("ControlList", list);
    expect(parseList(list)).toEqual(list);
  });
  it.each([
    { ...fixture, rate: 5 },
    { ...fixture, source: "BROWSER" },
    { ...fixture, control_version: -1 },
    { ...fixture, stream_state: "OFFLINE" },
    { ...fixture, updated_at: "2026-10-20T12:00:00" },
  ])("rejects incompatible DTOs instead of inventing live state", (value) => {
    expect(() => parseControl(value)).toThrow(ApiError);
  });
  it("rejects duplicate machines", () => {
    expect(() =>
      parseList({ items: [fixture, fixture], as_of: new Date().toISOString() }),
    ).toThrow();
  });
  it("sends the exact method, body and stable idempotency identity", async () => {
    const request = vi.fn(
      async () => new Response(JSON.stringify(fixture), { status: 200 }),
    );
    vi.stubGlobal("fetch", request);
    const api = new HttpControlApi("", 1000, async () => "test-access-token");
    const command: Command = {
      id: "same-command-id",
      machineId: fixture.machine_id,
      action: "scenario",
      body: { expected_control_version: 2, scenario: "normal" },
      createdAt: new Date().toISOString(),
    };
    await api.execute(command);
    await api.execute(command);
    const call = request.mock.calls[0] as unknown as [string, RequestInit];
    expect(call[0]).toBe("/api/v1/simulator/machines/M-001/scenario");
    expect(call[1].method).toBe("PUT");
    expect(call[1].credentials).toBe("omit");
    expect(call[1].headers).toMatchObject({
      Authorization: "Bearer test-access-token",
      "Idempotency-Key": "same-command-id",
    });
    validate("ScenarioCommand", JSON.parse(call[1].body as string));
    const retry = request.mock.calls[1] as unknown as [string, RequestInit];
    expect(retry[0]).toBe(call[0]);
    expect(retry[1].headers).toEqual(call[1].headers);
    expect(retry[1].body).toEqual(call[1].body);
  });
  it("does not accept a successful HTTP response for the wrong machine", async () => {
    vi.stubGlobal(
      "fetch",
      async () =>
        new Response(JSON.stringify({ ...fixture, machine_id: "M-999" })),
    );
    await expect(
      new HttpControlApi("", 1000, async () => undefined).get("M-001"),
    ).rejects.toThrow("sai máy");
  });
});

describe("Command recovery and concurrency", () => {
  it("keeps timeout intent through reload, GET reconciliation and same-key retry", async () => {
    const storage = memory();
    const api = mockApi();
    const calls: Command[] = [];
    api.execute = async (command) => {
      calls.push(structuredClone(command));
      throw new ApiError(504, "TIMEOUT", "Timeout");
    };
    const first = new SimulatorController(api, storage, "commands", 20000);
    await first.refresh();
    await first.command("M-001", "resume");
    expect(first.getSnapshot().machines[0].stream_state).toBe("PAUSED");
    expect(first.getSnapshot().journal[0].status).toBe("unknown");
    await first.command("M-001", "scenario", "critical");
    expect(calls).toHaveLength(1);
    const reloaded = new SimulatorController(api, storage, "commands", 20000);
    await reloaded.refresh();
    await reloaded.reconcile("M-001");
    expect(reloaded.getSnapshot().journal[0].status).toBe("unknown");
    api.execute = async (command) => {
      calls.push(structuredClone(command));
      return { ...fixture, stream_state: "RUNNING", control_version: 3 };
    };
    await reloaded.retry(reloaded.getSnapshot().journal[0].id);
    expect(calls[1].id).toBe(calls[0].id);
    expect(calls[1].body).toEqual(calls[0].body);
    expect(reloaded.getSnapshot().journal[0].status).toBe("confirmed");
    expect(JSON.parse(storage.getItem("commands")!)).toEqual([]);
  });
  it("reconciles conflict without silently replaying a mutation", async () => {
    const api = mockApi();
    const execute = vi.fn(async () => {
      throw new ApiError(409, "CONTROL_VERSION_CONFLICT", "Conflict");
    });
    api.execute = execute;
    api.get = async () => ({ ...fixture, control_version: 8 });
    const controller = new SimulatorController(
      api,
      memory(),
      "commands",
      20000,
    );
    await controller.refresh();
    await controller.command("M-001", "resume");
    expect(execute).toHaveBeenCalledTimes(1);
    expect(controller.getSnapshot().journal[0].status).toBe("conflict");
    expect(controller.getSnapshot().machines[0].control_version).toBe(8);
  });
  it("never sends a command if its identity cannot be saved", async () => {
    const api = mockApi();
    const execute = vi.spyOn(api, "execute");
    const controller = new SimulatorController(
      api,
      {
        getItem: () => null,
        setItem: () => {
          throw new Error("Storage disabled");
        },
      },
      "commands",
      20000,
    );
    await controller.refresh();
    await controller.command("M-001", "resume");
    expect(execute).not.toHaveBeenCalled();
    expect(controller.getSnapshot().notice?.text).toContain("Chưa gửi lệnh");
  });
  it("does not roll back a newer mutation when an old poll completes later", async () => {
    const api = mockApi();
    const controller = new SimulatorController(
      api,
      memory(),
      "commands",
      20000,
    );
    await controller.refresh();
    let finish!: (value: Awaited<ReturnType<ControlApi["list"]>>) => void;
    api.list = () =>
      new Promise((resolve) => {
        finish = resolve;
      });
    const poll = controller.refresh();
    await controller.command("M-001", "resume");
    finish({ items: [fixture], as_of: new Date().toISOString() });
    await poll;
    expect(controller.getSnapshot().machines[0].control_version).toBe(3);
    expect(controller.getSnapshot().machines[0].stream_state).toBe("RUNNING");
  });
  it("stops new mutations after read authorization failure", async () => {
    const api = mockApi();
    const execute = vi.spyOn(api, "execute");
    const controller = new SimulatorController(
      api,
      memory(),
      "commands",
      20000,
    );
    await controller.refresh();
    api.list = async () => {
      throw new ApiError(403, "FORBIDDEN", "Forbidden");
    };
    await controller.refresh();
    await controller.command("M-001", "resume");
    expect(execute).not.toHaveBeenCalled();
  });
});

describe("Public runtime configuration", () => {
  const config = {
    mode: "live",
    apiBaseUrl: "",
    pollIntervalMs: 5000,
    requestTimeoutMs: 1000,
    staleAfterMs: 20000,
    auth: { mode: "none" },
    links: {},
  };
  it("permits explicit anonymous development only on loopback", () => {
    expect(
      parseConfig(structuredClone(config), "http://127.0.0.1:4174").mode,
    ).toBe("live");
    expect(() =>
      parseConfig(structuredClone(config), "https://simulator.example.com"),
    ).toThrow("localhost");
    expect(() =>
      parseConfig(
        { ...config, apiBaseUrl: "https://sensor.example.com" },
        "http://localhost:4174",
      ),
    ).toThrow("localhost");
  });
  it.each([
    "javascript:alert(1)",
    "https://user:secret@example.com",
    "https://example.com?token=secret",
  ])("rejects unsafe links: %s", (url) => {
    expect(() =>
      parseConfig(
        { ...config, mode: "demo", links: { business: url } },
        "http://localhost:4174",
      ),
    ).toThrow();
  });
});
