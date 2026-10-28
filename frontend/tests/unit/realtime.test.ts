import { describe, expect, it } from "vitest";
import { parseLiveSnapshot, decodeSSE, rawText } from "../../src/api/codec";
import { mergeLive, emptyLive } from "../../src/realtime/reducers";
import { freshness } from "../../src/realtime/freshness";

function snapshot(
  sequence = "1208925819614629174706177",
  validation = "UNVERIFIED",
) {
  return `{"machine_id":"M-001","telemetry":{"event_id":"e-${sequence}","stream_epoch":"z","sequence_number":${sequence},"server_accepted_at":"2026-10-28T00:00:00Z","received_at":"2026-10-28T00:00:00Z","validation_status":"${validation}","features_raw":{"sequence_number":9007199254740993,"raw":"bad"}},"latest_valid_prediction":null,"latest_processing":null,"as_of":"2026-10-28T00:00:00Z"}`;
}

describe("source ordering and precision", () => {
  it("recovers ambiguous nonterminal progress from REST without ranking retry states", () => {
    const current = parseLiveSnapshot(snapshot("2"));
    const pending = {
      source_event_id: "e-2",
      stream_epoch: "z",
      sequence_number: 2n,
      processing_status: "PENDING" as const,
      validation_status: null,
      updated_at: "2026-10-28T00:00:00Z",
      failure_code: null,
    };
    current.latest_processing = pending;
    const processing = { ...pending, processing_status: "PROCESSING" as const };
    const event = {
      schema_version: "1.0.0" as const,
      type: "processing.status_changed" as const,
      machine_id: "M-001",
      occurred_at: pending.updated_at,
      data: processing,
    };
    expect(() => mergeLive(current, event)).toThrow(/REST resync/);
    const synced = mergeLive(current, {
      ...current,
      latest_processing: processing,
    });
    expect(synced.latest_processing?.processing_status).toBe("PROCESSING");
    const retried = mergeLive(synced, {
      ...synced,
      latest_processing: { ...pending, processing_status: "RETRYING" },
    });
    expect(retried.latest_processing?.processing_status).toBe("RETRYING");
    const completed = mergeLive(retried, {
      ...retried,
      latest_processing: {
        ...pending,
        processing_status: "COMPLETED",
        validation_status: "VALID",
      },
    });
    expect(
      mergeLive(completed, current).latest_processing?.processing_status,
    ).toBe("COMPLETED");
  });
  it("preserves historical prediction on INVALID and does not regress terminal status", () => {
    const current = parseLiveSnapshot(snapshot("2"));
    current.latest_valid_prediction = {
      source_event_id: "e-1",
      stream_epoch: "old",
      sequence_number: 1n,
      source_server_accepted_at: "2026-10-28T00:00:00Z",
      prediction_id: "p-1",
      prediction: "NORMAL",
      failure_probability: 0.12,
      machine_state_at_prediction: "NORMAL",
      model_version: "m",
      preprocessor_version: "p",
      dataset_version: "d",
      policy_version: "r",
      completed_at: "2026-10-28T00:00:00Z",
    };
    const failed = {
      source_event_id: "e-2",
      stream_epoch: "z",
      sequence_number: 2n,
      processing_status: "FAILED" as const,
      validation_status: "VALID" as const,
      updated_at: "2026-10-28T00:00:00Z",
      failure_code: "ML_UNAVAILABLE",
    };
    current.latest_processing = failed;
    const next = mergeLive(current, {
      ...parseLiveSnapshot(snapshot("3", "INVALID")),
      latest_processing: {
        ...failed,
        source_event_id: "e-3",
        sequence_number: 3n,
        processing_status: "INVALID",
        validation_status: "INVALID",
        failure_code: null,
      },
    });
    expect(next.latest_valid_prediction?.source_event_id).toBe("e-1");
    const pending = mergeLive(next, {
      ...emptyLive("M-001"),
      latest_processing: {
        ...next.latest_processing!,
        processing_status: "PENDING",
        validation_status: null,
      },
    });
    expect(pending.latest_processing?.processing_status).toBe("INVALID");
    expect(() =>
      mergeLive(current, {
        ...emptyLive("M-001"),
        latest_processing: { ...failed, failure_code: "ML_TIMEOUT" },
      }),
    ).toThrow(/conflict/i);
  });
  it("treats object key order as irrelevant to same-sequence identity", () => {
    const current = parseLiveSnapshot(snapshot("12"));
    current.telemetry!.features_raw = { a: "first", b: "second" };
    const incoming = parseLiveSnapshot(snapshot("12", "VALID"));
    incoming.telemetry!.features_raw = { b: "second", a: "first" };
    expect(mergeLive(current, incoming).telemetry?.validation_status).toBe(
      "VALID",
    );
  });
  it("keeps sequences and raw numeric evidence exact beyond Number precision", () => {
    const value = parseLiveSnapshot(snapshot());
    expect(value.telemetry?.sequence_number).toBe(1208925819614629174706177n);
    expect(rawText(value.telemetry?.features_raw)).toContain(
      "9007199254740993",
    );
  });
  it.each(["9007199254740991", "9007199254740993", "1.00e3"])(
    "parses integer lexeme %s",
    (value) => {
      const got = parseLiveSnapshot(snapshot(value));
      expect(got.telemetry?.sequence_number).toBe(
        value === "1.00e3" ? 1000n : BigInt(value),
      );
    },
  );
  it("rejects non-positive/fractional cursors, bad DTO and oversized body", () => {
    for (const value of ["0", "-1", "1.5"])
      expect(() => parseLiveSnapshot(snapshot(value))).toThrow();
    expect(() => parseLiveSnapshot("{}")).toThrow();
    expect(() => parseLiveSnapshot(" ".repeat(2097153))).toThrow();
  });
  it("cannot downgrade newer telemetry with an older snapshot", () => {
    const newest = mergeLive(
      emptyLive("M-001"),
      parseLiveSnapshot(snapshot("12")),
    );
    const older = mergeLive(newest, parseLiveSnapshot(snapshot("11")));
    expect(older.telemetry?.sequence_number).toBe(12n);
    expect(older.telemetry?.event_id).toBe("e-12");
  });
  it("allows same-sequence validation advancement and rejects identity conflict", () => {
    const current = mergeLive(
      emptyLive("M-001"),
      parseLiveSnapshot(snapshot("12")),
    );
    const validated = mergeLive(
      current,
      parseLiveSnapshot(snapshot("12", "INVALID")),
    );
    expect(validated.telemetry?.validation_status).toBe("INVALID");
    expect(() =>
      mergeLive(validated, {
        ...parseLiveSnapshot(snapshot("12")),
        telemetry: {
          ...validated.telemetry!,
          event_id: "different",
        },
      }),
    ).toThrow(/conflict/i);
    expect(
      mergeLive(validated, parseLiveSnapshot(snapshot("12"))).telemetry
        ?.validation_status,
    ).toBe("INVALID");
  });
  it("does not let another machine overwrite the current session", () => {
    expect(() =>
      mergeLive(emptyLive("other"), parseLiveSnapshot(snapshot())),
    ).toThrow();
  });
});

describe("SSE transport", () => {
  it("handles UTF-8 chunks, CRLF, comments and multiline data", async () => {
    const value = parseLiveSnapshot(snapshot("1"));
    const body = JSON.stringify({
      schema_version: "1.0.0",
      type: "telemetry.received",
      machine_id: "M-001",
      data: {
        ...value.telemetry,
        sequence_number: 1,
        features_raw: { raw: "機" },
      },
      occurred_at: "2026-10-28T00:00:00Z",
    });
    const raw = new TextEncoder().encode(
      `: heartbeat\r\n\r\nevent: telemetry.received\r\nid: hint\r\ndata: ${body.slice(0, body.indexOf('"data"'))}\r\ndata: ${body.slice(body.indexOf('"data"'))}\r\n\r\n`,
    );
    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        for (let i = 0; i < raw.length; i++)
          controller.enqueue(raw.slice(i, i + 1));
        controller.close();
      },
    });
    const events = [];
    for await (const event of decodeSSE(stream)) events.push(event);
    expect(events).toHaveLength(1);
    expect(events[0].data.sequence_number).toBe(1n);
    expect(
      rawText(
        "features_raw" in events[0].data ? events[0].data.features_raw : null,
      ),
    ).toContain("機");
  });
  it("does not turn an incomplete EOF frame into a complete event", async () => {
    const stream = new ReadableStream<Uint8Array>({
      start(c) {
        c.enqueue(
          new TextEncoder().encode(
            'event: telemetry.received\ndata: {"broken":',
          ),
        );
        c.close();
      },
    });
    const got = [];
    for await (const event of decodeSSE(stream)) got.push(event);
    expect(got).toEqual([]);
  });
});

describe("freshness", () => {
  it("uses source age, not model completion time", () => {
    expect(
      freshness(
        "2026-10-28T00:00:00Z",
        Date.parse("2026-10-28T00:00:06Z"),
        2000,
        5000,
      ),
    ).toBe("STALE");
    expect(freshness(null, Date.now(), 2000, 5000)).toBe("NOT_AVAILABLE");
  });
});
