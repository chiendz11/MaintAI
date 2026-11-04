import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { parseHistory } from "../../src/api/history-codec";
import { NumericToken } from "../../src/api/codec";
import { BusinessApi } from "../../src/api/client";
import { ExportUnknown } from "../../src/api/history-client";

it("keeps history source sequence, offsets and raw numeric lexemes exact", () => {
  const text = readFileSync(
    new URL(
      "../../../contracts/v1.3/examples/m6/processing-page.json",
      import.meta.url,
    ),
    "utf8",
  ).replace(
    /"sequence_number": 1,/,
    '"sequence_number": 1208925819614629174706176,',
  );
  const page = parseHistory(text, "PROCESSING");
  const item = page.items[0];
  if (!("outcome" in item)) throw new Error("Expected Processing record");
  expect(item.outcome.sequence_number).toBe(1208925819614629174706176n);
  expect(page.items[0].features_raw.air_temperature_k).toBeInstanceOf(
    NumericToken,
  );
  expect(page.time_basis).toBe("processed_at");
});

it("rejects a legacy page presented as M6 history", () => {
  const text = readFileSync(
    new URL(
      "../../../contracts/v1.3/examples/history-page.json",
      import.meta.url,
    ),
    "utf8",
  );
  expect(() => parseHistory(text, "PROCESSING")).toThrow();
});

it("sends the profile and idempotency key once and preserves unknown export intent", async () => {
  let calls = 0;
  const api = new BusinessApi(
    "/api/v1",
    async () => "browser-token",
    async (_url, init) => {
      calls++;
      const headers = new Headers(init?.headers);
      expect(headers.get("X-MaintAI-History-Profile")).toBe(
        "M6-HISTORY-01/1.0.0",
      );
      expect(headers.get("Idempotency-Key")).toBe("intent-one");
      expect(headers.has("X-MaintAI-History-Delegation")).toBe(false);
      throw new TypeError("response lost");
    },
  );
  await expect(
    api.history.export(
      {
        profile: "M6-HISTORY-01/1.0.0",
        machine_ids: ["M-001"],
        from: "2026-10-29T00:00:00Z",
        to: "2026-10-29T01:00:00Z",
        resource: "PROCESSING",
        format: "CSV",
      },
      "intent-one",
      new AbortController().signal,
    ),
  ).rejects.toBeInstanceOf(ExportUnknown);
  expect(calls).toBe(1);
});
