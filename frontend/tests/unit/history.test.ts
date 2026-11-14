import { readFileSync } from "node:fs";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vitest";
import { parseHistory } from "../../src/api/history-codec";
import { NumericToken } from "../../src/api/codec";
import { BusinessApi } from "../../src/api/client";
import { ExportUnknown } from "../../src/api/history-client";
import {
  buildFeatureTrendSeries,
  FeatureTrendChart,
} from "../../src/features/machines/detail/HistoryPanel";

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

it("renders FeatureTrendChart safely for 0 points, 1 point and hybrid raw + live SSE stream without NaN", () => {
  const emptyHtml = renderToStaticMarkup(
    createElement(FeatureTrendChart, {
      series: [],
      mode: "ALL",
      onSelectMode: () => {},
    }),
  );
  expect(emptyHtml).toContain(
    "Chưa có mẫu dữ liệu cảm biến hợp lệ để vẽ xu hướng.",
  );
  expect(emptyHtml).not.toContain("NaN");

  const rawText = readFileSync(
    new URL(
      "../../../contracts/v1.3/examples/m6/raw-page-offset-coverage.json",
      import.meta.url,
    ),
    "utf8",
  );
  const rawPage = parseHistory(rawText, "RAW");
  const singleSeries = buildFeatureTrendSeries([rawPage.items[0]], []);
  expect(singleSeries).toHaveLength(1);
  const singleHtml = renderToStaticMarkup(
    createElement(FeatureTrendChart, {
      series: singleSeries,
      mode: "torque_nm",
      onSelectMode: () => {},
      aiLastSeenAt: singleSeries[0].timestamp,
      aiDegraded: true,
    }),
  );
  expect(singleHtml).not.toContain("NaN");
  expect(singleHtml).not.toContain("Infinity");
  expect(singleHtml).toContain("Chế độ chẩn đoán vật lý trực tiếp");

  const hybridSeries = buildFeatureTrendSeries(rawPage.items, [
    {
      event_id: "live-sse-1",
      stream_epoch: "epoch-1",
      sequence_number: 99n,
      received_at: "2026-10-18T00:05:00Z",
      server_accepted_at: "2026-10-18T00:05:00Z",
      validation_status: "VALID",
      features_raw: {
        air_temperature_k: 299.5,
        process_temperature_k: 310.2,
        rotational_speed_rpm: 1320,
        torque_nm: 63.4,
        tool_wear_min: 212,
      },
    },
  ]);
  expect(hybridSeries.length).toBeGreaterThanOrEqual(2);
  expect(hybridSeries.at(-1)?.isLive).toBe(true);
  const stackedHtml = renderToStaticMarkup(
    createElement(FeatureTrendChart, {
      series: hybridSeries,
      mode: "ALL",
      onSelectMode: () => {},
      aiLastSeenAt: "2026-10-18T00:01:00Z",
      aiDegraded: true,
    }),
  );
  expect(stackedHtml).not.toContain("NaN");
  expect(stackedHtml).toContain("AI Last Seen");
  expect(stackedHtml).toContain("pulsing-dot");
  expect(stackedHtml).toContain("63.4 Nm");
});
