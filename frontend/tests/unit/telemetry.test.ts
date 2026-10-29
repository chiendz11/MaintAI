import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vitest";
import { TelemetryCard } from "../../src/features/machines/live_telemetry/TelemetryCard";
import type { Telemetry } from "../../src/types/live";

it("labels old telemetry as Last Telemetry with a stale indicator", () => {
  const value: Telemetry = {
    event_id: "e-1",
    stream_epoch: "epoch",
    sequence_number: 1n,
    server_accepted_at: "2026-10-28T00:00:00Z",
    received_at: "2026-10-28T00:00:00Z",
    validation_status: "VALID",
    features_raw: {},
  };
  const html = renderToStaticMarkup(
    createElement(TelemetryCard, {
      value,
      now: Date.parse("2026-10-28T00:00:06Z"),
      freshMs: 2000,
      staleMs: 5000,
    }),
  );
  expect(html).toContain("Last Telemetry");
  expect(html).toContain("STALE");
});
