import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vitest";
import { NumericToken } from "../../src/api/codec";
import { TelemetryCard } from "../../src/features/machines/live_telemetry/TelemetryCard";
import {
  computeDerivedFeatures,
  evaluatePhysicalHealth,
} from "../../src/shared/physical-guardrails";
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

it("evaluates NORMAL, WARNING and CRITICAL physical guardrails and derived metrics", () => {
  const normal = evaluatePhysicalHealth({
    air_temperature_k: new NumericToken("298.1"),
    process_temperature_k: new NumericToken("308.6"),
    rotational_speed_rpm: new NumericToken("1500"),
    torque_nm: new NumericToken("42.8"),
    tool_wear_min: new NumericToken("108"),
  });
  expect(normal.status).toBe("NORMAL");
  expect(normal.triggeredReasons).toEqual([]);
  expect(normal.primaryBadge).toBe("Physical OK");
  expect(normal.derived.temp_delta_k).toBe(10.5);
  expect(normal.derived.mechanical_power_w).toBeCloseTo(6723.0, 0);

  const warning = evaluatePhysicalHealth({
    air_temperature_k: 298.0,
    process_temperature_k: 306.4,
    rotational_speed_rpm: 1340,
    torque_nm: 54.0,
    tool_wear_min: 195,
  });
  expect(warning.status).toBe("WARNING");
  expect(warning.triggeredReasons).toEqual(
    expect.arrayContaining([
      expect.stringContaining("Torque 50–60 Nm"),
      expect.stringContaining("Tool wear 180–210 min"),
      expect.stringContaining("Temp delta < 8.6 K"),
      expect.stringContaining("Speed < 1350 RPM"),
    ]),
  );

  const critical = evaluatePhysicalHealth({
    air_temperature_k: 300.0,
    process_temperature_k: 307.5,
    rotational_speed_rpm: 1200,
    torque_nm: 64.2,
    tool_wear_min: 215,
  });
  expect(critical.status).toBe("CRITICAL");
  expect(critical.primaryBadge).toContain("🚨 Torque High (64.2 Nm)");
  expect(critical.triggeredReasons).toEqual(
    expect.arrayContaining([
      expect.stringContaining("Torque > 60 Nm"),
      expect.stringContaining("Tool wear > 210 min"),
      expect.stringContaining("Temp delta < 8.0 K"),
      expect.stringContaining("Speed < 1250 RPM"),
    ]),
  );
  expect(computeDerivedFeatures(null)).toEqual({
    temp_delta_k: null,
    mechanical_power_w: null,
  });
});

it("renders threshold bars, derived metrics and view-trend buttons in TelemetryCard", () => {
  const value: Telemetry = {
    event_id: "e-crit",
    stream_epoch: "epoch",
    sequence_number: 2n,
    server_accepted_at: "2026-10-28T00:00:00Z",
    received_at: "2026-10-28T00:00:00Z",
    validation_status: "VALID",
    features_raw: {
      product_type: "M",
      air_temperature_k: 299.0,
      process_temperature_k: 309.5,
      rotational_speed_rpm: 1450,
      torque_nm: 62.5,
      tool_wear_min: 190,
    },
  };
  const html = renderToStaticMarkup(
    createElement(TelemetryCard, {
      value,
      now: Date.parse("2026-10-28T00:00:01Z"),
      freshMs: 2000,
      staleMs: 5000,
      onSelectFeature: () => {},
    }),
  );
  expect(html).toContain("Temp Delta (ΔT)");
  expect(html).toContain("Est. Power (P)");
  expect(html).toContain("📈 View Trend");
  expect(html).toContain("🚨 Torque High (62.5 Nm)");
  expect(html).not.toContain("NaN");
});
