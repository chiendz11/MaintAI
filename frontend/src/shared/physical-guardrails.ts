import { NumericToken } from "../api/codec";

export type PhysicalStatus = "NORMAL" | "WARNING" | "CRITICAL";

export type TelemetryFeatures = {
  product_type?: string;
  air_temperature_k?: number;
  process_temperature_k?: number;
  rotational_speed_rpm?: number;
  torque_nm?: number;
  tool_wear_min?: number;
};

export type MetricKey =
  | "air_temperature_k"
  | "process_temperature_k"
  | "rotational_speed_rpm"
  | "torque_nm"
  | "tool_wear_min"
  | "temp_delta_k"
  | "mechanical_power_w";

export type TrendFeatureKey =
  | "ALL"
  | "torque_nm"
  | "rotational_speed_rpm"
  | "temperatures"
  | "tool_wear_min";

export type FeatureMeta = {
  key: MetricKey;
  label: string;
  shortLabel: string;
  unit: string;
  min: number;
  max: number;
  warningLow?: number;
  warningHigh?: number;
  criticalLow?: number;
  criticalHigh?: number;
  trendKey: TrendFeatureKey;
};

export const FEATURE_METADATA: Record<MetricKey, FeatureMeta> = {
  air_temperature_k: {
    key: "air_temperature_k",
    label: "Nhiệt độ không khí",
    shortLabel: "Air Temp",
    unit: "K",
    min: 295,
    max: 306,
    warningHigh: 303,
    criticalHigh: 304.5,
    trendKey: "temperatures",
  },
  process_temperature_k: {
    key: "process_temperature_k",
    label: "Nhiệt độ quá trình",
    shortLabel: "Process Temp",
    unit: "K",
    min: 305,
    max: 316,
    warningHigh: 312,
    criticalHigh: 313,
    trendKey: "temperatures",
  },
  rotational_speed_rpm: {
    key: "rotational_speed_rpm",
    label: "Tốc độ quay",
    shortLabel: "Speed",
    unit: "rpm",
    min: 1100,
    max: 2800,
    warningLow: 1350,
    warningHigh: 2200,
    criticalLow: 1250,
    criticalHigh: 2500,
    trendKey: "rotational_speed_rpm",
  },
  torque_nm: {
    key: "torque_nm",
    label: "Mô-men xoắn",
    shortLabel: "Torque",
    unit: "Nm",
    min: 0,
    max: 80,
    warningHigh: 50,
    criticalHigh: 60,
    trendKey: "torque_nm",
  },
  tool_wear_min: {
    key: "tool_wear_min",
    label: "Độ mòn dụng cụ",
    shortLabel: "Tool Wear",
    unit: "phút",
    min: 0,
    max: 250,
    warningHigh: 180,
    criticalHigh: 210,
    trendKey: "tool_wear_min",
  },
  temp_delta_k: {
    key: "temp_delta_k",
    label: "Độ chênh nhiệt (ΔT)",
    shortLabel: "Temp Delta",
    unit: "K",
    min: 5,
    max: 15,
    warningLow: 8.6,
    criticalLow: 8.0,
    trendKey: "temperatures",
  },
  mechanical_power_w: {
    key: "mechanical_power_w",
    label: "Công suất cơ học (P)",
    shortLabel: "Est. Power",
    unit: "W",
    min: 2000,
    max: 10500,
    warningLow: 3500,
    warningHigh: 8500,
    criticalLow: 3000,
    criticalHigh: 9000,
    trendKey: "torque_nm",
  },
};

export type MetricEvaluation = {
  value: number;
  status: PhysicalStatus;
  percent: number;
};

export type PhysicalEvaluation = {
  status: PhysicalStatus;
  triggeredReasons: string[];
  primaryBadge: string;
  metrics: Partial<Record<MetricKey, MetricEvaluation>>;
  derived: {
    temp_delta_k: number | null;
    mechanical_power_w: number | null;
  };
};

export function readNumericFeature(value: unknown): number | undefined {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : undefined;
  }
  if (value instanceof NumericToken) {
    const parsed = Number(value.source);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  if (
    value !== null &&
    typeof value === "object" &&
    "source" in value &&
    typeof (value as { source: unknown }).source === "string"
  ) {
    const parsed = Number((value as { source: string }).source);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

export function extractTelemetryFeatures(
  raw: Record<string, unknown> | TelemetryFeatures | null | undefined,
): TelemetryFeatures {
  if (!raw || typeof raw !== "object") return {};
  const record = raw as Record<string, unknown>;
  return {
    product_type:
      typeof record.product_type === "string" ? record.product_type : undefined,
    air_temperature_k: readNumericFeature(record.air_temperature_k),
    process_temperature_k: readNumericFeature(record.process_temperature_k),
    rotational_speed_rpm: readNumericFeature(record.rotational_speed_rpm),
    torque_nm: readNumericFeature(record.torque_nm),
    tool_wear_min: readNumericFeature(record.tool_wear_min),
  };
}

export function computeDerivedFeatures(
  raw: Record<string, unknown> | TelemetryFeatures | null | undefined,
): { temp_delta_k: number | null; mechanical_power_w: number | null } {
  const features = extractTelemetryFeatures(raw);
  const temp_delta_k =
    features.process_temperature_k !== undefined &&
    features.air_temperature_k !== undefined
      ? Number(
          (features.process_temperature_k - features.air_temperature_k).toFixed(
            2,
          ),
        )
      : null;
  const mechanical_power_w =
    features.torque_nm !== undefined &&
    features.rotational_speed_rpm !== undefined
      ? Number(
          (
            (features.torque_nm * features.rotational_speed_rpm * 2 * Math.PI) /
            60
          ).toFixed(1),
        )
      : null;
  return { temp_delta_k, mechanical_power_w };
}

function clampPercent(value: number, min: number, max: number): number {
  if (!Number.isFinite(value) || max <= min) return 0;
  return Math.max(
    4,
    Math.min(100, Math.round(((value - min) / (max - min)) * 100)),
  );
}

function escalate(
  current: PhysicalStatus,
  next: PhysicalStatus,
): PhysicalStatus {
  const rank: Record<PhysicalStatus, number> = {
    NORMAL: 0,
    WARNING: 1,
    CRITICAL: 2,
  };
  return rank[next] > rank[current] ? next : current;
}

export function evaluatePhysicalHealth(
  raw: Record<string, unknown> | TelemetryFeatures | null | undefined,
): PhysicalEvaluation {
  const features = extractTelemetryFeatures(raw);
  const derived = computeDerivedFeatures(features);
  const triggeredReasons: string[] = [];
  const badges: { status: PhysicalStatus; text: string }[] = [];
  const metrics: Partial<Record<MetricKey, MetricEvaluation>> = {};
  let overall: PhysicalStatus = "NORMAL";

  // 1. Torque (Nm): Normal < 50, Warning 50-60, Critical > 60
  if (features.torque_nm !== undefined) {
    const v = features.torque_nm;
    let status: PhysicalStatus = "NORMAL";
    if (v > 60) {
      status = "CRITICAL";
      triggeredReasons.push(`Torque > 60 Nm (${v.toFixed(1)} Nm)`);
      badges.push({
        status,
        text: `🚨 Torque High (${v.toFixed(1)} Nm)`,
      });
    } else if (v >= 50) {
      status = "WARNING";
      triggeredReasons.push(`Torque 50–60 Nm (${v.toFixed(1)} Nm)`);
      badges.push({
        status,
        text: `⚠️ Torque Warn (${v.toFixed(1)} Nm)`,
      });
    }
    overall = escalate(overall, status);
    metrics.torque_nm = {
      value: v,
      status,
      percent: clampPercent(
        v,
        FEATURE_METADATA.torque_nm.min,
        FEATURE_METADATA.torque_nm.max,
      ),
    };
  }

  // 2. Tool Wear (min): Normal < 180, Warning 180-210, Critical > 210
  if (features.tool_wear_min !== undefined) {
    const v = features.tool_wear_min;
    let status: PhysicalStatus = "NORMAL";
    if (v > 210) {
      status = "CRITICAL";
      triggeredReasons.push(`Tool wear > 210 min (${Math.round(v)}m)`);
      badges.push({
        status,
        text: `🚨 Wear Critical (${Math.round(v)}m)`,
      });
    } else if (v >= 180) {
      status = "WARNING";
      triggeredReasons.push(`Tool wear 180–210 min (${Math.round(v)}m)`);
      badges.push({
        status,
        text: `⚠️ Wear High (${Math.round(v)}m)`,
      });
    }
    overall = escalate(overall, status);
    metrics.tool_wear_min = {
      value: v,
      status,
      percent: clampPercent(
        v,
        FEATURE_METADATA.tool_wear_min.min,
        FEATURE_METADATA.tool_wear_min.max,
      ),
    };
  }

  // 3. Temperatures & Temp Delta (ΔT)
  if (features.air_temperature_k !== undefined) {
    const v = features.air_temperature_k;
    const status: PhysicalStatus =
      v > 304.5 ? "CRITICAL" : v > 303 ? "WARNING" : "NORMAL";
    metrics.air_temperature_k = {
      value: v,
      status,
      percent: clampPercent(
        v,
        FEATURE_METADATA.air_temperature_k.min,
        FEATURE_METADATA.air_temperature_k.max,
      ),
    };
  }
  if (features.process_temperature_k !== undefined) {
    const v = features.process_temperature_k;
    let status: PhysicalStatus = "NORMAL";
    if (v > 313) {
      status = "CRITICAL";
      triggeredReasons.push(`Process temp > 313 K (${v.toFixed(1)} K)`);
      badges.push({
        status,
        text: `🚨 Temp High (${v.toFixed(1)} K)`,
      });
    } else if (v > 312) {
      status = "WARNING";
    }
    overall = escalate(overall, status);
    metrics.process_temperature_k = {
      value: v,
      status,
      percent: clampPercent(
        v,
        FEATURE_METADATA.process_temperature_k.min,
        FEATURE_METADATA.process_temperature_k.max,
      ),
    };
  }
  if (derived.temp_delta_k !== null) {
    const v = derived.temp_delta_k;
    let status: PhysicalStatus = "NORMAL";
    if (
      v < 8.0 ||
      (features.process_temperature_k !== undefined &&
        features.process_temperature_k > 313)
    ) {
      status = "CRITICAL";
      if (v < 8.0) {
        triggeredReasons.push(`Temp delta < 8.0 K (${v.toFixed(1)} K)`);
        badges.push({
          status,
          text: `🚨 ΔT Low (${v.toFixed(1)} K)`,
        });
      }
    } else if (v < 8.6) {
      status = "WARNING";
      triggeredReasons.push(`Temp delta < 8.6 K (${v.toFixed(1)} K)`);
      badges.push({
        status,
        text: `⚠️ ΔT Warn (${v.toFixed(1)} K)`,
      });
    }
    overall = escalate(overall, status);
    metrics.temp_delta_k = {
      value: v,
      status,
      percent: clampPercent(
        v,
        FEATURE_METADATA.temp_delta_k.min,
        FEATURE_METADATA.temp_delta_k.max,
      ),
    };
  }

  // 4. Rotational Speed (RPM): Normal 1350-2200, Warning <1350 or >2200, Critical <1250 or >2500
  if (features.rotational_speed_rpm !== undefined) {
    const v = features.rotational_speed_rpm;
    let status: PhysicalStatus = "NORMAL";
    if (v < 1250 || v > 2500) {
      status = "CRITICAL";
      triggeredReasons.push(
        v < 1250
          ? `Speed < 1250 RPM (${Math.round(v)} RPM)`
          : `Speed > 2500 RPM (${Math.round(v)} RPM)`,
      );
      badges.push({
        status,
        text: `🚨 Speed ${v < 1250 ? "Low" : "High"} (${Math.round(v)} RPM)`,
      });
    } else if (v < 1350 || v > 2200) {
      status = "WARNING";
      triggeredReasons.push(
        v < 1350
          ? `Speed < 1350 RPM (${Math.round(v)} RPM)`
          : `Speed > 2200 RPM (${Math.round(v)} RPM)`,
      );
      badges.push({
        status,
        text: `⚠️ Speed ${v < 1350 ? "Low" : "High"} (${Math.round(v)} RPM)`,
      });
    }
    overall = escalate(overall, status);
    metrics.rotational_speed_rpm = {
      value: v,
      status,
      percent: clampPercent(
        v,
        FEATURE_METADATA.rotational_speed_rpm.min,
        FEATURE_METADATA.rotational_speed_rpm.max,
      ),
    };
  }

  // 5. Mechanical Power (P): Normal 3500-8500 W, Warning <3500 or >8500 W, Critical <3000 or >9000 W
  if (derived.mechanical_power_w !== null) {
    const v = derived.mechanical_power_w;
    let status: PhysicalStatus = "NORMAL";
    if (v < 3000 || v > 9000) {
      status = "CRITICAL";
      triggeredReasons.push(
        v < 3000
          ? `Power < 3000 W (${Math.round(v)} W)`
          : `Power > 9000 W (${Math.round(v)} W)`,
      );
      badges.push({
        status,
        text: `🚨 Power ${v < 3000 ? "Low" : "High"} (${Math.round(v)} W)`,
      });
    } else if (v < 3500 || v > 8500) {
      status = "WARNING";
      triggeredReasons.push(
        v < 3500
          ? `Power < 3500 W (${Math.round(v)} W)`
          : `Power > 8500 W (${Math.round(v)} W)`,
      );
      badges.push({
        status,
        text: `⚠️ Power ${v < 3500 ? "Low" : "High"} (${Math.round(v)} W)`,
      });
    }
    overall = escalate(overall, status);
    metrics.mechanical_power_w = {
      value: v,
      status,
      percent: clampPercent(
        v,
        FEATURE_METADATA.mechanical_power_w.min,
        FEATURE_METADATA.mechanical_power_w.max,
      ),
    };
  }

  const criticalBadge = badges.find((b) => b.status === "CRITICAL");
  const warningBadge = badges.find((b) => b.status === "WARNING");
  const primaryBadge =
    criticalBadge?.text ?? warningBadge?.text ?? "Physical OK";

  return {
    status: overall,
    triggeredReasons,
    primaryBadge,
    metrics,
    derived,
  };
}
