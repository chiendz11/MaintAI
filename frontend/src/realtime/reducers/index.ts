import { NumericToken } from "../../api/codec";
import {
  evaluatePhysicalHealth,
  extractTelemetryFeatures,
  type PhysicalEvaluation,
  type TelemetryFeatures,
} from "../../shared/physical-guardrails";
import type {
  Cursor,
  LiveEvent,
  LiveSnapshot,
  Prediction,
  Processing,
  ProcessingStatus,
  Telemetry,
} from "../../types/live";

export type FleetMachineHealth = {
  machine_id: string;
  latest_features: TelemetryFeatures;
  physicalHealth: PhysicalEvaluation;
  telemetry_accepted_at: string | null;
  prediction_accepted_at: string | null;
  processing_status: ProcessingStatus | null;
  sequence_number: bigint;
};

const fleetHealthStore = new Map<string, FleetMachineHealth>();
const fleetListeners = new Set<() => void>();

function notifyFleetListeners() {
  for (const listener of fleetListeners) listener();
}

export function subscribeFleetHealth(listener: () => void): () => void {
  fleetListeners.add(listener);
  return () => {
    fleetListeners.delete(listener);
  };
}

export function getFleetMachineHealth(
  machine_id: string,
): FleetMachineHealth | undefined {
  return fleetHealthStore.get(machine_id);
}

export function getAllFleetMachineHealth(): ReadonlyMap<
  string,
  FleetMachineHealth
> {
  return fleetHealthStore;
}

export function clearFleetHealthStore(): void {
  fleetHealthStore.clear();
  notifyFleetListeners();
}

export function updateFleetSnapshot(snapshot: LiveSnapshot): void {
  const existing = fleetHealthStore.get(snapshot.machine_id);
  const telemetry = snapshot.telemetry;
  if (
    telemetry &&
    (!existing || telemetry.sequence_number >= existing.sequence_number)
  ) {
    fleetHealthStore.set(snapshot.machine_id, {
      machine_id: snapshot.machine_id,
      latest_features: extractTelemetryFeatures(telemetry.features_raw),
      physicalHealth: evaluatePhysicalHealth(telemetry.features_raw),
      telemetry_accepted_at: telemetry.server_accepted_at,
      prediction_accepted_at:
        snapshot.latest_valid_prediction?.source_server_accepted_at ??
        existing?.prediction_accepted_at ??
        null,
      processing_status:
        snapshot.latest_processing?.processing_status ??
        existing?.processing_status ??
        null,
      sequence_number: telemetry.sequence_number,
    });
    notifyFleetListeners();
  } else if (
    existing &&
    (snapshot.latest_processing || snapshot.latest_valid_prediction)
  ) {
    fleetHealthStore.set(snapshot.machine_id, {
      ...existing,
      prediction_accepted_at:
        snapshot.latest_valid_prediction?.source_server_accepted_at ??
        existing.prediction_accepted_at,
      processing_status:
        snapshot.latest_processing?.processing_status ??
        existing.processing_status,
    });
    notifyFleetListeners();
  }
}

export function applyFleetEvent(event: LiveEvent): void {
  const existing = fleetHealthStore.get(event.machine_id);
  if (event.type === "telemetry.received") {
    const telemetry = event.data as Telemetry;
    if (existing && telemetry.sequence_number < existing.sequence_number)
      return;
    fleetHealthStore.set(event.machine_id, {
      machine_id: event.machine_id,
      latest_features: extractTelemetryFeatures(telemetry.features_raw),
      physicalHealth: evaluatePhysicalHealth(telemetry.features_raw),
      telemetry_accepted_at: telemetry.server_accepted_at,
      prediction_accepted_at: existing?.prediction_accepted_at ?? null,
      processing_status: existing?.processing_status ?? null,
      sequence_number: telemetry.sequence_number,
    });
    notifyFleetListeners();
  } else if (event.type === "processing.status_changed" && existing) {
    const processing = event.data as Processing;
    fleetHealthStore.set(event.machine_id, {
      ...existing,
      processing_status: processing.processing_status,
    });
    notifyFleetListeners();
  } else if (event.type === "prediction.completed" && existing) {
    const prediction = event.data as Prediction;
    fleetHealthStore.set(event.machine_id, {
      ...existing,
      prediction_accepted_at: prediction.source_server_accepted_at,
    });
    notifyFleetListeners();
  }
}

export function emptyLive(machine: string): LiveSnapshot {
  return {
    machine_id: machine,
    telemetry: null,
    latest_valid_prediction: null,
    latest_processing: null,
    as_of: "",
  };
}
function source(
  value: Cursor & { event_id?: string; source_event_id?: string },
): string | undefined {
  return value.event_id ?? value.source_event_id;
}
function terminal(value: Processing): boolean {
  return ["COMPLETED", "INVALID", "FAILED"].includes(value.processing_status);
}
function identity(value: unknown): string {
  if (value instanceof NumericToken) return "numeric:" + value.source;
  if (typeof value === "bigint") return "integer:" + value.toString();
  if (Array.isArray(value))
    return "array:" + JSON.stringify(value.map(identity));
  if (value !== null && typeof value === "object")
    return (
      "object:" +
      JSON.stringify(
        Object.entries(value)
          .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
          .map(([key, v]) => [key, identity(v)]),
      )
    );
  return typeof value + ":" + JSON.stringify(value);
}
function mergeAxis<T extends Telemetry | Prediction | Processing>(
  current: T | null,
  incoming: T | null,
  key: string,
  authoritative = false,
): T | null {
  if (!incoming) return current;
  if (!current || incoming.sequence_number > current.sequence_number)
    return incoming;
  if (incoming.sequence_number < current.sequence_number) return current;
  if (
    source(incoming) !== source(current) ||
    incoming.stream_epoch !== current.stream_epoch
  )
    throw new Error("Source identity conflict; REST resync required");
  if (key === "telemetry") {
    const old = current as Telemetry,
      next = incoming as Telemetry;
    if (identity(old.features_raw) !== identity(next.features_raw))
      throw new Error("Telemetry conflict");
    if (old.validation_status === "UNVERIFIED") return incoming;
    if (next.validation_status === "UNVERIFIED") return current;
    if (old.validation_status !== next.validation_status)
      throw new Error("Validation conflict");
  } else if (key === "latest_processing") {
    const old = current as Processing,
      next = incoming as Processing;
    if (terminal(old) && !terminal(next)) return current;
    if (!terminal(old) && terminal(next)) return incoming;
    // Same-cursor retry states have no revision token. Only a fresh REST
    // snapshot can settle the ambiguity; never rank them or sort timestamps.
    if (authoritative && !terminal(old) && !terminal(next)) return incoming;
    if (identity(old) !== identity(next))
      throw new Error("Processing conflict; REST resync required");
  } else if (identity(current) !== identity(incoming))
    throw new Error("Prediction conflict");
  return current;
}
export function mergeLive(
  current: LiveSnapshot,
  update: LiveSnapshot | LiveEvent,
): LiveSnapshot {
  if (current.machine_id !== update.machine_id)
    throw new Error("Machine scope conflict");
  let snapshot: Partial<LiveSnapshot>;
  if ("type" in update) {
    const key = {
      "telemetry.received": "telemetry",
      "prediction.completed": "latest_valid_prediction",
      "processing.status_changed": "latest_processing",
    }[update.type];
    snapshot = { [key]: update.data };
  } else snapshot = update;
  const merged: LiveSnapshot = {
    ...current,
    telemetry: mergeAxis(
      current.telemetry,
      snapshot.telemetry ?? null,
      "telemetry",
    ),
    latest_valid_prediction: mergeAxis(
      current.latest_valid_prediction,
      snapshot.latest_valid_prediction ?? null,
      "latest_valid_prediction",
    ),
    latest_processing: mergeAxis(
      current.latest_processing,
      snapshot.latest_processing ?? null,
      "latest_processing",
      !("type" in update),
    ),
    as_of:
      !current.as_of ||
      (snapshot.as_of && Date.parse(snapshot.as_of) > Date.parse(current.as_of))
        ? (snapshot.as_of ?? current.as_of)
        : current.as_of,
  };
  updateFleetSnapshot(merged);
  return merged;
}
