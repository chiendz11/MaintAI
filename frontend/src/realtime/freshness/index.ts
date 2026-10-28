export function freshness(
  source: string | null | undefined,
  now: number,
  freshMs: number,
  staleMs: number,
) {
  if (!source) return "NOT_AVAILABLE";
  const age = now - Date.parse(source);
  if (!Number.isFinite(age)) return "NOT_AVAILABLE";
  return age < freshMs ? "FRESH" : age <= staleMs ? "DELAYED" : "STALE";
}
