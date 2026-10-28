export type RuntimeConfig = {
  mode: "demo" | "live";
  apiBaseUrl: string;
  freshMs: number;
  staleMs: number;
  auth?: {
    authority: string;
    clientId: string;
    scope: string;
    audience?: string;
  };
};
export function parseConfig(value: unknown, origin: string): RuntimeConfig {
  if (!value || typeof value !== "object")
    throw new Error("Thiếu cấu hình giao diện.");
  const config = value as RuntimeConfig;
  if (!["demo", "live"].includes(config.mode))
    throw new Error("Cần chọn rõ chế độ demo hoặc live.");
  const api = config.apiBaseUrl ?? "/api/v1";
  if (
    typeof api !== "string" ||
    !api.startsWith("/") ||
    api.startsWith("//") ||
    /[?#\\]/.test(api)
  )
    throw new Error("Business API phải dùng đường dẫn cùng origin.");
  const freshMs = config.freshMs ?? 2000,
    staleMs = config.staleMs ?? 5000;
  if (
    !Number.isInteger(freshMs) ||
    !Number.isInteger(staleMs) ||
    freshMs <= 0 ||
    staleMs < freshMs ||
    staleMs > 300000
  )
    throw new Error("Ngưỡng freshness không hợp lệ.");
  if (config.mode === "live") {
    const auth = config.auth;
    if (
      !auth ||
      typeof auth.authority !== "string" ||
      typeof auth.clientId !== "string" ||
      !auth.clientId.trim() ||
      !auth.scope?.split(" ").includes("openid")
    )
      throw new Error("Live cần cấu hình OIDC public client.");
    const url = new URL(auth.authority),
      local =
        ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname) &&
        ["localhost", "127.0.0.1", "[::1]"].includes(new URL(origin).hostname);
    if (
      url.username ||
      url.password ||
      url.search ||
      url.hash ||
      (url.protocol !== "https:" && !(url.protocol === "http:" && local))
    )
      throw new Error(
        "OIDC phải dùng HTTPS, hoặc HTTP loopback trong local test.",
      );
  }
  return { ...config, apiBaseUrl: api.replace(/\/$/, ""), freshMs, staleMs };
}
export async function loadConfig(): Promise<RuntimeConfig> {
  const response = await fetch("/runtime-config.json", {
    cache: "no-store",
    redirect: "error",
  });
  if (!response.ok) throw new Error("Không đọc được cấu hình giao diện.");
  return parseConfig(await response.json(), location.origin);
}
