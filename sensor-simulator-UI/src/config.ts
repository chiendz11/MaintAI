export interface RuntimeConfig {
  mode: "demo" | "live";
  apiBaseUrl: string;
  pollIntervalMs: number;
  requestTimeoutMs: number;
  staleAfterMs: number;
  auth:
    | { mode: "none" }
    | {
        mode: "oidc";
        authority: string;
        clientId: string;
        scope: string;
        audience?: string;
      };
  links: { business?: string; grafana?: string; mlflow?: string };
}

const loopback = (hostname: string) =>
  ["localhost", "127.0.0.1", "[::1]"].includes(hostname);
export function publicUrl(value: string, origin: string): URL {
  const url = new URL(value, origin);
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    (url.protocol !== "https:" &&
      !(url.protocol === "http:" && loopback(url.hostname)))
  )
    throw new Error(
      "URL phải dùng HTTPS (hoặc HTTP localhost), không chứa token, query hay fragment.",
    );
  return url;
}

export function parseConfig(value: unknown, origin: string): RuntimeConfig {
  const config = value as RuntimeConfig;
  if (
    !config ||
    !["demo", "live"].includes(config.mode) ||
    typeof config.apiBaseUrl !== "string"
  )
    throw new Error("runtime-config.json thiếu mode hoặc apiBaseUrl hợp lệ.");
  for (const key of [
    "pollIntervalMs",
    "requestTimeoutMs",
    "staleAfterMs",
  ] as const) {
    if (
      !Number.isInteger(config[key]) ||
      config[key] < 1000 ||
      config[key] > 300000
    )
      throw new Error(`${key} phải là số nguyên từ 1000 đến 300000 ms.`);
  }
  if (config.staleAfterMs < config.pollIntervalMs * 2)
    throw new Error("staleAfterMs cần ít nhất bằng hai chu kỳ polling.");
  const api = publicUrl(config.apiBaseUrl || origin, origin);
  config.apiBaseUrl = config.apiBaseUrl ? api.href.replace(/\/$/, "") : "";
  if (!config.auth || !["none", "oidc"].includes(config.auth.mode))
    throw new Error("Cần cấu hình auth.mode rõ ràng.");
  if (
    config.mode === "live" &&
    config.auth.mode === "none" &&
    (!loopback(new URL(origin).hostname) || !loopback(api.hostname))
  )
    throw new Error(
      "Live anonymous chỉ được bật khi cả UI và Control API đều ở localhost.",
    );
  if (config.auth.mode === "oidc") {
    publicUrl(config.auth.authority, origin);
    if (
      !config.auth.clientId ||
      !config.auth.scope?.split(" ").includes("openid")
    )
      throw new Error("OIDC cần public clientId và scope openid.");
  }
  if (!config.links || typeof config.links !== "object")
    throw new Error("links phải là object.");
  for (const link of Object.values(config.links)) {
    if (typeof link !== "string") throw new Error("Link UI phải là URL.");
    publicUrl(link, origin);
  }
  return config;
}

export async function loadConfig(): Promise<RuntimeConfig> {
  const response = await fetch("/runtime-config.json", {
    cache: "no-store",
    redirect: "error",
  });
  if (!response.ok) throw new Error("Không tải được runtime-config.json.");
  return parseConfig(await response.json(), window.location.origin);
}
