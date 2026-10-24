import {
  InMemoryWebStorage,
  UserManager,
  WebStorageStateStore,
} from "oidc-client-ts";
import { ApiError } from "./api";
import type { RuntimeConfig } from "./config";

export async function createSession(config: RuntimeConfig) {
  if (config.mode === "demo" || config.auth.mode === "none")
    return {
      name: config.mode === "demo" ? "Demo Operator" : "Local Operator",
      subject: config.mode === "demo" ? "demo" : "localhost",
      authenticated: true,
      token: async (): Promise<string | undefined> => undefined,
      login: async () => {},
      logout: async () => {},
    };
  const auth = config.auth;
  const manager = new UserManager({
    authority: auth.authority,
    client_id: auth.clientId,
    redirect_uri: `${location.origin}/auth/callback`,
    response_type: "code",
    scope: auth.scope,
    extraQueryParams: auth.audience ? { audience: auth.audience } : undefined,
    automaticSilentRenew: false,
    loadUserInfo: false,
    // Tokens only live in memory. Session storage holds transient PKCE/state, never client secrets.
    userStore: new WebStorageStateStore({ store: new InMemoryWebStorage() }),
    stateStore: new WebStorageStateStore({
      store: sessionStorage,
      prefix: "maintai.simulator.oidc.",
    }),
  });
  let user = null;
  if (location.pathname === "/auth/callback") {
    try {
      user = await manager.signinRedirectCallback();
    } finally {
      history.replaceState(null, "", "/");
    }
  }
  return {
    name: user
      ? String(
          user.profile.name ?? user.profile.preferred_username ?? "Operator",
        )
      : "Chưa đăng nhập",
    subject: user?.profile.sub ?? "anonymous",
    authenticated: !!user && !user.expired,
    token: async () => {
      const current = await manager.getUser();
      if (!current || current.expired)
        throw new ApiError(401, "SESSION_EXPIRED", "Cần đăng nhập lại.");
      return current.access_token;
    },
    login: async () => {
      await manager.signinRedirect();
    },
    logout: async () => {
      await manager.removeUser();
      location.assign("/");
    },
  };
}
export type Session = Awaited<ReturnType<typeof createSession>>;
