import {
  InMemoryWebStorage,
  UserManager,
  WebStorageStateStore,
} from "oidc-client-ts";
import { ApiError } from "../api/client";
import type { RuntimeConfig } from "./config";

export async function createSession(config: RuntimeConfig) {
  if (config.mode === "demo")
    return {
      authenticated: true,
      name: "Demo Viewer",
      role: "TECHNICIAN",
      subject: "demo-technician",
      canManage: true,
      token: async () => "",
      login: async () => {},
      logout: async () => {
        location.assign("/machines");
      },
    };
  const auth = config.auth!;
  const manager = new UserManager({
    authority: auth.authority,
    client_id: auth.clientId,
    redirect_uri: location.origin + "/auth/callback",
    response_type: "code",
    scope: auth.scope,
    extraQueryParams: auth.audience ? { audience: auth.audience } : undefined,
    automaticSilentRenew: false,
    loadUserInfo: false,
    userStore: new WebStorageStateStore({ store: new InMemoryWebStorage() }),
    stateStore: new WebStorageStateStore({
      store: sessionStorage,
      prefix: "maintai.business.oidc.",
    }),
  });
  let user = null;
  if (location.pathname === "/auth/callback") {
    try {
      user = await manager.signinRedirectCallback();
    } finally {
      history.replaceState(null, "", "/machines");
    }
  }
  return {
    authenticated: !!user && !user.expired,
    name: user
      ? String(
          user.profile.name ?? user.profile.preferred_username ?? "Người dùng",
        )
      : "Chưa đăng nhập",
    role: String(user?.profile[auth.rolesClaim ?? "roles"] ?? "Business"),
    subject: user?.profile.sub ?? "",
    canManage:
      Array.isArray(user?.profile[auth.rolesClaim ?? "roles"]) &&
      (user?.profile[auth.rolesClaim ?? "roles"] as unknown[]).some(
        (role) => role === "TECHNICIAN" || role === "ADMIN",
      ),
    token: async () => {
      const value = await manager.getUser();
      if (!value || value.expired) throw new ApiError(401, "SESSION_EXPIRED");
      return value.access_token;
    },
    login: async () => {
      await manager.signinRedirect();
    },
    logout: async () => {
      await manager.removeUser();
      location.assign("/machines");
    },
  };
}
export type Session = Awaited<ReturnType<typeof createSession>>;
