import { expect, it } from "vitest";
import { parseConfig } from "../../src/app/config";

it("requires explicit live OIDC without an anonymous or demo fallback", () => {
  expect(() =>
    parseConfig({ mode: "live", apiBaseUrl: "/api/v1" }, "https://ui.test"),
  ).toThrow();
  expect(() => parseConfig({ mode: "invalid" }, "https://ui.test")).toThrow();
  expect(parseConfig({ mode: "demo" }, "https://ui.test").mode).toBe("demo");
});
it("limits live API to same-origin paths and shared issuer to HTTPS", () => {
  const auth = {
    authority: "https://issuer.test",
    clientId: "business",
    scope: "openid profile",
    audience: "business-test",
  };
  expect(
    parseConfig(
      { mode: "live", apiBaseUrl: "/api/v1", auth },
      "https://ui.test",
    ).apiBaseUrl,
  ).toBe("/api/v1");
  expect(() =>
    parseConfig(
      { mode: "live", apiBaseUrl: "https://untrusted.test", auth },
      "https://ui.test",
    ),
  ).toThrow();
  expect(() =>
    parseConfig(
      {
        mode: "live",
        apiBaseUrl: "/api/v1",
        auth: { ...auth, authority: "http://issuer.test" },
      },
      "https://ui.test",
    ),
  ).toThrow();
});
