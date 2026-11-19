"""Minimal GitHub REST API client to create branches, commit files, and open PRs."""

from __future__ import annotations

import base64
import os

import httpx


class GitHubApiError(RuntimeError):
    """Raised when GitHub API returns an error without leaking credentials."""


class GitHubPullRequestClient:
    """Interacts with GitHub REST API v3 using Bearer token auth."""

    def __init__(
        self,
        *,
        token: str | None = None,
        api_base_url: str | None = None,
        client: httpx.Client | None = None,
        timeout: float = 20.0,
    ):
        auth_token = (token or os.environ.get("GITHUB_TOKEN") or "").strip()
        if not auth_token:
            raise ValueError("GITHUB_TOKEN is required to open GitOps pull requests")
        self._token = auth_token
        self.api_base_url = (
            api_base_url or os.environ.get("GITHUB_API_URL") or "https://api.github.com"
        ).rstrip("/")
        self._client = client
        self._timeout = timeout

    def _headers(self) -> dict[str, str]:
        return {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {self._token}",
            "X-GitHub-Api-Version": "2022-11-28",
        }

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict | None = None,
        params: dict | None = None,
    ) -> dict:
        own = self._client is None
        client = httpx.Client(timeout=self._timeout) if own else self._client
        assert client is not None
        try:
            resp = client.request(
                method,
                f"{self.api_base_url}{path}",
                headers=self._headers(),
                json=json_body,
                params=params,
            )
            if resp.status_code >= 400:
                raise GitHubApiError(
                    f"GitHub API {method} {path} failed with HTTP {resp.status_code}"
                )
            return resp.json() if resp.content else {}
        except httpx.HTTPError as exc:
            raise GitHubApiError(
                f"GitHub API network error on {path}: {type(exc).__name__}"
            ) from exc
        finally:
            if own:
                client.close()

    def get_branch_sha(self, repo: str, branch: str) -> str:
        data = self._request("GET", f"/repos/{repo}/git/ref/heads/{branch}")
        return str(data["object"]["sha"])

    def create_branch(self, repo: str, branch: str, *, from_sha: str) -> None:
        self._request(
            "POST",
            f"/repos/{repo}/git/refs",
            json_body={"ref": f"refs/heads/{branch}", "sha": from_sha},
        )

    def get_file(self, repo: str, path: str, *, ref: str) -> tuple[str, str]:
        data = self._request("GET", f"/repos/{repo}/contents/{path}", params={"ref": ref})
        raw_content = base64.b64decode(data["content"]).decode("utf-8")
        return raw_content, str(data["sha"])

    def update_file(
        self,
        repo: str,
        path: str,
        *,
        content: str,
        message: str,
        branch: str,
        sha: str,
    ) -> None:
        encoded = base64.b64encode(content.encode("utf-8")).decode("ascii")
        self._request(
            "PUT",
            f"/repos/{repo}/contents/{path}",
            json_body={
                "message": message,
                "content": encoded,
                "branch": branch,
                "sha": sha,
            },
        )

    def create_pull_request(
        self,
        repo: str,
        *,
        title: str,
        head: str,
        base: str,
        body: str,
    ) -> dict:
        return self._request(
            "POST",
            f"/repos/{repo}/pulls",
            json_body={"title": title, "head": head, "base": base, "body": body},
        )
