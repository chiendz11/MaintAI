"""Repository-scoped GitHub App dispatch; never prints or persists credentials."""

import json
import os
import time
from urllib.request import Request, urlopen

import jwt

TARGET = "chiendz11/MaintAI-gitops"
HEADERS = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}


def dispatch_workload(intent, *, opener=urlopen):
    issued = int(time.time())
    app_id, installation = os.environ["GITHUB_APP_ID"], os.environ["GITHUB_APP_INSTALLATION_ID"]
    if not app_id.isdigit() or not installation.isdigit():
        raise ValueError("Invalid GitHub App configuration")
    encoded = jwt.encode(
        {"iat": issued - 30, "exp": issued + 540, "iss": app_id},
        os.environ["GITHUB_APP_PRIVATE_KEY"],
        algorithm="RS256",
    )
    request = Request(
        f"https://api.github.com/app/installations/{installation}/access_tokens",
        data=json.dumps(
            {"repositories": ["MaintAI-gitops"], "permissions": {"actions": "write"}}
        ).encode(),
        headers={**HEADERS, "Authorization": "Bearer " + encoded},
        method="POST",
    )
    with opener(request, timeout=30) as response:
        token = json.load(response)["token"]
    request = Request(
        f"https://api.github.com/repos/{TARGET}/actions/workflows/workload-release.yml/dispatches",
        data=json.dumps({"ref": "main", "inputs": {"intent": json.dumps(intent)}}).encode(),
        headers={**HEADERS, "Authorization": "Bearer " + token},
        method="POST",
    )
    with opener(request, timeout=30) as response:
        if response.status != 204:
            raise ValueError("GitOps dispatch was not acknowledged")
    print("GitOps dispatch acknowledged; this is not a created/merged PR or a deployment.")
