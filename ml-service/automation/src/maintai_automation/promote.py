"""Promote verified ml-service-shadow configuration to Champion via MaintAI-gitops PR."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from maintai_automation.github_pr import GitHubPullRequestClient
from maintai_automation.gitops_yaml import promote_shadow_to_champion

KUSTOMIZATION_PATH = "environments/local/apps/kustomization.yaml"
HPA_PATH = "environments/local/apps/hpa.yaml"


def format_promote_pr_body(summary: dict) -> str:
    lines = [
        (
            f"Promote verified shadow candidate `{summary['model_version']}` "
            "to Champion (`ml-service`)."
        ),
        "",
        f"- **Promoted `MODEL_DIR`:** `{summary['promoted_model_dir']}`",
        f"- **Producer Git SHA:** `{summary['producer_git_sha']}`",
        f"- **Decommission Shadow & Mirror Proxy:** `{summary['decommission_shadow']}`",
        "",
        "### Promoted Artifacts (exact shadow VersionId & SHA-256)",
    ]
    for art in summary["artifacts"]:
        lines.append(
            f"- `{art['key']}` · `VersionId: {art['version_id']}` · `sha256: {art['sha256']}`"
        )
    return "\n".join(lines) + "\n"


def run_promote_shadow(
    *,
    gitops_repo: str = "chiendz11/MaintAI-gitops",
    base_branch: str = "main",
    decommission_shadow: bool = True,
    dry_run: bool = False,
    github_client: GitHubPullRequestClient | None = None,
    local_gitops_dir: Path | None = None,
) -> dict:
    if local_gitops_dir is not None:
        kust_file = local_gitops_dir / KUSTOMIZATION_PATH
        hpa_file = local_gitops_dir / HPA_PATH
        updated_kust, updated_hpa, summary = promote_shadow_to_champion(
            kust_file.read_text(encoding="utf-8"),
            hpa_file.read_text(encoding="utf-8"),
            decommission_shadow=decommission_shadow,
        )
        pr_title = f"mlops(local): promote {summary['model_version']} to Champion"
        pr_body = format_promote_pr_body(summary)
        if not dry_run:
            kust_file.write_text(updated_kust, encoding="utf-8")
            if decommission_shadow:
                hpa_file.write_text(updated_hpa, encoding="utf-8")
        return {
            "mode": "local",
            "title": pr_title,
            "body": pr_body,
            "summary": summary,
            "kustomization": updated_kust,
            "hpa": updated_hpa,
        }

    gh = github_client or GitHubPullRequestClient()
    base_sha = gh.get_branch_sha(gitops_repo, base_branch)
    kust_text, kust_sha = gh.get_file(gitops_repo, KUSTOMIZATION_PATH, ref=base_branch)
    hpa_text, hpa_sha = gh.get_file(gitops_repo, HPA_PATH, ref=base_branch)

    updated_kust, updated_hpa, summary = promote_shadow_to_champion(
        kust_text,
        hpa_text,
        decommission_shadow=decommission_shadow,
    )
    pr_title = f"mlops(local): promote {summary['model_version']} to Champion"
    pr_body = format_promote_pr_body(summary)
    branch_name = f"mlops/promote-{summary['model_version']}-{int(time.time())}"

    if dry_run:
        return {
            "mode": "dry-run",
            "branch": branch_name,
            "title": pr_title,
            "body": pr_body,
            "summary": summary,
            "kustomization": updated_kust,
            "hpa": updated_hpa,
        }

    gh.create_branch(gitops_repo, branch_name, from_sha=base_sha)
    gh.update_file(
        gitops_repo,
        KUSTOMIZATION_PATH,
        content=updated_kust,
        message=f"mlops(promote): switch ml-service Champion to {summary['model_version']}",
        branch=branch_name,
        sha=kust_sha,
    )
    if decommission_shadow and updated_hpa != hpa_text:
        gh.update_file(
            gitops_repo,
            HPA_PATH,
            content=updated_hpa,
            message=(
                "mlops(promote): decommission ml-service-shadow HPA "
                f"after promoting {summary['model_version']}"
            ),
            branch=branch_name,
            sha=hpa_sha,
        )
    pr = gh.create_pull_request(
        gitops_repo,
        title=pr_title,
        head=branch_name,
        base=base_branch,
        body=pr_body,
    )
    return {
        "mode": "pr",
        "branch": branch_name,
        "pr_url": pr.get("html_url"),
        "pr_number": pr.get("number"),
        "summary": summary,
    }


def register_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "promote-shadow", help="Promote current ml-service-shadow to Champion"
    )
    parser.add_argument("--gitops-repo", default="chiendz11/MaintAI-gitops")
    parser.add_argument("--base-branch", default="main")
    parser.add_argument(
        "--decommission-shadow",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Disable shadow deployment, mirror proxy, and shadow HPA after promoting Champion",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--local-gitops-dir", type=Path, default=None)


def run_cli(args: argparse.Namespace) -> int:
    result = run_promote_shadow(
        gitops_repo=args.gitops_repo,
        base_branch=args.base_branch,
        decommission_shadow=args.decommission_shadow,
        dry_run=args.dry_run,
        local_gitops_dir=args.local_gitops_dir,
    )
    print(json.dumps(result, indent=2))
    return 0
