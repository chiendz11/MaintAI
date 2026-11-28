"""Open PR #1 updating ml-service-shadow in MaintAI-gitops with trained bundle & metrics."""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from maintai_automation.github_pr import GitHubPullRequestClient
from maintai_automation.gitops_yaml import ensure_shadow_hpa, update_shadow_kustomization

KUSTOMIZATION_PATH = "environments/local/apps/kustomization.yaml"
HPA_PATH = "environments/local/apps/hpa.yaml"


def format_shadow_pr_body(published: dict) -> str:
    val = published.get("validation_metrics") or {}
    test = published.get("test_metrics") or {}
    mlflow = published.get("mlflow") or {}
    ds_ver = published["dataset_version"]
    ds_vid = published.get("dataset_version_id", "")
    man_key = published.get("manifest_key", "")
    man_vid = published.get("manifest_version_id", "")
    run_id = mlflow.get("run_id", "")
    run_url = mlflow.get("run_url", "")
    reg_ver = mlflow.get("registered_model_version", "")
    lines = [
        f"Automated shadow candidate rollout for `{published['model_version']}`.",
        "",
        "### Dataset & Lineage",
        f"- **Dataset version:** `{ds_ver}` (`VersionId: {ds_vid}`)",
        f"- **Manifest:** `{man_key}` (`VersionId: {man_vid}`)",
        f"- **Training Source SHA:** `{published.get('source_git_sha', '')}`",
        f"- **MLflow Run:** [{run_id}]({run_url}) (Model `{reg_ver}` `@shadow`)",
        "",
        "### Offline Quality Gate Metrics (`Recall >= 0.85`, `Precision >= 0.50`)",
        "| Split | Recall (Failure) | Precision (Failure) | F1 | F2 | AP | Threshold |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
        (
            f"| Validation | {val.get('recall_failure', 0):.4f} | "
            f"{val.get('precision_failure', 0):.4f} | "
            f"{val.get('f1', 0):.4f} | {val.get('f2', 0):.4f} | "
            f"{val.get('average_precision', 0):.4f} | "
            f"{published.get('threshold', 0):.6f} |"
        ),
    ]
    if test:
        lines.append(
            f"| Held-out Test | {test.get('recall_failure', 0):.4f} | "
            f"{test.get('precision_failure', 0):.4f} | "
            f"{test.get('f1', 0):.4f} | {test.get('f2', 0):.4f} | "
            f"{test.get('average_precision', 0):.4f} | "
            f"{published.get('threshold', 0):.6f} |"
        )
    lines.extend(["", "### Versioned Artifacts (`maintai-models`)"])
    for art in published["artifacts"]:
        lines.append(
            f"- `{art['key']}` · `VersionId: {art['version_id']}` · `sha256: {art['sha256']}`"
        )
    return "\n".join(lines) + "\n"


def publish_shadow_pr(
    *,
    directory: Path,
    release_context_path: Path | None = None,
    gitops_repo: str | None = None,
    base_branch: str | None = None,
    dry_run: bool = False,
    github_client: GitHubPullRequestClient | None = None,
    local_gitops_dir: Path | None = None,
) -> dict:
    release_ctx = {}
    if release_context_path and release_context_path.is_file():
        release_ctx = json.loads(release_context_path.read_text(encoding="utf-8"))

    repo = gitops_repo or str(release_ctx.get("gitops_repo") or "chiendz11/MaintAI-gitops")
    branch = base_branch or str(release_ctx.get("gitops_branch") or "main")
    published = json.loads((directory / "published-artifacts.json").read_text(encoding="utf-8"))
    model_version = published["model_version"]
    pr_title = f"mlops(local): deploy shadow candidate {model_version}"
    pr_body = format_shadow_pr_body(published)

    if local_gitops_dir is not None:
        kust_file = local_gitops_dir / KUSTOMIZATION_PATH
        hpa_file = local_gitops_dir / HPA_PATH
        updated_kust = update_shadow_kustomization(
            kust_file.read_text(encoding="utf-8"),
            artifacts=published["artifacts"],
            model_version=model_version,
            producer_git_sha=published.get("source_git_sha", ""),
        )
        updated_hpa, hpa_changed = ensure_shadow_hpa(hpa_file.read_text(encoding="utf-8"))
        if not dry_run:
            kust_file.write_text(updated_kust, encoding="utf-8")
            if hpa_changed:
                hpa_file.write_text(updated_hpa, encoding="utf-8")
        return {
            "mode": "local",
            "title": pr_title,
            "body": pr_body,
            "kustomization": updated_kust,
            "hpa_changed": hpa_changed,
        }

    gh = github_client or GitHubPullRequestClient()
    base_sha = gh.get_branch_sha(repo, branch)
    kust_text, kust_sha = gh.get_file(repo, KUSTOMIZATION_PATH, ref=branch)
    hpa_text, hpa_sha = gh.get_file(repo, HPA_PATH, ref=branch)

    updated_kust = update_shadow_kustomization(
        kust_text,
        artifacts=published["artifacts"],
        model_version=model_version,
        producer_git_sha=published.get("source_git_sha", ""),
    )
    updated_hpa, hpa_changed = ensure_shadow_hpa(hpa_text)

    feature_branch = f"mlops/shadow-{model_version}-{int(time.time())}"
    if dry_run:
        return {
            "mode": "dry-run",
            "branch": feature_branch,
            "title": pr_title,
            "body": pr_body,
            "kustomization": updated_kust,
            "hpa_changed": hpa_changed,
        }

    gh.create_branch(repo, feature_branch, from_sha=base_sha)
    gh.update_file(
        repo,
        KUSTOMIZATION_PATH,
        content=updated_kust,
        message=f"mlops(shadow): update ml-service-shadow to {model_version}",
        branch=feature_branch,
        sha=kust_sha,
    )
    if hpa_changed:
        gh.update_file(
            repo,
            HPA_PATH,
            content=updated_hpa,
            message=f"mlops(shadow): restore ml-service-shadow HPA for {model_version}",
            branch=feature_branch,
            sha=hpa_sha,
        )
    pr = gh.create_pull_request(
        repo,
        title=pr_title,
        head=feature_branch,
        base=branch,
        body=pr_body,
    )
    return {
        "mode": "pr",
        "branch": feature_branch,
        "pr_url": pr.get("html_url"),
        "pr_number": pr.get("number"),
    }


def register_parser(subparsers) -> None:
    parser = subparsers.add_parser(
        "publish", help="Open MaintAI-gitops PR updating ml-service-shadow"
    )
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--context", type=Path, default=None)
    parser.add_argument("--gitops-repo", default=None)
    parser.add_argument("--base-branch", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--local-gitops-dir", type=Path, default=None)


def run_cli(args: argparse.Namespace) -> int:
    env_dry_run = os.environ.get("PUBLISH_DRY_RUN", "false").strip().lower() in {"1", "true", "yes"}
    result = publish_shadow_pr(
        directory=args.directory,
        release_context_path=args.context,
        gitops_repo=args.gitops_repo,
        base_branch=args.base_branch,
        dry_run=args.dry_run or env_dry_run,
        local_gitops_dir=args.local_gitops_dir,
    )
    print(json.dumps(result, indent=2))
    return 0
