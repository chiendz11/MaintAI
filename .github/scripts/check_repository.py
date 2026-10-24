"""Read-only CI checks for documentation provenance and runtime inventory."""

import hashlib
import json
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[2]


def local_path(name: str, parent: Path = ROOT) -> Path:
    target = (parent / unquote(name)).resolve()
    if not target.is_relative_to(ROOT):
        raise ValueError(f"Repository reference escapes checkout: {name}")
    if not target.exists():
        raise ValueError(f"Repository reference does not exist: {target.relative_to(ROOT)}")
    return target


def without_fences(text: str) -> str:
    return re.sub(r"^```[^\n]*\n.*?^```\s*$", "", text, flags=re.MULTILINE | re.DOTALL)


def anchors(path: Path) -> set[str]:
    text = without_fences(path.read_text())
    used = set(re.findall(r'<a\s+(?:id|name)=[\'"]([^\'"]+)', text))
    for title in re.findall(r"^#{1,6}\s+(.+?)\s*#*\s*$", text, flags=re.MULTILINE):
        title = title.replace("`", "").lower()
        base = "".join(c for c in title if c.isalnum() or c in "_- ").replace(" ", "-")
        slug, index = base, 0
        while slug in used:
            index += 1
            slug = f"{base}-{index}"
        used.add(slug)
    return used


def check_links(path: Path) -> tuple[int, int]:
    count, companions = 0, 0
    for target in re.findall(
        r"\]\((<[^>]+>|[^\s)]+)(?:\s+\"[^\"]*\")?\)", without_fences(path.read_text())
    ):
        target = target.strip("<>")
        if urlsplit(target).scheme:
            continue
        name, _, anchor = target.partition("#")
        candidate = (path.parent / unquote(name)).resolve() if name else path
        if not candidate.is_relative_to(ROOT) and any(
            candidate.is_relative_to(ROOT.parent / repo)
            for repo in ("MaintAI-gitops", "Maint-infrastructure", "MaintAI-k3d")
        ):
            companions += 1  # Separate checkouts are not present on an app-only runner.
            continue
        destination = local_path(name, path.parent) if name else path
        if anchor and destination.suffix == ".md" and unquote(anchor) not in anchors(destination):
            raise ValueError(f"Missing heading in {path.relative_to(ROOT)}: {target}")
        count += 1
    return count, companions


def main():
    manifest = json.loads((ROOT / "docs/source-manifest.json").read_text())
    documents = manifest["documents"]
    names = [doc["path"] for doc in documents]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate active document in source manifest")
    for doc in documents:
        path = local_path(doc["path"])
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != doc["sha256"]:
            raise ValueError(f"Documentation checksum mismatch: {doc['path']}")
    markdown = {local_path(name) for name in names if name.endswith(".md")}
    markdown.update(
        local_path(name)
        for name in (
            "README.md",
            "AGENTS.md",
            "architecture/README.md",
            "contracts/README.md",
            ".github/README.md",
            "sensor/README.md",
            "sensor-simulator-UI/README.md",
            "ml-service/README.md",
            "ml-service/mlflow/README.md",
            "processing/README.md",
            "messaging/README.md",
            "backend/README.md",
            "contracts/kafka/README.md",
        )
    )
    checks = [check_links(path) for path in sorted(markdown)]
    links, companions = sum(c[0] for c in checks), sum(c[1] for c in checks)
    inventory = json.loads((ROOT / "architecture/runtime-map.json").read_text())
    for decision in inventory.get("design_decisions", []):
        local_path(decision["document"])
        local_path(decision["implementation_report"])
    app_descriptors = inventory["runtime_descriptors"]
    platform_descriptors = inventory.get("platform_runtime_descriptors", [])
    ids, descriptors = set(), app_descriptors + platform_descriptors
    if len(descriptors) != len(set(descriptors)):
        raise ValueError("Duplicate runtime descriptor")
    for descriptor in descriptors:
        runtime = json.loads(local_path(descriptor).read_text())
        if runtime["runtime_id"] in ids:
            raise ValueError(f"Duplicate runtime ID: {runtime['runtime_id']}")
        ids.add(runtime["runtime_id"])
        if runtime["target_contract_revision"] != inventory["target_contract_revision"]:
            raise ValueError(f"Runtime baseline differs from inventory: {descriptor}")
        local_path(runtime["dockerfile"])
        for name in runtime["source_paths"]:
            local_path(name)
        if transport := runtime.get("transport_implementation"):
            local_path(transport["document"])
            if transport["sdk_version"] != manifest["kafka_sdk_version"]:
                raise ValueError(f"Transport SDK pin differs from source manifest: {descriptor}")
            if transport["kafka_bundle_version"] != manifest["kafka_contract_bundle_version"]:
                raise ValueError(f"Transport bundle pin differs from source manifest: {descriptor}")
    images = json.loads((ROOT / "sensor-simulator-UI/ci-images.json").read_text())
    for key in ("node_image", "nginx_image"):
        if not re.fullmatch(r"[\w./:-]+@sha256:[0-9a-f]{64}", images[key]):
            raise ValueError(f"CI container base image must use an immutable digest: {key}")
    print(
        f"Verified {len(documents)} document hashes, {links} local links/anchors, "
        f"{len(app_descriptors)} app + {len(platform_descriptors)} platform runtime descriptors "
        f"and pinned UI CI images. "
        f"{companions} companion-repository links are outside this checkout."
    )


if __name__ == "__main__":
    main()
