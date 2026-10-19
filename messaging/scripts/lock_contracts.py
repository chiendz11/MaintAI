"""Version and verify the Kafka contract bundle; never mutate it at runtime."""

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2] / "contracts"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--bundle-version")
    args = parser.parse_args()
    lock = ROOT / "kafka/manifest.json"
    old = json.loads(lock.read_text()) if lock.exists() else None
    version = args.bundle_version or (old["bundle_version"] if old else "1.0.0")
    files = [
        p
        for p in (ROOT / "kafka").rglob("*")
        if p.is_file() and p != lock and p.suffix in {".json", ".sql"}
    ] + [ROOT / "v1.3/schemas/runtime.schema.json", ROOT / "v1.3/events/alert-signal.schema.json"]
    value = {
        "schema_version": "1.0.0",
        "bundle_version": version,
        "target_design": "1.4.0",
        "sha256": {
            str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(files)
        },
    }
    if args.check:
        if value != old:
            raise SystemExit("Kafka contract lock differs from source files")
        print(f"PASS Kafka bundle {version}: {len(files)} immutable inputs")
    else:
        if old and old != value and old["bundle_version"] == version:
            raise SystemExit("Changed immutable bundle requires a new --bundle-version")
        lock.write_text(json.dumps(value, indent=2) + "\n")
        print(f"Locked Kafka bundle {version}: {len(files)} inputs")


if __name__ == "__main__":
    main()
