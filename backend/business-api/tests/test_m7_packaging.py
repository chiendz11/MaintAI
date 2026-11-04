"""A source release can build a self-contained wheel outside the checkout."""

import subprocess
import tarfile
import zipfile
from pathlib import Path


def test_sdist_contains_canonical_bundle_and_rebuilds_wheel(tmp_path):
    root = Path(__file__).resolve().parents[3]
    import sys

    def build(*args):
        result = subprocess.run(
            [sys.executable, "-m", "build", "--no-isolation", *args],
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result.returncode == 0, result.stdout[-3000:] + result.stderr[-3000:]

    # Use a source copy so build metadata never affects the checkout.
    import shutil

    source = tmp_path / "source"
    source.mkdir()
    shutil.copytree(
        root / "backend",
        source / "backend",
        ignore=shutil.ignore_patterns("__pycache__", "*.egg-info", "build", "dist"),
    )
    shutil.copytree(root / "contracts", source / "contracts")
    build("--sdist", str(source / "backend"), "--outdir", str(tmp_path / "dist"))
    archive = next((tmp_path / "dist").glob("*.tar.gz"))
    with tarfile.open(archive) as tar:
        tar.extractall(tmp_path / "unpacked", filter="data")
    unpacked = next((tmp_path / "unpacked").iterdir())
    build("--wheel", str(unpacked), "--outdir", str(tmp_path / "wheels"))
    wheel = next((tmp_path / "wheels").glob("*.whl"))
    with zipfile.ZipFile(wheel) as bundle:
        for path in (root / "contracts/v1.3/schemas").glob("*.schema.json"):
            target = next(
                name for name in bundle.namelist() if name.endswith("/contracts/" + path.name)
            )
            assert bundle.read(target) == path.read_bytes()
        assert "rag_client/__init__.py" in bundle.namelist()
        assert "backend/shared/monitoring/catalog.py" in bundle.namelist()
