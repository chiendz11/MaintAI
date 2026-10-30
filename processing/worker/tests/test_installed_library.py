"""The wheel must import modules from the existing skeleton directories."""

import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest


@pytest.mark.parametrize("build_kind", ["wheel", "sdist", "sdist-rebuilt"])
def test_wheel_works_outside_repository(tmp_path, build_kind):
    processing = Path(__file__).resolve().parents[2]
    assert (processing / "pyproject.toml").is_file(), "Processing package metadata is missing"
    project = tmp_path / "source"
    shutil.copytree(
        processing,
        project,
        ignore=shutil.ignore_patterns("__pycache__", "*.egg-info", "build", ".pytest_cache"),
    )
    # M6 schemas are explicit package inputs from the canonical contracts bundle.
    schema_inputs = tmp_path / "contracts/v1.3/schemas"
    schema_inputs.mkdir(parents=True)
    for name in ("history-m6.schema.json", "raw-catalog-m6.schema.json"):
        shutil.copy2(processing.parent / "contracts/v1.3/schemas" / name, schema_inputs / name)
    if build_kind != "wheel":
        import tarfile

        checkout_metadata = (project / "pyproject.toml").read_bytes()
        distributions = tmp_path / "distributions"
        distributions.mkdir()
        built = subprocess.run(
            [
                sys.executable,
                "-c",
                "from setuptools.build_meta import build_sdist; "
                f"build_sdist({str(distributions)!r})",
            ],
            cwd=project,
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert built.returncode == 0, built.stdout + built.stderr
        assert (project / "pyproject.toml").read_bytes() == checkout_metadata
        assert not (project / "contracts").exists(), "sdist polluted the source checkout"
        (source_archive,) = distributions.glob("*.tar.gz")
        extracted = tmp_path / "extracted"
        with tarfile.open(source_archive) as archive:
            assert all(".." not in Path(m.name).parts for m in archive.getmembers()), (
                "Source distribution references files outside its release tree"
            )
            archive.extractall(extracted, filter="data")
        (project,) = extracted.iterdir()
        # Move the canonical checkout away: the sdist must carry its own inputs.
        schema_inputs.parent.parent.rename(tmp_path / "canonical-schemas-not-available")
        if build_kind == "sdist-rebuilt":
            rebuilt = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    "from setuptools.build_meta import build_sdist; "
                    f"build_sdist({str(distributions)!r})",
                ],
                cwd=project,
                capture_output=True,
                text=True,
                timeout=30,
            )
            assert rebuilt.returncode == 0, rebuilt.stdout + rebuilt.stderr
    wheels = tmp_path / "wheels"
    wheels.mkdir()
    build = subprocess.run(
        [
            sys.executable,
            "-c",
            f"from setuptools.build_meta import build_wheel; build_wheel({str(wheels)!r})",
        ],
        cwd=project,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert build.returncode == 0, build.stdout + build.stderr
    (wheel,) = wheels.glob("*.whl")
    bundled = tmp_path / "bundled-schemas"
    bundled.mkdir()
    with zipfile.ZipFile(wheel) as archive:
        for name in ("history-m6.schema.json", "raw-catalog-m6.schema.json"):
            key = "maintai_processing-0.1.0.data/data/share/maintai-processing/contracts/" + name
            raw = archive.read(key)
            assert raw == (processing.parent / "contracts/v1.3/schemas" / name).read_bytes()
            (bundled / name).write_bytes(raw)
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    smoke = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "import sys; "
            f"sys.path.insert(0, {str(wheel)!r}); "
            "from processing.shared.data_quality.validator import FeatureValidator; "
            "from processing.shared.risk_engine.policy import RiskPolicy; "
            "from processing.shared.risk_engine.engine import evaluate_risk; "
            "from processing.shared.history_meta.schemas import Schemas; "
            f"assert 'HistoryPage' in Schemas({str(bundled)!r}).locations; "
            "p = RiskPolicy.from_dict({'policy_version':'fixture-only', "
            "'thresholds':{'watch':0.4,'high':0.65,'critical':0.85}, "
            "'create_alert_for':['HIGH']}); "
            "assert evaluate_risk(failure_probability=0.7, policy=p).machine_state == 'HIGH'; "
            "print('wheel smoke passed')",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert smoke.returncode == 0, smoke.stdout + smoke.stderr
    assert "wheel smoke passed" in smoke.stdout
