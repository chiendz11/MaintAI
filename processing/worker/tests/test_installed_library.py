"""The wheel must import modules from the existing skeleton directories."""

import os
import shutil
import subprocess
import sys
from pathlib import Path


def test_wheel_works_outside_repository(tmp_path):
    processing = Path(__file__).resolve().parents[2]
    assert (processing / "pyproject.toml").is_file(), "Processing package metadata is missing"
    project = tmp_path / "source"
    shutil.copytree(
        processing,
        project,
        ignore=shutil.ignore_patterns("__pycache__", "*.egg-info", "build", ".pytest_cache"),
    )
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
