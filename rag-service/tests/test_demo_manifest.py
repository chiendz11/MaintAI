import json
from pathlib import Path

import pytest

from ingestion.prepare_demo import read_manifest

ROOT = Path(__file__).resolve().parents[1]


def test_bundled_manifest_pins_pdf_and_embedding_revision():
    manifest = read_manifest(ROOT / "knowledge-demo.json")
    assert manifest["embedding"]["dimensions"] == 384
    assert len(manifest["embedding"]["revision"]) == 40
    assert manifest["documents"][0]["version"] == "demo-1"


@pytest.mark.parametrize("mutation", ["pdf", "model", "duplicate", "escape", "empty"])
def test_preparation_rejects_changed_inputs_before_database_or_download(
    tmp_path, mutation
):
    manifest = json.loads((ROOT / "knowledge-demo.json").read_text())
    if mutation == "pdf":
        manifest["documents"][0]["sha256"] = "0" * 64
    elif mutation == "model":
        manifest["embedding"]["revision"] = "main"
    elif mutation == "duplicate":
        manifest["documents"] *= 2
    elif mutation == "escape":
        manifest["documents"][0]["path"] = "../README.md"
    else:
        manifest["documents"] = []
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError):
        read_manifest(path)
