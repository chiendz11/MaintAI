"""Render the four local Graphviz diagrams; no app, cloud or network operation."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import struct
import subprocess
import sys
import xml.etree.ElementTree as ET
from html import escape
from pathlib import Path

DIRECTORY = Path(__file__).resolve().parent
REPOSITORY = DIRECTORY.parent.parent
REVISION = "1.2.0"
AS_OF = "2026-10-23"
DIAGRAMS = (
    ("01-use-case", "1 · Use Case", "Vai trò, chức năng và ranh giới của bốn UI."),
    (
        "03-logical-architecture",
        "3 · Kiến trúc logic",
        "Owner, năm logical DB và keyed modules nội bộ Worker; C4-inspired.",
    ),
    (
        "04-deployment",
        "4 · Deployment",
        "EKS, các workload độc lập, image groups, Kafka node pool và RDS/S3.",
    ),
    (
        "05-data-flow",
        "5 · Luồng dữ liệu",
        "Keyed Processing, safe offset frontier, ordered outbox và dữ liệu tới UI.",
    ),
)
REFERENCES = (
    "docs/MaintAI_Functional_Overview.md",
    "docs/MaintAI_System_Contracts_v1_4_DEPLOYMENT_PROVENANCE.md",
    "docs/MaintAI_Implementation_Development_Guide_v1_4_DEPLOYMENT_PROVENANCE.md",
    "docs/DEPLOYMENT_ARCHITECTURE.md",
    "docs/RUNTIME_DATAFLOW.md",
    "docs/VERSIONING_AND_PROVENANCE.md",
    "docs/UI_ARCHITECTURE.md",
    "docs/implementation-readiness.md",
    "docs/KAFKA_DEVELOPMENT_GUIDE.md",
    "docs/PROCESSING_CONCURRENCY_AND_ORDERING.md",
    "docs/PROCESSING_ORDERING_IMPLEMENTATION_REPORT.md",
    "architecture/runtime-map.json",
    "contracts/kafka/topology.json",
    "sensor/README.md",
    "storage-consumer/README.md",
)
PLATFORM_REFERENCES = (
    "../MaintAI-gitops/platform/kafka/kafka.yaml",
    "../MaintAI-gitops/platform/kafka/nodepool.yaml",
    "../MaintAI-gitops/platform/kafka/topic-sensor-readings.yaml",
    "../MaintAI-gitops/applications/addons/strimzi.yaml",
    "../MaintAI-gitops/platform/mlops/mlflow.yaml",
)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def svg_with_description(raw: str, slug: str, title: str, description: str) -> str:
    # Strip Graphviz's external DOCTYPE and keep the standalone SVG self-contained.
    raw = raw[raw.index("<svg ") :]
    raw = raw.replace(
        "<svg ",
        f'<svg role="img" aria-labelledby="{slug}-title {slug}-desc" ',
        1,
    )
    opening = re.search(r"<svg\b[^>]*>", raw)
    if opening is None:
        raise ValueError("Graphviz did not return an SVG root")
    text = (
        f'\n<title id="{slug}-title">MaintAI — {escape(title)}</title>'
        f'\n<desc id="{slug}-desc">{escape(description)} '
        f"Thiết kế mục tiêu v1.4; snapshot {AS_OF}; không xác nhận đã deploy.</desc>"
    )
    raw = raw[: opening.end()] + text + raw[opening.end() :]
    ET.fromstring(raw)
    return raw


def gallery(svgs: dict[str, str]) -> str:
    tabs = "\n".join(
        f'<button role="tab" id="tab-{slug}" aria-controls="canvas" '
        f'data-diagram="{slug}">{escape(title)}</button>'
        for slug, title, _ in DIAGRAMS
    )
    templates = "\n".join(
        f'<template id="source-{slug}">{svgs[slug]}</template>'
        for slug, _, _ in DIAGRAMS
    )
    metadata = json.dumps(
        {
            slug: {"title": title, "description": description}
            for slug, title, description in DIAGRAMS
        },
        ensure_ascii=False,
    )
    return (
        """<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>MaintAI — Bộ sơ đồ hệ thống</title>
  <style>
    :root { color-scheme: light; font-family: system-ui, sans-serif; color: #16324f; background: #f3f6fa; }
    * { box-sizing: border-box; }
    body { margin: 0; }
    header { padding: 20px 28px 14px; background: white; border-bottom: 1px solid #dce5ee; }
    .eyebrow { margin: 0 0 6px; letter-spacing: .15em; color: #52718e; font-size: 11px; font-weight: 700; }
    h1 { margin: 0; font-size: clamp(20px, 3vw, 28px); letter-spacing: -.04em; }
    .subtitle { margin: 8px 0 0; color: #567087; font-size: 13px; line-height: 1.6; }
    .badge { display: inline-block; margin-left: 8px; padding: 3px 9px; border-radius: 20px; color: #835716; background: #fff0cc; font-size: 11px; vertical-align: middle; }
    nav { display: flex; gap: 8px; overflow-x: auto; padding: 14px 28px; }
    button, .download { font: inherit; font-size: 13px; border: 1px solid #cbd9e6; border-radius: 7px; padding: 8px 12px; background: white; color: #23425e; white-space: nowrap; cursor: pointer; }
    button:hover, .download:hover { background: #edf5ff; }
    button:focus-visible, a:focus-visible { outline: 3px solid #72a7dd; outline-offset: 2px; }
    nav button[aria-selected="true"] { color: white; background: #173d62; border-color: #173d62; }
    main { margin: 0 20px 16px; background: white; border: 1px solid #d4e0ea; border-radius: 12px; overflow: hidden; box-shadow: 0 7px 25px #18324c08; }
    .toolbar { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; padding: 12px 16px; border-bottom: 1px solid #dce5ee; }
    #description { margin: 0; font-size: 13px; line-height: 1.5; max-width: 680px; }
    .actions { display: flex; gap: 6px; flex-wrap: wrap; align-items: center; }
    .download { text-decoration: none; }
    #canvas { height: calc(100vh - 255px); min-height: 440px; touch-action: none; cursor: grab; background: #fff; }
    #canvas:active { cursor: grabbing; }
    #canvas svg { display: block; width: 100%; height: 100%; user-select: none; }
    footer { padding: 0 28px 18px; font-size: 12px; line-height: 1.6; color: #587087; }
    footer a { color: #235883; }
    @media (max-width: 650px) {
      header { padding: 16px; } nav { padding: 12px 16px; } main { margin: 0 8px 12px; }
      .badge { margin: 8px 0 0; } #canvas { min-height: 460px; height: 65vh; } footer { padding: 0 16px 16px; }
    }
  </style>
</head>
<body>
  <header>
    <p class="eyebrow">MAINTAI / VISUAL ARCHITECTURE</p>
    <h1>Bốn góc nhìn về hệ thống <span class="badge">Thiết kế mục tiêu v1.4</span></h1>
    <p class="subtitle">Snapshot __AS_OF__ · Bộ sơ đồ revision __REVISION__ · PROC-ORDER-01 target · Xem offline.</p>
  </header>
  <nav role="tablist" aria-label="Chọn sơ đồ">__TABS__</nav>
  <main>
    <div class="toolbar">
      <p id="description" aria-live="polite"></p>
      <div class="actions">
        <button id="zoom-out" aria-label="Thu nhỏ">−</button>
        <button id="zoom-in" aria-label="Phóng to">+</button>
        <button id="fit">Vừa khung</button>
        <a id="svg-download" class="download" download>SVG</a>
        <a id="png-download" class="download" download>PNG</a>
        <a id="source-download" class="download" download>Nguồn .dot</a>
      </div>
    </div>
    <div id="canvas" role="tabpanel" tabindex="0" aria-label="Sơ đồ, kéo để di chuyển và cuộn để zoom"></div>
  </main>
  <footer>Kéo để di chuyển · Cuộn để zoom · Dùng nút <b>Vừa khung</b> để khôi phục.<br>
    Các khối là kiến trúc mục tiêu; source/khung không chứng minh đã deploy. Xem <a href="README.md">README</a> để biết trạng thái, chú giải và nguồn tham chiếu.</footer>
  __TEMPLATES__
  <script>
    'use strict';
    const diagrams = __METADATA__;
    const canvas = document.querySelector('#canvas');
    let svg, initial, view, drag;
    function applyView() { svg.setAttribute('viewBox', view.join(' ')); }
    function select(slug) {
      if (!Object.hasOwn(diagrams, slug)) slug = Object.keys(diagrams)[0];
      canvas.replaceChildren(document.querySelector('#source-' + slug).content.cloneNode(true));
      svg = canvas.querySelector('svg');
      initial = svg.getAttribute('viewBox').trim().split(/\\s+/).map(Number);
      view = [...initial]; drag = null;
      document.querySelector('#description').textContent = diagrams[slug].description;
      for (const button of document.querySelectorAll('[data-diagram]')) {
        const active = button.dataset.diagram === slug;
        button.setAttribute('aria-selected', String(active));
        button.tabIndex = active ? 0 : -1;
      }
      canvas.setAttribute('aria-labelledby', 'tab-' + slug);
      document.querySelector('#svg-download').href = slug + '.svg';
      document.querySelector('#png-download').href = slug + '.png';
      document.querySelector('#source-download').href = slug + '.dot';
      history.replaceState(null, '', '#' + slug);
    }
    function zoom(factor, point) {
      const nextWidth = view[2] * factor;
      if (nextWidth < initial[2] / 30 || nextWidth > initial[2] * 5) return;
      point ??= { x: view[0] + view[2] / 2, y: view[1] + view[3] / 2 };
      view = [point.x - (point.x - view[0]) * factor,
              point.y - (point.y - view[1]) * factor, nextWidth, view[3] * factor];
      applyView();
    }
    function point(event) {
      const p = svg.createSVGPoint(); p.x = event.clientX; p.y = event.clientY;
      return p.matrixTransform(svg.getScreenCTM().inverse());
    }
    for (const button of document.querySelectorAll('[data-diagram]')) {
      button.addEventListener('click', () => select(button.dataset.diagram));
      button.addEventListener('keydown', event => {
        if (!['ArrowLeft', 'ArrowRight'].includes(event.key)) return;
        event.preventDefault(); const tabs = [...document.querySelectorAll('[data-diagram]')];
        const step = event.key === 'ArrowRight' ? 1 : -1;
        const next = tabs[(tabs.indexOf(button) + step + tabs.length) % tabs.length];
        select(next.dataset.diagram); next.focus();
      });
    }
    document.querySelector('#zoom-in').addEventListener('click', () => zoom(.8));
    document.querySelector('#zoom-out').addEventListener('click', () => zoom(1.25));
    document.querySelector('#fit').addEventListener('click', () => { view = [...initial]; applyView(); });
    canvas.addEventListener('wheel', event => { event.preventDefault(); zoom(event.deltaY < 0 ? .9 : 1.1, point(event)); }, { passive: false });
    canvas.addEventListener('pointerdown', event => {
      if (event.button !== 0) return;
      drag = point(event); canvas.setPointerCapture(event.pointerId);
    });
    canvas.addEventListener('pointermove', event => {
      if (!drag) return; const next = point(event);
      view[0] += drag.x - next.x; view[1] += drag.y - next.y; applyView();
    });
    canvas.addEventListener('pointerup', () => { drag = null; });
    canvas.addEventListener('pointercancel', () => { drag = null; });
    canvas.addEventListener('keydown', event => {
      if (event.key === '+' || event.key === '=') { event.preventDefault(); zoom(.8); }
      if (event.key === '-') { event.preventDefault(); zoom(1.25); }
      if (event.key === '0') { event.preventDefault(); view = [...initial]; applyView(); }
    });
    select(location.hash.slice(1));
  </script>
</body>
</html>
""".replace("__TABS__", tabs)
        .replace("__TEMPLATES__", templates)
        .replace("__METADATA__", metadata)
        .replace("__AS_OF__", "/".join(reversed(AS_OF.split("-"))))
        .replace("__REVISION__", REVISION)
    )


def render() -> None:
    if not shutil.which("dot"):
        raise SystemExit("Install Graphviz so the 'dot' command is available.")
    version = subprocess.run(["dot", "-V"], check=True, capture_output=True, text=True)
    svgs = {}
    artifacts = []
    for slug, title, description in DIAGRAMS:
        source = DIRECTORY / f"{slug}.dot"
        result = subprocess.run(
            ["dot", "-Tsvg", str(source)], check=True, capture_output=True, text=True
        )
        if result.stderr.strip():
            print(result.stderr, file=sys.stderr)
        svg = DIRECTORY / f"{slug}.svg"
        svg.write_text(
            svg_with_description(result.stdout, slug, title, description),
            encoding="utf-8",
        )
        svgs[slug] = svg.read_text(encoding="utf-8")
        png = DIRECTORY / f"{slug}.png"
        subprocess.run(
            ["dot", "-Tpng", "-Gdpi=110", "-Gsize=30,24", str(source), "-o", str(png)],
            check=True,
        )
        width, height = struct.unpack(">II", png.read_bytes()[16:24])
        artifacts.append(
            {
                "id": slug,
                "title": title,
                "source": source.name,
                "svg": svg.name,
                "png": png.name,
                "png_size": [width, height],
                "sha256": {path.name: digest(path) for path in (source, svg, png)},
            }
        )
        print(f"Rendered {slug}: PNG {width}x{height}")
    (DIRECTORY / "index.html").write_text(gallery(svgs), encoding="utf-8")
    runtime_map = json.loads((REPOSITORY / "architecture/runtime-map.json").read_text())
    runtime_sources = runtime_map["runtime_descriptors"] + runtime_map.get(
        "platform_runtime_descriptors", []
    )
    sources = [*REFERENCES, *runtime_sources]
    manifest = {
        "diagram_revision": REVISION,
        "as_of": AS_OF,
        "target_contract_revision": "1.4.0",
        "semantics": "Target architecture and source snapshot; not deployment or E2E evidence.",
        "renderer": (version.stderr or version.stdout).strip(),
        "font": "DejaVu Sans",
        "diagrams": artifacts,
        "local_files_sha256": {
            name: digest(DIRECTORY / name)
            for name in ("render.py", "README.md", "index.html")
        },
        "app_references_sha256": {name: digest(REPOSITORY / name) for name in sources},
        "platform_references_sha256": {
            name: digest(REPOSITORY / name)
            for name in PLATFORM_REFERENCES
            if (REPOSITORY / name).is_file()
        },
    }
    (DIRECTORY / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def check() -> None:
    manifest = json.loads((DIRECTORY / "manifest.json").read_text())
    failures = []
    count = 0
    for diagram in manifest["diagrams"]:
        for name, expected in diagram["sha256"].items():
            path = DIRECTORY / name
            count += 1
            if not path.is_file() or digest(path) != expected:
                failures.append(name)
        ET.parse(DIRECTORY / diagram["svg"])
    for section, base in (
        ("local_files_sha256", DIRECTORY),
        ("app_references_sha256", REPOSITORY),
        ("platform_references_sha256", REPOSITORY),
    ):
        for name, expected in manifest[section].items():
            path = base / name
            if section == "platform_references_sha256" and not path.is_file():
                print(f"SKIP optional sibling reference: {name}")
                continue
            count += 1
            if not path.is_file() or digest(path) != expected:
                failures.append(name)
    if failures:
        raise SystemExit(
            "Snapshot changed; review diagrams and re-render:\n" + "\n".join(failures)
        )
    print(f"PASS: 4 diagrams, valid SVG XML, {count} file/reference hashes")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check", action="store_true", help="Check existing exports and source pins"
    )
    args = parser.parse_args()
    check() if args.check else render()


if __name__ == "__main__":
    main()
