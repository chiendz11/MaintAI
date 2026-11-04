#!/usr/bin/env python3
"""Build the eight-page report, including two landscape diagram pages.

Use Python with requirements.txt installed. Noto Serif and Noto Sans TTF files
are loaded from --fonts; --symbols supplies a DejaVu Sans TTF for math/arrows.
No network is used.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import re
import tempfile
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_LEFT
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import (
    BaseDocTemplate,
    Flowable,
    Frame,
    NextPageTemplate,
    PageBreak,
    PageTemplate,
    Paragraph,
    Table,
    TableStyle,
)
from reportlab.graphics import renderPDF
from svglib.fonts import register_font
from svglib.svglib import svg2rlg

HERE = Path(__file__).resolve().parent
INK = colors.HexColor("#183045")
TEXT = colors.HexColor("#24313a")
MUTED = colors.HexColor("#5e6a73")
TEAL = colors.HexColor("#176b70")
RULE = colors.HexColor("#d5dfe4")
PALE = colors.HexColor("#f1f6f8")
PAGE_W, PAGE_H = A4
MARGIN = 45
WIDTH = PAGE_W - 2 * MARGIN
TOTAL_PAGES = 8


def register_fonts(directory: Path, symbols: Path) -> None:
    fonts = {
        "Body": "NotoSerif-Regular.ttf",
        "BodyBold": "NotoSerif-Bold.ttf",
        "Sans": "NotoSans-Regular.ttf",
        "SansBold": "NotoSans-Bold.ttf",
    }
    for name, filename in fonts.items():
        pdfmetrics.registerFont(TTFont(name, str(directory / filename)))
    pdfmetrics.registerFont(TTFont("Symbols", str(symbols)))
    register_font("DejaVu Sans", str(symbols), rlgFontName="Symbols")
    pdfmetrics.registerFontFamily(
        "Body", normal="Body", bold="BodyBold", italic="Body", boldItalic="BodyBold"
    )
    pdfmetrics.registerFontFamily(
        "Sans", normal="Sans", bold="SansBold", italic="Sans", boldItalic="SansBold"
    )


def styles() -> dict[str, ParagraphStyle]:
    body = ParagraphStyle(
        "BodyText", fontName="Body", fontSize=11, leading=15.6,
        textColor=TEXT, alignment=TA_JUSTIFY, spaceAfter=7,
        allowWidows=0, allowOrphans=0,
    )
    return {
        "body": body,
        "title": ParagraphStyle(
            "ReportTitle", fontName="SansBold", fontSize=22, leading=28,
            textColor=INK, spaceAfter=7,
        ),
        "subtitle": ParagraphStyle(
            "Subtitle", fontName="Sans", fontSize=9.2, leading=13,
            textColor=MUTED, spaceAfter=14,
        ),
        "section": ParagraphStyle(
            "Section", fontName="SansBold", fontSize=17, leading=23,
            textColor=INK, spaceAfter=10, keepWithNext=True,
        ),
        "subsection": ParagraphStyle(
            "Subsection", fontName="SansBold", fontSize=11.4, leading=16,
            textColor=TEAL, spaceBefore=9, spaceAfter=5, keepWithNext=True,
        ),
        "cell": ParagraphStyle(
            "Cell", fontName="Sans", fontSize=9.35, leading=13.1,
            textColor=TEXT, alignment=TA_LEFT,
        ),
        "header_cell": ParagraphStyle(
            "HeaderCell", fontName="SansBold", fontSize=9.25, leading=13.1,
            textColor=INK, alignment=TA_LEFT,
        ),
        "note": ParagraphStyle(
            "Note", fontName="Sans", fontSize=9.7, leading=14.2,
            textColor=INK, alignment=TA_LEFT,
        ),
        "formula": ParagraphStyle(
            "Formula", fontName="Sans", fontSize=10.4, leading=16,
            textColor=INK, alignment=TA_LEFT,
        ),
        "caption": ParagraphStyle(
            "Caption", fontName="Sans", fontSize=8.4, leading=11.5,
            textColor=MUTED, alignment=TA_CENTER, spaceAfter=7,
        ),
        "source": ParagraphStyle(
            "Source", fontName="Sans", fontSize=8.8, leading=12.3,
            textColor=MUTED, alignment=TA_LEFT, spaceBefore=6,
        ),
    }


def markup(text: str) -> str:
    text = html.escape(text)
    text = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)
    text = re.sub(r"`([^`]+)`", r'<font name="Sans">\1</font>', text)
    return re.sub(r"[→≤≥]", r'<font name="Symbols">\g<0></font>', text)


def draw_text(canvas, text, x, y, font, size, align="center"):
    """Draw text with explicit font fallback; Noto omits some math/arrows."""
    glyphs = pdfmetrics.getFont(font).face.charToGlyph
    runs = []
    for char in text:
        use_font = font if ord(char) in glyphs else "Symbols"
        if ord(char) not in pdfmetrics.getFont(use_font).face.charToGlyph:
            raise ValueError(f"No embedded font contains {char!r}")
        if runs and runs[-1][0] == use_font:
            runs[-1] = (use_font, runs[-1][1] + char)
        else:
            runs.append((use_font, char))
    width = sum(pdfmetrics.stringWidth(value, name, size) for name, value in runs)
    if align == "center":
        x -= width / 2
    elif align == "right":
        x -= width
    for name, value in runs:
        canvas.setFont(name, size)
        canvas.drawString(x, y, value)
        x += pdfmetrics.stringWidth(value, name, size)


def box(content: str, style: ParagraphStyle, accent: bool = False) -> Table:
    flow = Table([[Paragraph(content, style)]], colWidths=[WIDTH])
    settings = [
        ("BACKGROUND", (0, 0), (-1, -1), PALE),
        ("LEFTPADDING", (0, 0), (-1, -1), 11),
        ("RIGHTPADDING", (0, 0), (-1, -1), 11),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]
    if accent:
        settings.append(("LINEBEFORE", (0, 0), (0, -1), 2, TEAL))
    flow.setStyle(TableStyle(settings))
    flow.spaceBefore, flow.spaceAfter = 4, 9
    return flow


def table(lines: list[str], sty: dict[str, ParagraphStyle], *, compact: bool = False) -> Table:
    raw = [
        [cell.strip() for cell in line.strip().strip("|").split("|")]
        for line in lines
        if not re.fullmatch(r"[\s|:\-]+", line)
    ]
    columns = len(raw[0])
    if any(len(row) != columns for row in raw):
        raise ValueError("Inconsistent report table columns")
    ratios = (
        [0.25, 0.75] if raw[0] == ["Mã sinh viên", "Họ và tên"] else
        [0.18, 0.82] if raw[0][0] == "UI" else
        [0.38, 0.62] if columns == 2 else [0.22, 0.37, 0.41]
    )
    data = [
        [Paragraph(markup(cell), sty["header_cell" if i == 0 else "cell"])
         for cell in row]
        for i, row in enumerate(raw)
    ]
    result = Table(
        data, colWidths=[WIDTH * ratio for ratio in ratios],
        repeatRows=1, hAlign="LEFT",
    )
    result.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e5eff3")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f7f9fa")]),
        ("LINEBELOW", (0, 0), (-1, 0), 0.7, RULE),
        ("LINEBELOW", (0, 1), (-1, -1), 0.35, RULE),
        ("LEFTPADDING", (0, 0), (-1, -1), 8),
        ("RIGHTPADDING", (0, 0), (-1, -1), 8),
        ("TOPPADDING", (0, 0), (-1, -1), 3 if compact else 6),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3 if compact else 6),
    ]))
    result.spaceAfter = 8
    return result


class Figure(Flowable):
    """Compact report-native vector summaries of the existing logical design."""

    def __init__(self, kind: str):
        super().__init__()
        if kind not in {"online", "release"}:
            raise ValueError(f"Unknown figure: {kind}")
        self.kind = kind
        self.width = WIDTH
        self.height = (246 if kind == "online" else 80) * WIDTH / 500
        self.spaceBefore = 4
        self.spaceAfter = 4

    def draw(self) -> None:
        canvas = self.canv
        canvas.saveState()
        canvas.scale(WIDTH / 500, WIDTH / 500)
        if self.kind == "online":
            self.online()
        else:
            self.release()
        canvas.restoreState()

    def node(self, x, y, w, h, title, subtitle, fill="#f0f5f8"):
        canvas = self.canv
        canvas.setFillColor(colors.HexColor(fill))
        canvas.setStrokeColor(colors.HexColor("#aabec9"))
        canvas.setLineWidth(0.7)
        canvas.roundRect(x, y, w, h, 4, fill=1, stroke=1)
        for value, font, size, baseline in [
            (title, "SansBold", 9.0, y + h / 2 + 3),
            (subtitle, "Sans", 8.05, y + h / 2 - 10),
        ]:
            if pdfmetrics.stringWidth(value, font, size) > w - 10:
                raise ValueError(f"Figure label too wide: {value}")
            canvas.setFillColor(INK)
            draw_text(canvas, value, x + w / 2, baseline, font, size)

    def arrow(self, points, dashed=False):
        canvas = self.canv
        canvas.setStrokeColor(colors.HexColor("#536f81"))
        canvas.setFillColor(colors.HexColor("#536f81"))
        canvas.setLineWidth(0.9)
        canvas.setDash([3, 2] if dashed else [])
        path = canvas.beginPath()
        path.moveTo(*points[0])
        for point in points[1:]:
            path.lineTo(*point)
        canvas.drawPath(path)
        canvas.setDash([])
        end, previous = points[-1], points[-2]
        angle = math.atan2(end[1] - previous[1], end[0] - previous[0])
        head = canvas.beginPath()
        head.moveTo(*end)
        for delta in [-0.4, 0.4]:
            head.lineTo(end[0] - 5 * math.cos(angle + delta),
                        end[1] - 5 * math.sin(angle + delta))
        head.close()
        canvas.drawPath(head, fill=1, stroke=0)

    def online(self):
        self.node(0, 198, 130, 40, "Sensor Simulator", "ID + sequence + spool")
        self.node(163, 198, 140, 40, "Ingestion API", "auth / envelope / ACK")
        self.node(356, 198, 144, 40, "Kafka", "sensor.readings", "#e6f0f4")
        self.arrow([(130, 218), (163, 218)])
        self.arrow([(303, 218), (356, 218)])
        for x in [75, 250, 425]:
            self.arrow([(428, 198), (428, 183), (x, 183), (x, 160)])
        self.canv.setFillColor(MUTED)
        self.canv.setFont("Sans", 8.2)
        self.canv.drawCentredString(250, 171, "Ba group độc lập, mỗi group đọc toàn bộ topic")
        self.node(0, 116, 150, 44, "Processing Worker", "DQ → gọi ML → risk", "#fbf3e4")
        self.node(177, 116, 146, 44, "Storage Consumer", "raw archive")
        self.node(350, 116, 150, 44, "Business Consumer", "telemetry / result / alert")
        self.node(0, 64, 150, 38, "processing_db", "history + Inbox + Outbox", "#fbf3e4")
        self.node(177, 64, 146, 38, "S3 / MinIO", "raw + manifest", "#eef4ee")
        self.node(350, 64, 150, 38, "business_db", "latest + alert + audit")
        self.node(0, 10, 150, 38, "Outbox Relay", "results / alert.signals", "#fbf3e4")
        self.node(350, 10, 150, 38, "Business API → Main UI", "REST / SSE sau commit")
        for x in [75, 250, 425]:
            self.arrow([(x, 116), (x, 102)])
        for x in [75, 425]:
            self.arrow([(x, 64), (x, 48)])
        self.arrow([(150, 29), (164, 29), (164, 57), (338, 57), (338, 138), (350, 138)])
        self.canv.setFont("Sans", 8.2)
        self.canv.drawCentredString(251, 45, "qua Kafka: results / signals")

    def release(self):
        self.node(0, 24, 141, 44, "Dataset được duyệt", "manifest + split")
        self.node(177, 24, 146, 44, "Train / đánh giá", "MLflow + candidate", "#fbf3e4")
        self.node(359, 24, 141, 44, "Review + PR GitOps", "pin bundle → inference", "#eef4ee")
        self.arrow([(141, 46), (177, 46)])
        self.arrow([(323, 46), (359, 46)])
        self.canv.setFont("Sans", 8.2)
        self.canv.setFillColor(MUTED)
        self.canv.drawCentredString(250, 6, "Bundle gồm model, preprocessing và drift reference khớp phiên bản")


class SvgFigure(Flowable):
    """Embed a packaged SVG as vectors; fit it to a landscape report page."""

    def __init__(self, path: Path):
        super().__init__()
        if not path.is_relative_to(HERE) or path.suffix != ".svg":
            raise ValueError(f"Report figure must be a local packaged SVG: {path}")
        self.drawing = svg2rlg(str(path))
        if self.drawing is None:
            raise ValueError(f"Cannot load report figure: {path}")
        self.hAlign = "CENTER"
        self.spaceBefore = 2
        self.spaceAfter = 5

    def wrap(self, available_width, available_height):
        self.scale = min(available_width / self.drawing.width,
                         380 / self.drawing.height)
        self.width = self.drawing.width * self.scale
        self.height = self.drawing.height * self.scale
        return self.width, self.height

    def draw(self):
        self.canv.saveState()
        self.canv.scale(self.scale, self.scale)
        renderPDF.draw(self.drawing, self.canv, 0, 0)
        self.canv.restoreState()


def parse_page(text: str, sty: dict[str, ParagraphStyle], *, compact_tables: bool = False) -> list:
    lines = text.strip().splitlines()
    flowables = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if not line:
            i += 1
            continue
        image = re.fullmatch(r"!\[([^\]]+)\]\(([^)]+)\)", line)
        if image:
            flowables.append(SvgFigure((HERE / image.group(2)).resolve()))
            flowables.append(Paragraph(markup(image.group(1)), sty["caption"]))
            i += 1
            continue
        if line.startswith("```"):
            kind = line[3:].strip()
            content = []
            i += 1
            while i < len(lines) and not lines[i].strip().startswith("```"):
                content.append(lines[i])
                i += 1
            if i == len(lines):
                raise ValueError("Unclosed report code fence")
            if kind == "diagram":
                name = "".join(content).strip()
                flowables.append(Figure(name))
                caption = (
                    "Hình 1. Luồng reading được tiếp nhận và ba nhánh xử lý; sơ đồ giản lược."
                    if name == "online" else
                    "Hình 2. Dữ liệu được duyệt mới đi vào quy trình phát hành model."
                )
                flowables.append(Paragraph(caption, sty["caption"]))
            elif kind == "formula":
                flowables.append(box("<br/>".join(markup(v) for v in content), sty["formula"]))
            else:
                raise ValueError(f"Unsupported code fence: {kind}")
            i += 1
            continue
        if line.startswith("|"):
            group = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                group.append(lines[i].strip())
                i += 1
            flowables.append(table(group, sty, compact=compact_tables))
            continue
        if line.startswith("### "):
            flowables.append(Paragraph(markup(line[4:]), sty["subsection"]))
        elif line.startswith("## "):
            flowables.append(Paragraph(markup(line[3:]), sty["section"]))
        elif line.startswith("# "):
            flowables.append(Paragraph(markup(line[2:]), sty["title"]))
        elif line.startswith("> "):
            flowables.append(box(markup(line[2:]), sty["note"], accent=True))
        else:
            paragraph = [line]
            i += 1
            while i < len(lines) and lines[i].strip():
                if re.match(r"[|#>]|```", lines[i].strip()):
                    break
                paragraph.append(lines[i].strip())
                i += 1
            content = " ".join(paragraph)
            style = (
                "subtitle" if content.startswith("Chủ đề 3 ·") else
                "source" if content.startswith("Nguồn yêu cầu:") else "body"
            )
            flowables.append(Paragraph(markup(content), sty[style]))
            continue
        i += 1
    return flowables


def chrome(canvas, document) -> None:
    canvas.saveState()
    page_width, page_height = canvas._pagesize
    canvas.setTitle("MaintAI — Thiết kế kiến trúc")
    canvas.setAuthor("Nhóm 8 — MaintAI")
    canvas.setSubject("Yêu cầu, giả định 100 máy và biện luận kiến trúc hệ thống")
    canvas.setCreator("MaintAI report builder")
    canvas.setStrokeColor(RULE)
    canvas.setLineWidth(0.5)
    canvas.line(MARGIN, page_height - 36, page_width - MARGIN, page_height - 36)
    canvas.line(MARGIN, 39, page_width - MARGIN, 39)
    canvas.setFillColor(TEAL)
    canvas.setFont("SansBold", 9)
    canvas.drawString(MARGIN, page_height - 27, "MAINTAI")
    canvas.setFillColor(MUTED)
    canvas.setFont("Sans", 8.1)
    draw_text(canvas, "YÊU CẦU → GIẢ ĐỊNH → KIẾN TRÚC",
              page_width - MARGIN, page_height - 27, "Sans", 8.1, align="right")
    canvas.drawString(MARGIN, 25, "24/10/2026 · Báo cáo cơ sở thiết kế")
    canvas.setFillColor(INK)
    canvas.setFont("SansBold", 8.8)
    canvas.drawRightString(page_width - MARGIN, 25, f"{document.page} / {TOTAL_PAGES}")
    canvas.restoreState()


class Report(BaseDocTemplate):
    def afterFlowable(self, flowable):
        if isinstance(flowable, Paragraph) and flowable.style.name == "Section":
            key = f"section-{self.page}"
            self.canv.bookmarkPage(key)
            self.canv.addOutlineEntry(flowable.getPlainText(), key, level=0)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fonts", type=Path, default=Path("/usr/share/fonts/noto"))
    parser.add_argument("--symbols", type=Path,
                        default=Path("/usr/share/fonts/TTF/DejaVuSans.ttf"))
    parser.add_argument("--source", type=Path, default=HERE / "report.md")
    parser.add_argument(
        "--output", type=Path,
        default=HERE / "Nhóm-8_MaintAI_Kien-truc.pdf",
    )
    args = parser.parse_args()
    register_fonts(args.fonts, args.symbols)
    sty = styles()
    pages = args.source.read_text(encoding="utf-8").split("<!-- PAGEBREAK -->")
    if len(pages) != TOTAL_PAGES:
        raise ValueError("The manuscript must contain exactly eight page sections")
    story = []
    for index, page in enumerate(pages):
        if index:
            if index == 6:
                story.append(NextPageTemplate("Landscape"))
            story.append(PageBreak())
        page_styles = sty
        if index == 0:
            # Fit the member roster on page one without changing fonts or later pages.
            page_styles = {
                **sty,
                "body": ParagraphStyle(
                    "FirstPageBody", parent=sty["body"], leading=14.6, spaceAfter=4,
                ),
                "title": ParagraphStyle(
                    "FirstPageTitle", parent=sty["title"], leading=26, spaceAfter=5,
                ),
                "subtitle": ParagraphStyle(
                    "FirstPageSubtitle", parent=sty["subtitle"], spaceAfter=4,
                ),
                "section": ParagraphStyle(
                    "Section", parent=sty["section"], leading=21, spaceAfter=6,
                ),
                "subsection": ParagraphStyle(
                    "FirstPageSubsection", parent=sty["subsection"],
                    leading=15, spaceBefore=6, spaceAfter=4,
                ),
            }
        story.extend(parse_page(page, page_styles, compact_tables=index == 0))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="maintai-report-") as temp:
        temporary = Path(temp) / "report.pdf"
        document = Report(
            # Retain the original 6pt frame padding and visible 45pt margins.
            str(temporary), pagesize=A4,
            leftMargin=MARGIN - 6, rightMargin=MARGIN - 6,
            topMargin=45, bottomMargin=45, pageCompression=1,
            title="MaintAI — Thiết kế kiến trúc", author="Nhóm 8 — MaintAI",
        )
        portrait_frame = Frame(
            document.leftMargin, document.bottomMargin,
            document.width, document.height, id="PortraitFrame",
        )
        land_width, land_height = landscape(A4)
        landscape_frame = Frame(
            MARGIN - 6, 45, land_width - 2 * (MARGIN - 6),
            land_height - 90, id="LandscapeFrame",
        )
        document.addPageTemplates([
            PageTemplate(id="Portrait", frames=[portrait_frame],
                         pagesize=A4, onPage=chrome),
            PageTemplate(id="Landscape", frames=[landscape_frame],
                         pagesize=landscape(A4), onPage=chrome),
        ])
        document.build(story)
        if document.page != TOTAL_PAGES:
            raise RuntimeError(
                f"Layout produced {document.page} pages; edit content/spacing to fit eight"
            )
        data = temporary.read_bytes()
        args.output.write_bytes(data)
    print(json.dumps({
        "file": str(args.output), "pages": document.page,
        "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
