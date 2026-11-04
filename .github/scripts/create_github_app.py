#!/usr/bin/env python3
"""Automated assistant to create a GitHub App via GitHub's official Manifest Flow.

1. Starts a temporary local server on http://localhost:8765
2. Generates an HTML page that submits the manifest to GitHub
3. Handles the redirect callback from GitHub
4. Exchanges code for App ID and Private Key (.pem)
5. Automatically sets GITOPS_APP_ID and GITOPS_APP_PRIVATE_KEY via gh CLI
6. Prompts user with the 1-click installation link
"""

import json
import subprocess
import sys
import threading
import time
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

PORT = 8765
REPO = "chiendz11/MaintAI"
APP_NAME = f"maintai-sync-{int(time.time())}"

MANIFEST = {
    "name": APP_NAME,
    "url": "https://github.com/chiendz11/MaintAI",
    "hook_attributes": {
        "url": "https://example.com/webhook",
        "active": False,
    },
    "redirect_url": f"http://localhost:{PORT}/callback",
    "public": False,
    "default_permissions": {
        "contents": "write",
        "pull_requests": "write",
        "metadata": "read",
    },
    "default_events": [],
}

app_result = {}
shutdown_event = threading.Event()


CSS = """
body { font-family: -apple-system, sans-serif; display: flex;
justify-content: center; align-items: center; height: 100vh;
margin: 0; background: #0d1117; color: #c9d1d9; }
.card { background: #161b22; border: 1px solid #30363d;
border-radius: 8px; padding: 32px; max-width: 480px; text-align: center; }
h2 { color: #58a6ff; margin-top: 0; }
p { line-height: 1.5; font-size: 14px; color: #8b949e; }
button, .btn { background: #238636; color: white; border: none;
padding: 12px 24px; font-size: 16px; font-weight: 600;
border-radius: 6px; cursor: pointer; text-decoration: none; display: inline-block; }
.badge { display: inline-block; padding: 4px 8px; background: #21262d;
border-radius: 4px; font-family: monospace; color: #58a6ff; margin: 4px 0 16px 0; }
"""


class CallbackHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == "/":
            manifest_json = json.dumps(MANIFEST)
            html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Tạo GitHub App</title><style>{CSS}</style></head>
<body>
<div class="card">
    <h2>Tạo GitHub App cho MaintAI</h2>
    <p>Xác nhận tạo GitHub App với quyền hạn tối thiểu (Contents: write trên 2 repo platform).</p>
    <form action="https://github.com/settings/apps/new" method="post">
        <input type="hidden" name="manifest" value='{manifest_json}'>
        <button type="submit">Xác nhận tạo App trên GitHub</button>
    </form>
</div>
</body></html>"""
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html.encode())

        elif url.path == "/callback":
            query = parse_qs(url.query)
            code = query.get("code", [None])[0]
            if not code:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"Missing code in callback")
                return

            try:
                conversion_url = f"https://api.github.com/app-manifests/{code}/conversions"
                req = urllib.request.Request(
                    conversion_url,
                    method="POST",
                    headers={
                        "Accept": "application/vnd.github+json",
                        "User-Agent": "MaintAI-Setup-Script",
                    },
                )
                with urllib.request.urlopen(req) as resp:
                    data = json.loads(resp.read().decode())

                app_result["id"] = data["id"]
                app_result["slug"] = data["slug"]
                app_result["pem"] = data["pem"]

                install_url = f"https://github.com/settings/apps/{data['slug']}/installations"

                html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>App tạo thành công</title><style>{CSS}</style></head>
<body>
<div class="card">
    <h2 style="color: #3fb950;">✓ GitHub App đã tạo thành công!</h2>
    <div class="badge">App ID: {data["id"]} ({data["slug"]})</div>
    <p>Private Key và App ID đang được lưu vào GitHub Secrets của repo <code>{REPO}</code>.</p>
    <p><strong>Bước cuối:</strong> Bấm nút để cài đặt App vào 2 repo platform.</p>
    <a class="btn" href="{install_url}" target="_blank">Cài đặt App &rarr;</a>
</div>
</body></html>"""
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(html.encode())

            except Exception as e:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(f"Conversion failed: {e}".encode())
            finally:
                shutdown_event.set()


def main():
    print("=" * 60)
    print("  MaintAI — Hỗ trợ tạo GitHub App tự động qua Manifest Flow")
    print("=" * 60)
    print(f"\n1. Đang khởi động máy chủ cục bộ tại http://localhost:{PORT} ...")

    server = HTTPServer(("127.0.0.1", PORT), CallbackHandler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    url = f"http://localhost:{PORT}/"
    print("2. Hãy mở liên kết sau trong trình duyệt để tạo App:")
    print(f"\n   👉  \033[1;36m{url}\033[0m\n")

    try:
        webbrowser.open(url)
    except Exception:
        pass

    print("Đang chờ bạn bấm xác nhận trên trình duyệt...")
    while not shutdown_event.is_set():
        time.sleep(0.5)

    time.sleep(1)
    server.shutdown()

    if "id" in app_result and "pem" in app_result:
        app_id = str(app_result["id"])
        slug = app_result["slug"]
        pem = app_result["pem"]

        print(f"\n[OK] Nhận thông tin App thành công: ID={app_id}, Slug={slug}")
        print("Đang lưu GITOPS_APP_ID vào GitHub Secrets...")
        cmd_id = ["gh", "secret", "set", "GITOPS_APP_ID", "--body", app_id, "--repo", REPO]
        subprocess.run(cmd_id, check=True)

        print("Đang lưu GITOPS_APP_PRIVATE_KEY vào GitHub Secrets...")
        cmd_pem = ["gh", "secret", "set", "GITOPS_APP_PRIVATE_KEY", "--body", pem, "--repo", REPO]
        subprocess.run(cmd_pem, check=True)

        print(f"\n✓ Hoàn tất cấu hình Secret cho {REPO}!")
        print("\nBước cuối cùng:")
        print(f"Truy cập: https://github.com/settings/apps/{slug}/installations")
        print("Chọn tài khoản 'chiendz11' và cấp quyền (Install) cho 2 repo:")
        print("  - chiendz11/MaintAI-gitops")
        print("  - chiendz11/Maint-infrastructure")
        print("\nSau đó, bất cứ khi nào push vào MaintAI/main, workflow sẽ tự động chạy!")
    else:
        print("\n[LỖI] Không nhận được thông tin GitHub App.", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
