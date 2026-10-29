
import json

import httpx

from llm_generator.generator import LLMGenerator


def test_generate_json(monkeypatch):
    expected = {
        "summary": "Cần kiểm tra hệ thống làm mát",
        "uncertainty": "Chưa xác định được nguyên nhân",
        "checklist": [
            {
                "step": 1,
                "action": "Kiểm tra thông gió",
                "chunk_ids": ["101"]
            }
        ]
    }

    def fake_post(self, url, json):
        assert json["format"] == "json"

        request = httpx.Request("POST", url)

        return httpx.Response(
            200,
            request=request,
            json={"response": __import__("json").dumps(expected)}
        )

    monkeypatch.setattr(httpx.Client, "post", fake_post)

    generator = LLMGenerator()
    result = generator.generate_json("Explain motor overheating")

    assert result == expected


def test_generate_json_invalid_response(monkeypatch):
    def fake_post(self, url, json):
        request = httpx.Request("POST", url)
        return httpx.Response(
            200,
            request=request,
            json={"response": "not valid json"}
        )

    monkeypatch.setattr(httpx.Client, "post", fake_post)

    generator = LLMGenerator()

    try:
        generator.generate_json("Explain motor overheating")
    except RuntimeError:
        return

    raise AssertionError("Phải từ chối JSON không hợp lệ")
