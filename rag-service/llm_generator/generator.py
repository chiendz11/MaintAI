
import os
import httpx
import json

from dotenv import load_dotenv

load_dotenv()


class LLMGenerator:
    def __init__(self):
        self.base_url = os.getenv(
            "OLLAMA_BASE_URL",
            "http://localhost:11434"
        ).rstrip("/")

        self.model = os.getenv(
            "OLLAMA_MODEL",
            "qwen2.5:3b"
        )

        self.timeout = float(
            os.getenv("OLLAMA_TIMEOUT", "120")
        )

        if self.timeout <= 0:
            raise ValueError(
                "OLLAMA_TIMEOUT phải lớn hơn 0"
            )

    def generate(self, prompt: str) -> str:
        if not prompt or not prompt.strip():
            raise ValueError(
                "Prompt không được để trống"
            )

        url = f"{self.base_url}/api/generate"

        payload = {
            "model": self.model,
            "prompt": prompt.strip(),
            "stream": False,
            "options": {
                "temperature": 0.1
            }
        }

        try:
            with httpx.Client(
                timeout=self.timeout
            ) as client:
                response = client.post(
                    url,
                    json=payload
                )

                response.raise_for_status()
                data = response.json()

        except httpx.TimeoutException as exc:
            raise RuntimeError(
                "LLM xử lý quá thời gian cho phép"
            ) from exc

        except httpx.ConnectError as exc:
            raise RuntimeError(
                "Không kết nối được Ollama. "
                "Hãy kiểm tra Ollama đã chạy chưa."
            ) from exc

        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"Ollama trả về lỗi HTTP "
                f"{exc.response.status_code}: "
                f"{exc.response.text[:300]}"
            ) from exc

        except (httpx.RequestError, ValueError) as exc:
            raise RuntimeError(
                f"Lỗi khi gọi Ollama: {exc}"
            ) from exc

        if not isinstance(data, dict):
            raise RuntimeError(
                "Ollama trả về dữ liệu không hợp lệ"
            )

        answer = data.get("response")

        if not isinstance(answer, str) or not answer.strip():
            raise RuntimeError(
                "LLM không tạo được câu trả lời"
            )

        return answer.strip()
    
    def generate_json(self, prompt: str) -> dict:
        if not prompt or not prompt.strip():
            raise ValueError("Prompt không được để trống")

        url = f"{self.base_url}/api/generate"

        payload = {
            "model": self.model,
            "prompt": prompt.strip(),
            "stream": False,
            "format": "json",
            "options": {
                "temperature": 0.1
            }
        }

        try:
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(url, json=payload)
                response.raise_for_status()
                data = response.json()

        except httpx.TimeoutException as exc:
            raise RuntimeError(
                "LLM xử lý quá thời gian cho phép"
            ) from exc

        except httpx.ConnectError as exc:
            raise RuntimeError(
                "Không kết nối được Ollama"
            ) from exc

        except httpx.HTTPStatusError as exc:
            raise RuntimeError(
                f"Ollama HTTP {exc.response.status_code}: "
                f"{exc.response.text[:300]}"
            ) from exc

        except httpx.RequestError as exc:
            raise RuntimeError(
                f"Lỗi kết nối Ollama: {exc}"
            ) from exc

        try:
            answer = data["response"]
            result = json.loads(answer)
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                "Ollama không trả về JSON hợp lệ"
            ) from exc

        if not isinstance(result, dict):
            raise RuntimeError(
                "Kết quả JSON phải là một object"
            )

        required_fields = {
            "summary",
            "uncertainty",
            "checklist"
        }

        if not required_fields.issubset(result):
            raise RuntimeError(
                "LLM trả về thiếu trường bắt buộc"
            )

        if (
            not isinstance(result["summary"], str)
            or not isinstance(result["uncertainty"], str)
            or not isinstance(result["checklist"], list)
        ):
            raise RuntimeError(
                "LLM trả về sai kiểu dữ liệu"
            )

        return result

