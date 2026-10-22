import pymupdf
from pathlib import Path

def load_pdf(file_path: str):
    documents = []
    with pymupdf.open(file_path) as pdf:
        for page_number, page in enumerate(pdf,start =1):
            text = page.get_text("text")
            if not text.strip():
                continue
            documents.append({
                "text": text,
                "metadata" :{
                    "source": Path(file_path).name, "page":page_number,
                },
            })
    return documents