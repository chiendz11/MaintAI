import hashlib
import io
import os
import tempfile
import urllib.request
import zipfile
from pathlib import Path

DATASET_URL = (
    "https://archive.ics.uci.edu/static/public/601/"
    "ai4i%2B2020%2Bpredictive%2Bmaintenance%2Bdataset.zip"
)
DATASET_SHA256 = "dc6630cd9b1f0f853922fad78a1b6436570d3f1ec863f1dd5c4340ac56bc8a8e"


def download_dataset(output: Path, *, force: bool = False) -> Path:
    if output.exists() and not force:
        if hashlib.sha256(output.read_bytes()).hexdigest() != DATASET_SHA256:
            raise ValueError("Existing dataset checksum differs; use --force to replace it")
        return output
    with urllib.request.urlopen(DATASET_URL, timeout=30) as response:
        archive_bytes = response.read(5_000_001)
    if len(archive_bytes) > 5_000_000:
        raise ValueError("Dataset archive exceeds expected size")
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        # Read a known member; never extract arbitrary archive paths.
        if archive.getinfo("ai4i2020.csv").file_size > 5_000_000:
            raise ValueError("Dataset CSV exceeds expected size")
        data = archive.read("ai4i2020.csv")
    if hashlib.sha256(data).hexdigest() != DATASET_SHA256:
        raise ValueError("UCI dataset checksum mismatch; downloaded data was not written")
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=output.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(data)
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return output
