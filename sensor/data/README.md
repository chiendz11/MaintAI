# AI4I dataset

Tải dataset từ root repo bằng `.venv/bin/maintai-sensor download-data`.
File `ai4i2020.csv` được kiểm tra SHA-256 và giữ local, không commit vào Git.

Nguồn: [AI4I 2020 Predictive Maintenance Dataset, UCI](https://archive.ics.uci.edu/dataset/601/ai4i%2B2020%2Bpredictive%2Bmaintenance%2Bdataset),
[DOI 10.24432/C5HS5C](https://doi.org/10.24432/C5HS5C).
License: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).

Các CSV trong `tests/fixtures/` là fixture minh họa dựa trên format AI4I, không
phải reference/training data và không dùng làm bằng chứng model accuracy.
