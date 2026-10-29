# Protocol chia dữ liệu AI4I phiên bản 1

Trạng thái: protocol phát triển; chưa xác nhận là protocol đánh giá chung
giữa các nhóm.

## Dữ liệu

- Nguồn: UCI AI4I 2020, DOI 10.24432/C5HS5C.
- Source SHA-256:
  dc6630cd9b1f0f853922fad78a1b6436570d3f1ec863f1dd5c4340ac56bc8a8e
- 10.000 bản ghi: 9.661 Normal, 339 Failure.
- Kiểm tra ban đầu không phát hiện feature trùng chính xác.
- Không loại bản ghi hoặc sửa nhãn trong bước chia dữ liệu.

## Phương pháp

- Train/validation/test: 60%/20%/20%.
- Seed: 42, chọn trước khi đánh giá model.
- Chia ngẫu nhiên riêng theo từng lớp để duy trì tỷ lệ Failure.
- Lưu zero-based row indices, không tính header.
- Mọi model và rule-based baseline dùng cùng tập đánh giá.
- IDs và failure-mode labels không được dùng làm feature.
- Preprocessing và resampling chỉ fit trên train.
- Chọn model, hyperparameter và threshold trên validation.
- Test chỉ dùng đánh giá cuối sau khi khóa lựa chọn.

## Kích thước

| Tập | Normal | Failure | Tổng |
| --- | ---: | ---: | ---: |
| Train | 5797 | 203 | 6000 |
| Validation | 1932 | 68 | 2000 |
| Test | 1932 | 68 | 2000 |

## Lý do lựa chọn và giới hạn

Tách validation và test để việc lựa chọn không trực tiếp tối ưu theo test.
Stratification hạn chế việc lớp Failure hiếm bị phân bố quá lệch giữa các tập.

Random split phục vụ benchmark trong cùng dataset tổng hợp. Nó không chứng minh
khả năng dự báo tương lai hoặc tổng quát hóa sang máy/nhà máy chưa gặp.
Các bản ghi gần nhau trong quá trình sinh dữ liệu có thể còn phụ thuộc;
kiểm tra không trùng chính xác chưa loại trừ mọi dạng phụ thuộc hoặc leakage.

Validation và test mỗi tập chỉ có 68 Failure, nên ước lượng recall còn bất định.
Báo cáo cuối cần trình bày số mẫu và khoảng tin cậy phù hợp.

Không đổi seed hoặc split để cải thiện kết quả. Thay đổi protocol phải có
lý do, phiên bản mới và ghi rõ ảnh hưởng đến tính độc lập của đánh giá.

## Xác minh

Chạy từ root repo:

    .\.venv\python.exe .\ml-service\training\data_preparation\create_split.py --verify-only

Xác minh phải giữ nguyên manifest. Không tự cập nhật hash để bỏ qua mismatch.