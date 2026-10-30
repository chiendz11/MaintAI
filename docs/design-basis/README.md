# Báo cáo cơ sở thiết kế hệ thống MaintAI

**Chủ đề:** Predictive Maintenance & Technician Copilot.
**Nội dung:** Yêu cầu — Giả định vận hành — Kiến trúc — Biện luận.
**Phiên bản:** 1.2.3 — ngày 25/10/2026.

## Bản PDF để nộp

[Nhóm 8 — MaintAI: Yêu cầu, giả định và biện luận kiến trúc (8 trang A4)](report/Nhóm-8_MaintAI_Kien-truc.pdf)
được biên tập theo mạch yêu cầu → giả định 100 máy → lựa chọn kiến trúc. Lý do
và đánh đổi được giải thích ngay cạnh từng lựa chọn. Sáu trang đầu giữ phần
phân tích cùng hai sơ đồ rút gọn; trang 7–8 bổ sung ảnh kiến trúc tổng thể và
deployment trên A4 ngang để đọc rõ các nhãn. Ảnh được nhúng dạng vector.
PDF đọc độc lập, không yêu cầu mở mã nguồn hoặc tài liệu khác.
Trang đầu có mã sinh viên và họ tên đầy đủ của bốn thành viên Nhóm 8.
[Bản nội dung có thể chỉnh sửa](report/report.md) được lưu cùng PDF; các phần
01–04 dưới đây giữ bản phân tích đầy đủ.

## Mục đích và phạm vi báo cáo

MaintAI là hệ thống hỗ trợ bảo trì thiết bị bằng dữ liệu cảm biến, mô hình học
máy và công cụ tra cứu tài liệu kỹ thuật. Hệ thống giúp kỹ thuật viên nhận biết
máy có dấu hiệu bất thường, ưu tiên xử lý cảnh báo và xem hướng dẫn có nguồn.
Con người quyết định kiểm tra và bảo trì; hệ thống không tự điều khiển máy thật.

Báo cáo giải thích cách nhóm đi từ yêu cầu của đề Chủ đề 3 đến các giả định về
quy mô sử dụng, cấu trúc hệ thống và lý do lựa chọn kiến trúc. Toàn bộ nội dung
cần thiết cho lập luận được trình bày trong năm tài liệu của bộ này. Người đọc
không cần biết cấu trúc mã nguồn, cách tổ chức phần mềm hoặc đọc thêm tài liệu
kỹ thuật nội bộ để hiểu thiết kế.

Giả định ban đầu là **100 máy giả lập, mỗi máy gửi 1 bản ghi mỗi giây**, tương
đương **100 readings/s**. MaintAI được thiết kế theo hướng **scale-ready**:
các thành phần có ranh giới trách nhiệm để tối ưu hoặc tăng năng lực riêng khi
số máy lớn hơn 100. Đây là định hướng thiết kế; năng lực phục vụ một mức tải cụ
thể cần được xác nhận bằng đo đạc trên cấu hình và môi trường tương ứng.

## Cấu trúc và thứ tự đọc

| Phần | Nội dung và câu hỏi chính |
| --- | --- |
| [01 — Yêu cầu hệ thống](01_REQUIREMENTS.md) | Đề bài yêu cầu hệ thống làm gì, ai sử dụng, chất lượng được đánh giá ra sao và phần nào nằm ngoài phạm vi? |
| [02 — Giả định vận hành và cách tính năng lực](02_ASSUMPTIONS_AND_CAPACITY.md) | Giả sử có bao nhiêu máy, tốc độ gửi và số người xem thế nào; từ đó tính tải, lưu trữ, dữ liệu tồn đọng và khả năng mở rộng ra sao? |
| [03 — Kiến trúc hệ thống](03_SYSTEM_ARCHITECTURE.md) | Mỗi thành phần làm gì, dữ liệu đi qua đâu, được lưu ở đâu và người dùng tương tác bằng những giao diện nào? |
| [04 — Biện luận kiến trúc](04_ARCHITECTURE_RATIONALE.md) | Vì sao chọn cấu trúc này, so với backend gộp thì được/mất gì, khi có lỗi hoặc cập nhật phiên bản người dùng bị ảnh hưởng thế nào? |

Mạch lập luận xuyên suốt báo cáo:

```text
Yêu cầu: theo dõi thiết bị, cảnh báo độc lập LLM và truy vết được kết quả.
    ↓
Giả định ban đầu: 100 máy × 1 reading/s = 100 readings/s.
Chạy thử nhỏ: 5 máy × 1 reading/s = 5 readings/s.
Khi có X > 100 máy: tính lại tải và đo điểm gây chậm ở từng thành phần.
    ↓
Kiến trúc: tiếp nhận qua Ingestion, phân phối qua Kafka, xử lý ở Processing,
lưu trạng thái nghiệp vụ ở Business, cập nhật UI và lưu trữ lịch sử riêng.
    ↓
Biện luận: giải thích lợi ích, chi phí vận hành và cách phục hồi của lựa chọn đó.
Kiểm chứng: chạy đúng mức tải rồi đo độ trễ đến màn hình, độ đúng và khả năng phục hồi.
```

Ví dụ, FR-03 yêu cầu cảnh báo vẫn hoạt động khi LLM gặp sự cố. Nhóm vì vậy đặt
luồng tạo cảnh báo và luồng sinh hướng dẫn bảo trì ở hai nhánh khác nhau. Nếu
LLM ngừng trả lời, cảnh báo vẫn có thể được lưu và hiển thị; phần hướng dẫn báo
đang chờ hoặc không khả dụng. Phần 04 giải thích cả lợi ích lẫn chi phí vận hành
của việc tách hai nhánh đó.

## Các thuật ngữ được dùng xuyên suốt

| Thuật ngữ | Ý nghĩa trong báo cáo |
| --- | --- |
| Sensor Simulator | Bộ giả lập tạo và gửi dữ liệu thiết bị để thử hệ thống; không đại diện cho cảm biến đang đo ở nhà máy thật. |
| Reading | Một bản ghi số đo của một máy tại một thời điểm. Bộ giả lập bổ sung định danh và thứ tự để biết dữ liệu thuộc máy nào. |
| Event | Thông điệp trao đổi giữa các thành phần, có thể mang reading, kết quả xử lý hoặc tín hiệu cần xem xét tạo cảnh báo. |
| Machine Learning (ML) | Mô hình học từ dữ liệu. Training là huấn luyện; inference là dùng mô hình đã huấn luyện để dự đoán cho đầu vào mới. |
| Risk policy và alert | Policy là quy tắc dùng kết quả xử lý để xác định rủi ro. Alert là cảnh báo có vòng đời, người tiếp nhận và lịch sử xử lý. |
| RAG và LLM | RAG tìm tài liệu liên quan rồi cung cấp bằng chứng cho LLM, tức mô hình ngôn ngữ lớn, để sinh câu trả lời có nguồn. |
| Maintenance Brief | Bản tóm tắt thông tin bảo trì và danh sách việc nên kiểm tra, có liên kết tới tài liệu hỗ trợ. |
| Data Drift | Sự thay đổi phân bố dữ liệu so với một bộ tham chiếu. Drift cần được đánh giá, không tự chứng minh model đã dự đoán sai. |
| Kafka | Hệ thống truyền thông điệp cho phép lưu event trong thời gian hữu hạn và để nhiều nhóm xử lý đọc theo tiến độ riêng. |
| Throughput và latency | Throughput là số bản ghi xử lý mỗi giây; latency là thời gian một bản ghi đi qua một phần hoặc toàn bộ hệ thống. |
| Baseline | Mốc để so sánh. Baseline tải của dự án là 100 máy; baseline model là mô hình đơn giản dùng so sánh chất lượng dự đoán. |
| Owner | Thành phần chịu trách nhiệm quản lý và thay đổi một loại dữ liệu. Processing quản lý lịch sử xử lý; Business quản lý vòng đời cảnh báo. |
| Artifact và pin phiên bản | Artifact là gói model, tài liệu hoặc tệp kết quả cần lưu. Pin nghĩa chọn chính xác phiên bản/ID/mã băm để một lần chạy không tự đổi sang bản mới. |
| REST và SSE | REST phục vụ các request đọc/thao tác qua HTTP; SSE là kết nối để server đẩy cập nhật một chiều tới trình duyệt. |
| Scale-ready | Kiến trúc có ranh giới và cơ chế để tăng năng lực khi tải tăng; không có nghĩa đã được kiểm chứng ở mọi quy mô. |

Phần 02 giải thích thêm partition, consumer, task và các biến tính tải. Phần 03
giải thích transaction, Inbox, Outbox, cursor và cách chia runtime. Các thuật
ngữ được định nghĩa ngay trước khi dùng cho lập luận chi tiết.

## Cách phân biệt yêu cầu, lựa chọn và kết quả

| Loại thông tin | Cách hiểu | Ví dụ trong MaintAI |
| --- | --- | --- |
| Yêu cầu đề bài | Hành vi hoặc tiêu chí chất lượng được đối chiếu từ đề Chủ đề 3. | Cảnh báo phải hoạt động độc lập với LLM, theo FR-03. |
| Quyết định thiết kế | Cách nhóm chọn để đáp ứng yêu cầu. | Dùng Kafka, tách Business API và Consumer, có bốn UI theo mục đích. |
| Giả định phân tích | Giá trị để ước lượng trước khi có số đo; có thể điều chỉnh sau thử nghiệm. | 100 máy ban đầu, raw payload trung bình 1 KiB và dùng 70% năng lực lý thuyết khi lập kế hoạch. |
| Kết quả kiểm chứng | Kết quả gắn với bài kiểm thử, cấu hình, môi trường và lần chạy cụ thể. | Độ trễ đến browser phải được đo trên ứng dụng tích hợp; tính toán năng lực chưa thay thế phép đo đó. |

Nguồn yêu cầu là **CHỦ ĐỀ 3_Requirement.docx**, được định danh và đối chiếu trong
phần 01. Đề bài không ấn định số máy, Kafka, 12 partition, số consumer hay AWS;
đây là những giả định và lựa chọn mà nhóm cần biện luận.

Báo cáo tập trung vào thiết kế và kế hoạch đánh giá. Những phần đã triển khai
riêng và những phần còn cần tích hợp được nêu tại phần 03, mục 8. Việc có sơ đồ
hoặc kết quả kiểm thử một thư viện chưa chứng minh toàn hệ thống đạt chất lượng
ML, độ trễ hay khả năng chịu lỗi. Quy trình mở rộng sau baseline nằm ở
[phần 02, mục 11](02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100).
