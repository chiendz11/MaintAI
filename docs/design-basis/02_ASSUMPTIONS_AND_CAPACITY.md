# 02 — Giả định vận hành, tải và cách tính năng lực

**Phiên bản:** 1.2.0 — 23/10/2026. Nguồn yêu cầu:
[phần 01 — Yêu cầu hệ thống](01_REQUIREMENTS.md).

Tài liệu này giải thích cách đi từ “có 100 máy” đến các đại lượng cần thiết kế:
bao nhiêu bản ghi mỗi giây, bao nhiêu việc được xử lý cùng lúc, cần lưu bao nhiêu
dữ liệu và mất bao lâu để xử lý phần tồn đọng. Các giá trị dưới đây được phân
biệt rõ giữa giả định phân tích, cấu hình chạy thử và kết quả tính toán. Người
đọc có thể theo dõi các phép tính ngay trong phần này; chúng chưa phải số đo
hiệu năng hoặc bằng chứng về năng lực của một môi trường đã triển khai.

## 1. Hệ thống được giả sử dùng trong hoàn cảnh nào?

Một nhóm bảo trì theo dõi nhiều thiết bị giả lập. Mỗi máy gửi các bản ghi gồm
sáu đặc trưng AI4I. Người dùng cần xem số đo mới nhất, prediction, trạng thái xử
lý, cảnh báo, lịch sử và hướng dẫn có nguồn. Backend vẫn xử lý và lưu dữ liệu
đầy đủ theo quy ước xử lý và lưu trữ; trình duyệt có thể giới hạn nhịp cập nhật số đo để dễ xem.

AI4I không phải bộ lịch sử đo theo thời gian của nhiều máy vật lý. Simulator
bổ sung `machine_id`, thời điểm và diễn biến scenario để tạo các luồng giả lập.
Vì vậy, 100 máy trong tài liệu là 100 luồng có định danh riêng do Simulator tạo.
Các loại sản phẩm L/M/H là đặc trưng của dữ liệu, không phải ba định danh máy.

Giả định ban đầu là **100 máy, mỗi máy 1 reading/s**, tương đương **100 readings/s**.
Hệ thống được thiết kế scale-ready để mở rộng sau mốc này bằng cách tính và đo
lại năng lực. Không có giới hạn cứng 100 máy được suy ra từ giả định thiết kế.

## 2. Các profile tải được dùng để biện luận

Một **profile tải** mô tả điều kiện của lần đánh giá: bao nhiêu máy đang chạy,
mỗi máy gửi nhanh thế nào và dữ liệu có đến dồn hay không. Phải ghi profile cùng
kết quả để tránh lấy một lần chạy nhỏ làm bằng chứng cho quy mô lớn hơn.

| ID | Điều kiện | Tổng tải | Mục đích |
| --- | --- | --- | --- |
| LOAD-N100 | 100 máy, mỗi máy gửi 1 reading/s. | Trung bình 100 readings/s. | Baseline vận hành ban đầu đã chốt; cần đo năng lực và độ trễ thực tế. |
| LOAD-D5 | 5 máy, mỗi máy gửi 1 reading/s. | Trung bình 5 readings/s. | Chạy thử nhỏ để kiểm tra các thành phần đã nối đúng; dùng cấu hình khởi đầu của bộ giả lập. |
| LOAD-BURST100 | Cả 100 máy gửi trong cùng khoảng 100 ms của mỗi chu kỳ 1 giây. | Trung bình 100 readings/s; trong 0,1 giây nhận dữ liệu, tốc độ tức thời xấp xỉ 1.000 readings/s. | Kiểm tra hàng đợi khi dữ liệu đến dồn, dù tải trung bình không đổi. |
| LOAD-X | X > 100 máy; tốc độ mỗi máy là r hoặc khác nhau theo từng máy. | R = X × r khi cùng tốc độ; R = Σr_i khi khác tốc độ. | Tính và kiểm chứng khả năng mở rộng theo mục 11. |

LOAD-BURST100 vẫn chỉ có 100 máy. Con số 1.000 readings/s mô tả tốc độ tức thời
trong cửa sổ nhận dữ liệu, không phải một baseline mới gồm 1.000 máy.

Các giả định khác được dùng xuyên suốt phép tính:

| ID | Giả định hoặc mốc cấu hình | Cách sử dụng |
| --- | --- | --- |
| A-PAYLOAD | Một raw envelope trung bình 1 KiB; một kết quả xử lý cuối cùng trung bình 2 KiB. | Ước lượng dung lượng trước khi đo kích thước thông điệp thực, metadata và mức nén. |
| A-VALID | Khi tính năng lực ML, giả sử 100% reading hợp lệ. | Tránh đánh giá thiếu tải inference chỉ vì nhiều dữ liệu thử nghiệm bị loại ở DQ. Các tình huống dữ liệu lỗi được chạy riêng. |
| A-UI | Có 10 trình duyệt đồng thời; mỗi người xem tối đa 50 máy hoặc trang chi tiết một máy. | Ước lượng dữ liệu gửi tới UI. Cách trình duyệt đăng ký phạm vi cập nhật cần được chốt khi thiết kế API. |
| A-UI-RATE | Số đo latest được hiển thị khoảng 1 lần/giây/máy. | Có thể gộp số đo trung gian; không được gộp mất các thay đổi vòng đời cảnh báo. |
| A-RAG | Mỗi người yêu cầu trung bình 1 brief/phút trong điều kiện thông thường. | Với 10 người là 10/60 ≈ 0,167 yêu cầu/giây. Cần đánh giá riêng khi yêu cầu đến dồn. |
| A-UTIL | Lập kế hoạch ở 70% năng lực lý thuyết, để lại 30% dự phòng. | Đây là hệ số tính sơ bộ, không bảo đảm p95 hoặc đủ dự phòng cho mọi sự cố. |
| A-REPLICAS | Bắt đầu phân tích với 1 consumer member/group, 8 active keys/Processing và 12 partition/topic. | Một replica là mức khởi đầu để phân tích local. Ví dụ 3 member chỉ minh họa cách chia tải; cần đo trước khi chọn cấu hình chạy. |
| A-RETENTION | Thiết kế Kafka chọn thời gian lưu 7 ngày; chính sách lưu giữ tổng thể cho production chưa chốt. | Mốc dữ liệu nóng 30 ngày/lưu trữ dài hạn 365 ngày chỉ là phương án tham khảo; chưa phải chính sách vận hành được duyệt. |

## 3. Từ cấu hình Simulator ra tốc độ đầu vào

### 3.1. Tốc độ danh nghĩa và số bản ghi

Gọi N là số máy, `SIM_INTERVAL_MS` là khoảng cách gửi tính bằng mili giây và
`SIM_SPEED` là hệ số tốc độ. Khi tất cả máy ở trạng thái `RUNNING` và nguồn phát
đủ nhanh, tốc độ mục tiêu được tính như sau:

```text
r = 1000 × SIM_SPEED / SIM_INTERVAL_MS       [reading/giây/máy]
R = N × r                                  [reading/giây]
Số reading trong D giây = R × D
```

Hệ số 1000 đổi mili giây sang giây. Với profile LOAD-N100:

```text
SIM_MACHINE_COUNT = 100
SIM_INTERVAL_MS   = 1000
SIM_SPEED         = 1

Mỗi máy: 1000 × 1 / 1000 = 1 reading/s
Toàn hệ thống: 100 × 1 = 100 readings/s
Trong 5 phút: 100 × 300 = 30.000 readings
```

Ba tham số trên lần lượt là số máy, chu kỳ phát tính bằng mili giây và hệ số tốc
độ. Chúng được nêu đầy đủ để xác định profile baseline, không yêu cầu người đọc
mở mã nguồn để hiểu cách tính. Profile khởi đầu của bộ giả lập vẫn là 5 máy,
tương đương 1.500 readings trong 300 giây khi tất cả máy chạy.

Môi trường local khởi động ở trạng thái `PAUSED`, tức chưa phát reading mới;
các tốc độ trên chỉ áp dụng sau khi máy được cho chạy. Sensor có lịch sinh và
trạng thái riêng từng máy, hàng đợi lưu trên đĩa (durable spool) và nhóm luồng
gửi HTTP (sender pool). Việc sinh reading và gửi lại do lỗi được tách ra để một
request chậm không làm dừng lịch sinh dữ liệu của toàn bộ máy.

### 3.2. Vì sao cấu hình 100/s chưa chứng minh nguồn thực sự gửi được 100/s?

Tốc độ thực còn phụ thuộc SQLite, CPU, số sender, thời gian chờ ACK, retry và
quota spool. Cần phân biệt tốc độ mong muốn với số bản ghi thực sự được sinh,
được tiếp nhận duy nhất và được xử lý xong. Cũng cần đếm bản trùng, bản bị điều
tiết và số còn tồn đọng.

Ví dụ, cấu hình đặt 100/s nhưng Sensor chỉ tạo được 40/s thì Kafka có thể không
có lag. Điều đó chỉ cho thấy downstream theo kịp 40/s. Khi đánh giá baseline
hoặc mở rộng lên X máy, trước hết phải chứng minh nguồn đã cung cấp đủ tải.

### 3.3. Ước lượng số luồng gửi HTTP

Cấu hình phát thử hiện dùng 4 sender workers và chỉ có một request đang gửi tại
một thời điểm cho mỗi máy. Nếu một request giữ sender trong thời gian trung bình
A giây, W sender có năng lực lý thuyết gần bằng W/A request mỗi giây.

```text
Năng lực ACK lý thuyết ≈ W / A
Số sender cần tính sơ bộ ≈ ceil(R × A / u)

W = 4; A = 10 ms = 0,010 giây
→ năng lực lý thuyết ≈ 4 / 0,010 = 400 requests/s

R = 100/s; u = 0,7; W = 4
→ để nằm trong mức sử dụng 70%, cần A ≤ 0,7 × 4 / 100
→ thời gian ACK trung bình cần ≤ 28 ms
```

`ceil` có nghĩa làm tròn lên thành số nguyên. Các phép tính chưa kể toàn bộ chi
phí lập lịch và I/O. Tăng sender chỉ hữu ích khi SQLite, connection pool,
Ingestion và Kafka còn đủ năng lực; cần đo từng phần trước khi điều chỉnh.

## 4. Mười hai partition phân chia 100 máy như thế nào?

**Partition** là một luồng có thứ tự trong Kafka. **Consumer member** là một
tiến trình tham gia đọc trong một group. Một member có thể nhận nhiều partition;
một partition có thể chứa reading của nhiều máy. Số task Processing chạy cùng
lúc bên trong member lại do giới hạn concurrency quyết định.

Giả sử key `machine_id` được phân bố tương đối đều và cách phân vùng giữ nguyên:

```text
100 máy / 12 partition ≈ 8,33 máy/partition
100 readings/s / 12 ≈ 8,33 readings/s/partition

1 member trong group: nhận cả 12 partition, tổng khoảng 100 readings/s.
3 member trong group: mỗi member nhận 4 partition, khoảng 33,33 readings/s.
```

Đây là trung bình, không phải bảo đảm partition nào cũng có cùng số máy. Hàm
hash, máy phát nhanh hơn và việc phân công partition có thể tạo lệch tải.
Processing, Storage raw và Business telemetry là ba group độc lập: mỗi group
đọc toàn bộ `sensor.readings` theo tiến độ riêng, thay vì mỗi group chỉ đọc một
phần ba dữ liệu.

**Consumer lag** biểu thị khoảng cách tiến độ đọc đã commit so với dữ liệu
hiện có trong Kafka. **Tuổi record chưa xử lý** cho biết dữ liệu cũ nhất đang
chờ đã nằm lại bao lâu. Cần xem cả hai: cùng một số record tồn nhưng nếu chúng
đã chờ nhiều giây thì ảnh hưởng độ mới của UI khác với một burst vừa đến.

Kafka vẫn ghi record theo offset trong từng partition. Tuy nhiên, Processing
có thể xử lý song song các máy khác nhau. Ví dụ, offset 100 thuộc máy A đang
chờ ML, còn offset 101 thuộc máy B đã hoàn tất. B không cần chờ A để tính toán,
nhưng mốc commit của partition không được đi qua record 100 chưa hoàn thành.
Khi A xong bền vững, bộ điều phối mới đưa mốc commit qua dãy record đã hoàn tất
liên tiếp. Cách này tránh bỏ sót A khi tiến trình khởi động lại.

Trong cùng một máy, reading tiếp theo phải chờ reading trước hoàn tất bền vững.
Quy tắc này giữ thứ tự trạng thái máy; việc tăng số task không cho phép các
reading của cùng máy tự vượt nhau.

Với burst 100 reading thuộc 100 máy, nếu 1 Processing member chạy tối đa 8 máy
cùng lúc và mỗi task mất đúng 20 ms, thời gian xử lý batch lý tưởng là:

```text
ceil(100 / 8) × 20 ms = 13 lượt × 20 ms = 260 ms
```

Nếu chia tương đối đều cho 3 member, member nhận 34 reading cần khoảng
`ceil(34 / 8) × 20 ms = 100 ms`. Các record đến sau vẫn có thời gian chờ. Hai
con số này chưa tính lệch tải, khóa DB, mạng, retry và những thành phần phía sau;
chúng minh họa cơ chế xếp hàng, chưa phải latency đo được.

## 5. Năng lực Processing và những nhánh cần tính riêng

### 5.1. Cách đọc công thức Processing

Dùng các ký hiệu sau:

| Ký hiệu | Ý nghĩa | Đơn vị |
| --- | --- | --- |
| R | Tổng tốc độ đầu vào cần đáp ứng. | Reading/giây |
| n | Số replica Processing có thể nhận công việc. | Tiến trình |
| c | Số máy có task đang chạy đồng thời tối đa trên một replica. | Active keys/replica |
| T | Thời gian trung bình một task giữ slot, từ chuẩn bị đến hoàn tất bền vững. | Giây/task |
| u | Hệ số sử dụng năng lực để lập kế hoạch; ở đây lấy 0,7. | Tỷ lệ |
| μ | Năng lực xử lý lý thuyết của toàn bộ replica đang xét. | Reading/giây |

Một slot hoàn thành khoảng 1/T task mỗi giây. Do có tổng n × c slot nên:

```text
μ ≈ n × c / T
R_planning ≈ u × n × c / T
n_cần ≈ ceil(R × T / (u × c))
```

Ví dụ, 1 replica có 8 active keys và T = 20 ms = 0,020 giây thì μ ≈ 400/s.
Lập kế hoạch ở mức 70% cho R_planning ≈ 280/s. Mức 100/s thấp hơn con số đó,
nhưng vẫn phải đo các dependency và độ trễ thực trước khi kết luận đủ năng lực.

Giữ `c = 8`, `u = 0,7` và tính cho baseline 100/s:

| T trung bình giả định | μ với 1 replica | Mức lập kế hoạch 70% với 1 replica | Số replica tính sơ bộ cho 100/s |
| --- | --- | --- | --- |
| 10 ms | 800/s | 560/s | 1 |
| 20 ms | 400/s | 280/s | 1 |
| 50 ms | 160/s | 112/s | 1 |
| 100 ms | 80/s | 56/s | 2 |

T gồm cả thời gian chờ đọc/ghi đĩa hoặc mạng (I/O) và transaction DB, không chỉ
thời gian tính toán inference trên CPU. Khi T = 50 ms, một replica chỉ còn ít
dự phòng so với 100/s; burst hoặc lệch tải có thể
làm hàng đợi tăng. Thời gian trung bình cũng không phản ánh những request rất
chậm ở phần đuôi phân bố. Các giới hạn ML/DB, thứ tự theo máy và bộ nhớ hàng đợi
có thể khiến năng lực thực thấp hơn phép tính. Quy tắc cùng máy xử lý tuần tự
và chỉ xác nhận tiến độ qua các record đã hoàn tất được giải thích tại mục 4;
phần 03 trình bày cách lưu kết quả và phục hồi khi Worker mất quyền xử lý.

### 5.2. Processing đủ nhanh chưa có nghĩa toàn bộ pipeline đủ nhanh

| Thành phần | Tải baseline, chưa tính retry | Nội dung cần đo |
| --- | --- | --- |
| Ingestion | 100 HTTP reading/s, cộng lưu lượng bị từ chối. | Thời gian ACK, chi phí auth và năng lực Kafka producer. |
| Processing/ML | Tối đa 100 inference/s khi mọi reading VALID. | Thời gian task, tổng năng lực ML và giới hạn connection pool. |
| Processing DB/Outbox | Một outcome bền vững cho mỗi reading được nhận; thêm signal khi policy yêu cầu. | Chi phí history/inbox/cursor/outbox, khóa DB, chỉ mục (index), nhật ký ghi DB (WAL) và tuổi Outbox chờ gửi. |
| Outbox Relay | Ít nhất 100 result publishes/s, cộng alert signals. | Publish/ACK và retry; mỗi runner hiện gửi tuần tự. |
| Business telemetry | Khoảng 100 cập nhật projection/s. | Thời gian xử lý callback, transaction và commit offset. |
| Business prediction | Tới 100 outcomes/s. | Ghi trạng thái mới nhất, chống trùng và kiểm tra thứ tự. |
| Business alert | Phụ thuộc policy; khi gây tải có thể gần tốc độ signal đầu vào. | Chống trùng, vòng đời, audit và khả năng xử lý alert storm. |
| Storage raw | Khoảng 100 raw receipts/s, cộng rejected receipts. | Thời gian lưu object/manifest, số lần gọi S3 và tuổi dữ liệu chờ archive. |

Business và Storage vẫn dùng runner mặc định xử lý tuần tự. Với một member nhận
100 record/s và hệ số sử dụng 70%, cả chu kỳ xử lý trung bình chỉ được khoảng
`0,7 / 100 = 0,007 giây = 7 ms/record`. Nếu dùng 3 member chia đều tải, mỗi member nhận khoảng
33,33/s và có budget khoảng 21 ms/record. SQL hoặc S3 có nhiều lượt gọi mạng
có thể làm các nhánh này trở thành điểm gây chậm, dù Processing còn dư năng lực.

Storage đang dùng microbatch 1: mỗi receipt được lưu thành một data object cùng
manifest. Với 100 receipt mới/s, riêng bước ghi đã cần ít nhất 200 PUT/s, chưa
kể GET/HEAD và retry. Không được áp công thức 8 active keys của Worker cho Storage
hoặc Relay; muốn thêm batching/concurrency cần review đúng semantics và tests.

Một signal không nhất thiết tạo một alert mới vì Business còn áp dụng chống
trùng, cooldown và vòng đời. Tuy vậy, Business vẫn phải nhận và xử lý signal.
Không thể tính tải chỉ từ một tỷ lệ hỏng thấp được đoán trước. Tùy chọn Storage
lưu thêm outcome cũng không thay phần lịch sử/export thuộc Processing.

## 6. Tải UI và Copilot khác tải cảm biến

Với 10 người, mỗi người đang xem 50 máy, nhịp hiển thị 1 lần/giây/máy tạo tối đa
`10 × 50 × 1 = 500` lượt cập nhật đối tượng mỗi giây. Một SSE message có thể mang
nhiều cập nhật, nên 500 lượt này không đồng nghĩa 500 message hoặc HTTP request.
Một trình duyệt dùng một kết nối SSE cho phạm vi dữ liệu được phép xem.

Nếu mỗi cập nhật có kích thước 1 KiB, tổng dữ liệu khoảng 0,49 MiB/s, chưa tính
protocol overhead và các loại cập nhật khác. Nếu phát cả 100 máy cho cả 10 người,
con số tăng thành khoảng 0,98 MiB/s; khi X tăng, cách broadcast đó tăng tải theo X.
Phân trang, giới hạn phạm vi xem và gộp số đo latest giúp giảm tải truyền/render.
Chúng không giảm số reading mà ML, DB hoặc archive phải xử lý, và không được làm
mất sự kiện vòng đời cảnh báo. Cách đăng ký, lọc và gom cập nhật cần được chốt
trong thiết kế API trước khi triển khai, để server và UI hiểu cùng một quy tắc.

RAG chạy khi có yêu cầu brief, không chạy cho từng reading. Ở mức giả định 10
người × 1 brief/phút, tải trung bình chỉ khoảng 0,167 request/s. Khi nhiều yêu cầu
đến cùng lúc, cần hàng đợi bền vững, thời hạn chờ và chống tạo tác vụ trùng. Cảnh
báo được tạo trước, không chờ LLM trả lời.

Khả năng con người xử lý cảnh báo là một giới hạn khác. ML xử lý được 100
readings/s không chứng minh 10 kỹ thuật viên có thể xử lý 100 alert/s. Cần đo
số alert thực sự phát sinh, thời gian đánh giá và mức quá tải của người dùng.

## 7. Dung lượng và lưu trữ

Quy ước `1 KiB = 1.024 bytes`, `1 MiB = 2^20 bytes`, `1 GiB = 2^30 bytes`.
Với tốc độ R, số reading một ngày bằng `R × 86.400`. Nhân tiếp với kích thước
trung bình sẽ ra dung lượng payload trước nén.

| Profile chạy liên tục 24 giờ | Readings/ngày | Raw ở mức 1 KiB/reading | Outcome ở mức 2 KiB/reading |
| --- | --- | --- | --- |
| LOAD-D5 | 432.000 | Khoảng 0,412 GiB/ngày | Khoảng 0,824 GiB/ngày |
| LOAD-N100 | 8.640.000 | Khoảng 8,24 GiB/ngày | Khoảng 16,48 GiB/ngày |

Phiên baseline 5 phút tạo mục tiêu 30.000 reading, tương đương khoảng 29,30 MiB
raw. Chạy thử 5 máy trong cùng thời gian tạo 1.500 reading, khoảng 1,46 MiB raw.
Các số trên chưa gồm metadata, manifest, retry, DB indexes, WAL hoặc bản sao Kafka.

Nếu baseline 100/s chạy 24 giờ/ngày, hai topic raw/outcome giữ 7 ngày và có hệ số
sao chép RF3, riêng payload ước lượng là:

```text
100 × 86.400 × 7 × (1 + 2) KiB × 3 bản sao ≈ 519,10 GiB
```

Đây là ước lượng trước nén, chưa phải dung lượng đĩa đo được. Với X máy cùng tốc
độ 1/s, nhân ước lượng baseline với X/100; nếu tốc độ khác nhau thì dùng R/100.

Các PVC local yêu cầu Kafka 8 GiB, PostgreSQL 8 GiB và MinIO 12 GiB chỉ là cấu
hình khởi đầu. Chúng chưa chứng minh đủ dung lượng hoặc IOPS cho 100/s dài ngày,
càng không mặc nhiên đáp ứng X > 100. Số object nhỏ do microbatch 1 cũng cần được
đánh giá trước khi dùng cho tải lớn.

Retention là thời gian giữ dữ liệu trước khi được phép dọn dẹp. Chính sách vận
hành thực tế cần chốt theo dung lượng và ngân sách. Phương án tham khảo là giữ
dữ liệu nóng 30 ngày và lưu dài hạn 365 ngày; hai mốc này chưa được duyệt cho
production và không dùng thay mốc Kafka 7 ngày trong phép tính phía trên.

Trước khi xóa lịch sử DB, phải xác minh từng bản ghi đã có trong archive đầy
đủ, đọc lại được và không đang bị đánh dấu giữ lại cho điều tra hoặc truy vết
(pin/evidence hold). Không xóa dữ liệu chỉ vì đủ tuổi khi archive còn chờ ghi
hoặc đang gửi lại do lỗi. Đây là điều kiện bảo vệ dữ liệu, không chỉ là một
lệnh dọn bảng theo thời gian.

## 8. Sự cố, dữ liệu tồn đọng và giới hạn spool

**Backlog** là lượng dữ liệu đã đến nhưng chưa xử lý xong. Nếu một nhánh dừng
trong S giây trong khi nguồn tiếp tục gửi R reading/s, lượng tồn đọng tăng gần
bằng R × S. Khi phục hồi, nhánh đó vừa xử lý tải mới vừa xử lý phần tồn cũ.

```text
Backlog ≈ R × thời_gian_dừng
Thời_gian_bắt_kịp ≈ backlog / (μ_phục_hồi − R), khi μ_phục_hồi > R
```

Ví dụ Processing dừng 60 giây ở mức 100/s sẽ tồn khoảng 6.000 reading. Nếu sau
khi chạy lại nó xử lý được tổng cộng 150/s, phần dành cho backlog chỉ còn
`150 − 100 = 50/s`. Thời gian bắt kịp lý tưởng là `6.000 / 50 = 120 giây`.
Nếu chỉ xử lý được đúng 100/s, backlog không giảm khi nguồn vẫn chạy.

Dữ liệu còn được giữ bền vững không có nghĩa độ trễ vẫn đạt 2 giây. Trong giai
đoạn chậm, UI phải thể hiện dữ liệu cũ hoặc đang xử lý trễ thay vì coi snapshot
đang hiển thị là trạng thái hiện thời.

Spool của Sensor trong cấu hình phát thử có ba giới hạn: tối đa 10.000 record,
32.000.000 bytes và tuổi dữ liệu tối đa (TTL) 86.400 giây. Nếu chỉ xét số record
và tạm bỏ qua giới hạn bytes:

```text
Ở 5/s:   10.000 / 5   = 2.000 giây dữ liệu
Ở 100/s: 10.000 / 100 = 100 giây dữ liệu
Ở X máy cùng tốc độ r: thời gian đệm ≈ 10.000 / (X × r) giây
```

Giới hạn nào chạm trước sẽ quyết định thời gian đệm thực. TTL một ngày quy định
tuổi dữ liệu, không bảo đảm spool chứa được một ngày traffic. Khi quota gây điều
tiết nguồn, cần ghi nhận số bị chặn và lượng tồn; không tính các reading chưa
được tạo hoặc tiếp nhận như dữ liệu đã được bảo vệ.

## 9. Mục tiêu cảnh báo 2 giây phải đo tới đâu?

QR-09 đặt mục tiêu p95 ≤ 2 giây từ lúc nhận dữ liệu hợp lệ đến khi người dùng
thấy alert. Thời gian tạo Maintenance Brief không nằm trong khoảng này. Để tìm
điểm gây chậm, có thể dùng phân bổ thời gian sơ bộ sau:

| Đoạn đường đi | Budget phân tích |
| --- | --- |
| Nhận HTTP, xác thực và chờ Kafka ACK | 150 ms |
| Chờ Kafka và chờ Processing nhận task | 250 ms |
| DQ, ML, risk và ghi kết quả bền vững | 500 ms |
| Relay gửi event và Business ghi alert | 250 ms |
| SSE đưa dữ liệu đến trình duyệt và UI hiển thị | 400 ms |
| Phần dự phòng | 450 ms |
| **Tổng budget** | **2.000 ms** |

Đây là cách chia budget để phân tích, không phải cộng các p95 của từng đoạn để
suy ra p95 toàn luồng. Mỗi mẫu đo phải theo cùng một event từ đầu đến cuối:
`t0` là lúc Ingestion nhận request của reading được biết là hợp lệ trong bộ test;
`t1` là lúc trình duyệt thực sự hiển thị alert tương ứng.

Một phép đo dừng khi Business commit alert vào DB hoặc API có thể đọc alert
chỉ phản ánh độ trễ phía server. Nó chưa bao gồm thời gian truyền SSE và hiển
thị trên trình duyệt, nên chưa đủ để nghiệm thu QR-09. Cần phép đo toàn luồng
với ID liên kết đúng reading và alert, cùng cách xử lý đồng hồ giữa các máy.

Nguồn timestamp tiếp nhận được truyền tới bước tạo alert và phép đo browser
vẫn cần được chốt khi tích hợp. Không lấy thời điểm giả lập của Sensor thay cho
lúc Ingestion thực sự nhận dữ liệu; cũng không dùng prediction latest của event
khác để ghép vào cùng mẫu đo. Nếu làm vậy, con số latency không còn mô tả đúng
hành trình của reading đang đánh giá.

Tập đo độ trễ cảnh báo gồm các reading hợp lệ mà model/policy thực sự xác định
cần cảnh báo; các trường hợp không cần alert được kiểm tra outcome riêng.
Timeout hoặc thiếu alert phải được báo như lỗi cùng tỷ lệ thành công và mức
bao phủ. Không loại các mẫu chậm rồi chỉ báo p95 của phần còn lại. Báo cáo phải
ghi profile, thời lượng, số mẫu, cách xử lý đồng hồ và sự cố trong lần chạy.

## 10. Khi nào chấp nhận hoặc điều chỉnh các giả định?

Đánh giá theo các bước nhỏ: chạy thử 5 máy, tăng qua 50 máy, rồi đo baseline
100 máy ở 1 reading/s/máy. Các bước X > 100 tiếp tục theo mục 11. Chốt thời lượng
chạy trên môi trường chung và lưu những nhóm số liệu sau:

- Tốc độ mục tiêu, số bản ghi thực sự sinh ra, unique accepted và processed;
  số retry, duplicate và lượng dữ liệu đang tồn.
- Kafka lag và tuổi record cũ nhất, số task chờ, mốc offset có thể commit,
  thời gian chờ Outbox và archive.
- Thời gian ML/DB/S3, mức dùng connection pool, CPU, RAM, dung lượng đĩa và I/O.
- Độ trễ đến trình duyệt, tỷ lệ alert thực sự hiển thị và thời gian xử lý hết
  backlog sau sự cố.

Cần thử cả dữ liệu đến đều, đến dồn, lệch tải giữa máy và tình huống lỗi. Nếu
nguồn không tạo đủ tải thì sửa nguồn; nếu Storage chậm thì tối ưu Storage.
Tăng partition chỉ được xem xét khi số đo cho thấy nhu cầu phân công hoặc năng
lực partition, kèm kế hoạch đổi mapping giữ đúng thứ tự. Số máy đơn thuần không
phải điều kiện tự động tách thêm service.

Demo 5 phút cũng chưa đủ cho mọi scenario. Ramp degradation/recovery hiện dài
300 readings/máy, tức khoảng 5 phút cho mỗi ramp ở 1 Hz. Có thể chia phiên demo
hoặc thiết kế profile ngắn hơn rồi kiểm tra khả năng tái hiện. Muốn chứng minh
scenario có tác dụng, phải quan sát input, prediction, policy và alert thực tế;
tên scenario không tự đảm bảo hệ thống sẽ trả một mức rủi ro cố định.

## 11. Scale-ready: tính và tối ưu latency khi X>100

Scale-ready ở MaintAI có nghĩa Ingestion, Worker, ML, Relay, Storage, Business
Consumer/API và Read API có ranh giới để điều chỉnh tài nguyên riêng. Kafka giữ
tiến độ theo group; Processing giữ thứ tự từng máy; cơ chế lưu bền vững và
projection giúp thay đổi số replica mà vẫn có cách bảo vệ tính đúng của dữ liệu.
100 máy là điểm bắt đầu tính toán, không phải trần của thiết kế.

### 11.1. Quy trình cho mỗi mức tải mới

1. **Mô tả tải cần phục vụ.** Ghi X, tốc độ từng máy, tỷ lệ VALID, dữ liệu đến
   dồn, lệch tải, retry, người xem và lượng history/brief/export. Tính R = X × r
   hoặc R = Σr_i. Mục tiêu alert tới UI vẫn là p95 ≤ 2 giây, không gồm LLM brief.
2. **Đo để tìm thành phần làm tăng thời gian chờ.** Đối chiếu tốc độ nguồn với
   lag, tuổi record, hàng đợi, ML/DB pool, Outbox, archive và browser render.
   Lag thấp chỉ có ý nghĩa khi nguồn đã phát đủ tải; UI vẫn có thể chậm sau khi
   consumer đã xử lý xong.
3. **Tính năng lực riêng của từng nhánh.** Với Worker, dùng
   `n_cần ≈ ceil(R × T / (u × c))` hoặc
   `c_cần ≈ ceil(R × T / (u × n))`. Với runner tuần tự, dùng
   `n_cần ≈ ceil(R_stage × T_cycle / u)`, trong đó T_cycle tính bằng giây.
   ML có thể nhận tới R inference/s; Storage, Relay và Business có tải riêng.
4. **Tối ưu đúng điểm gây chậm.** Có thể giảm T bằng cách nạp sẵn model và
   preprocessing, tối ưu SQL/index, rút ngắn transaction hoặc điều chỉnh pool.
   Nếu vẫn thiếu năng lực, tăng replica/concurrency phù hợp với dependency.
   Ở UI, dùng phạm vi xem, phân trang và gộp telemetry để giảm truyền/render.
   Các thay đổi không được bỏ reading, lịch sử hoặc alert lifecycle để làm đẹp số đo.
5. **Chạy lại dưới tải và khi có lỗi.** Đo throughput duy nhất, độ trễ toàn luồng,
   tỷ lệ thành công, khả năng các máy được phục vụ công bằng, bộ nhớ và backlog
   recovery. Chỉ xác nhận mức X mới khi cấu hình thực có đủ tài nguyên và bằng
   chứng. Lặp lại nếu tốc độ gửi, model hoặc cơ cấu request thay đổi.

Các công thức là bước chuẩn bị trước thử nghiệm. Thêm Worker có thể làm ML hoặc
DB tranh tài nguyên nhiều hơn; khi đó độ trễ còn tăng thay vì giảm. Giới hạn
concurrency cần được cân với connection pool và năng lực thực của dependency.

### 11.2. Ví dụ mở rộng từ baseline

Giả sử mọi máy gửi 1 reading/s, thời gian task trung bình T = 20 ms, c = 8 và
u = 0,7. Đây là cùng giả định đã dùng ở mục 5:

| Số máy X | Tổng tải R | Replica Processing tính sơ bộ | Việc cần kiểm tra cùng |
| --- | --- | --- | --- |
| 100 | 100/s | 1 | Xác nhận baseline, burst và các nhánh nguồn/ML/DB/Storage/UI. |
| 200 | 200/s | 1 | Dư địa giảm; lệch tải hoặc request chậm có thể đòi tối ưu sớm hơn. |
| 500 | 500/s | 2 | ML, DB pool, Relay, Business và Storage phải theo kịp. |
| 1.000 | 1.000/s | 4 | Đây là ví dụ tương lai sau baseline 100, chưa phải mức đã đạt hoặc yêu cầu ban đầu. |

Ví dụ ở 500 máy: `ceil(500 × 0,020 / (0,7 × 8)) = ceil(1,786) = 2 replica`.
Nếu T tăng lên 50 ms, 100 máy vẫn tính sơ bộ 1 replica, nhưng 500 máy cần 5 và
1.000 máy cần 9. Cùng số máy mà model hoặc DB chậm hơn thì nhu cầu tài nguyên
khác đi; vì thế không dùng một ngưỡng số máy cố định để quyết định scale.

Với 12 partition, tối đa 12 member trong một group có thể đồng thời được giao
partition. Thêm member vượt số partition không tự tăng năng lực đọc. Nếu cần đổi
số partition, phải xem xét quá trình chuyển mapping và bảo toàn thứ tự từ nguồn;
không tăng partition theo từng máy mới.

### 11.3. Kiểm tra độ trễ trên toàn đường đi

```text
D_total = D_ingestion + D_queue + D_DQ_ML + D_final_DB
        + D_outbox_relay + D_business_commit + D_SSE_browser
```

Mỗi D là thời gian của một đoạn trên **cùng event**. Phép tính giúp tìm đoạn nào
đang chiếm budget, không thay thế đo p95 toàn luồng. Nếu throughput chỉ vừa bằng
R, hệ thống không có dư năng lực xử lý backlog khi nguồn vẫn chạy. Cần dự phòng
được đo thực hoặc điều tiết nguồn theo chính sách có hiển thị trạng thái rõ.
