# 01 — Yêu cầu hệ thống xuất phát từ đề Chủ đề 3

**Phiên bản:** 1.2.0 — 23/10/2026. Sau phần này, đọc
[giả định vận hành và cách tính tải](02_ASSUMPTIONS_AND_CAPACITY.md).

Tài liệu này xác định hệ thống phải cung cấp hành vi gì trước khi lựa chọn công
nghệ. Mỗi yêu cầu được giải thích kèm trách nhiệm thiết kế và cách kiểm chứng.
Các mục “cần kiểm chứng” là kế hoạch nghiệm thu sau khi triển khai, chưa phải
công bố chức năng đã hoàn tất.

## 1. Nguồn gốc và cách đối chiếu

Nguồn chính là **CHỦ ĐỀ 3_Requirement.docx**, có tiêu đề “Predictive Maintenance &
Technician Copilot”, dành cho Nhóm 7/Nhóm 11. Các yêu cầu và giới hạn trong đề
được dùng làm căn cứ xây dựng báo cáo. Mã băm SHA256 dưới đây giúp xác định chính
xác phiên bản đề đã đối chiếu, tránh nhầm khi có nhiều bản chỉnh sửa:

```text
349d6f3c20fd338dedc63474e6fc24eec763154ac2f139b7a3627101093105c0
```

Các mã như `P087–088` là vị trí paragraph trong XML của DOCX, tính cả paragraph
trống và paragraph trong bảng. Chúng không phải số trang Word. Ví dụ, FR-01 nằm
ở P087–088 và mục tiêu độ trễ nằm ở P129–130. Nếu file gốc thay đổi, cần trích lại
nội dung và kiểm tra lại bảng đối chiếu thay vì giữ nguyên các vị trí cũ.

Mã FR được giữ theo đề bài. Các mã QR, ML, SIM, KB và EXT được dùng trong bộ tài
liệu này để tiện dẫn chiếu; chúng không bổ sung yêu cầu mới từ giảng viên. Các
mã ASM giữ cách đánh số giả định của đề. Phần giải thích đi kèm mỗi mã đã trình
bày nội dung cần hiểu; vị trí paragraph chỉ phục vụ đối chiếu khi cần kiểm tra
lại nguồn, không phải điều kiện để đọc báo cáo.

## 2. Bài toán và người sử dụng

MaintAI hỗ trợ kỹ thuật viên quan sát tình trạng thiết bị, nhận biết dấu hiệu
hỏng, ưu tiên kiểm tra và tra cứu hướng dẫn bảo trì. Mô hình ML phân loại khả
năng hỏng; chính sách rủi ro quyết định khi nào cần phát tín hiệu cảnh báo; RAG/LLM
hỗ trợ tìm tài liệu và trình bày hướng dẫn có nguồn. Quyết định kiểm tra hoặc
bảo trì thiết bị vẫn thuộc về con người.

| Bên liên quan | Hệ thống hỗ trợ công việc gì? | Nguồn |
| --- | --- | --- |
| Kỹ thuật viên | Nhận cảnh báo, đọc bằng chứng, thực hiện quy trình xử lý và ghi lại kết quả kiểm tra. | P005–012 |
| Trưởng ca hoặc trưởng nhóm bảo trì | Quan sát nhiều máy, xác định việc cần ưu tiên và chuyển cấp khi cần thêm người xử lý. | P012–014 |
| Quản lý sản xuất | Theo dõi tình trạng thiết bị và thông tin cần phối hợp với bộ phận bảo trì. | P013–014 |
| Admin, ML/MLOps | Quản lý truy cập, theo dõi sức khỏe hệ thống và giám sát dữ liệu, mô hình. | P013, P024, FR-10 |
| Người vận hành máy | Là bên chịu tác động từ quyết định bảo trì; hệ thống AI không được giao quyền điều khiển máy thay họ. | P014, P026–032 |

Các role `VIEWER`, `TECHNICIAN`, `ADMIN` và `SIMULATOR_OPERATOR` là tên do dự án
chọn để triển khai phân quyền. Đề yêu cầu xác thực và phân quyền, nhưng không
bắt buộc phải dùng chính các tên role này.

## 3. Mười yêu cầu chức năng và cách nghiệm thu

### FR-01 — Tiếp nhận và kiểm tra chất lượng dữ liệu

**Nguồn: P087–088.** Hệ thống nhận reading và kiểm tra cấu trúc, kiểu dữ liệu,
trường bị thiếu, miền giá trị cùng các ràng buộc liên quan trước khi đưa dữ liệu
vào ML. Trong kiến trúc hiện tại, Ingestion kiểm tra lớp thông điệp bên ngoài
(envelope), còn Processing thực hiện kiểm tra chất lượng dữ liệu sâu, gọi là DQ.

Cần kiểm chứng rằng reading bị kết luận `INVALID` không được gọi ML. Đồng thời,
phải phân biệt một HTTP request bị từ chối ngay tại Ingestion với một reading đã
được tiếp nhận vào Kafka nhưng sau đó bị Processing xác định là không hợp lệ.

HTTP `202 ACCEPTED` chỉ xác nhận Ingestion đã nhận được ACK từ Kafka. Nó chưa
xác nhận reading đạt DQ hay đã có prediction. Cách chia này đáp ứng điều kiện
“kiểm tra trước inference”; đề không yêu cầu hoàn thành toàn bộ DQ trước HTTP 202.

### FR-02 — Dự đoán bằng mô hình đã huấn luyện

**Nguồn: P089–090.** ML inference phải sử dụng mô hình đã huấn luyện để trả kết
quả Normal/Failure, điểm dự đoán và phiên bản mô hình. Processing chịu trách nhiệm
lưu kết quả cùng định danh input và thông tin phiên bản liên quan.

Cần đối chiếu input với đúng sáu đặc trưng được phép, kiểm tra output trên các
mẫu đã biết và xác minh phiên bản được trả về đúng với artifact thực sự đã nạp.
Một chuỗi tên phiên bản được ghi cố định trong response chưa đủ chứng minh việc này.

### FR-03 — Quyết định rủi ro và tạo cảnh báo độc lập với LLM

**Nguồn: P091–092.** Kết quả ML kết hợp với chính sách rủi ro để xác định trạng
thái và nhu cầu cảnh báo. Processing tính risk và phát tín hiệu; Business quyết
định tạo hoặc cập nhật alert theo quy tắc chống trùng và vòng đời nghiệp vụ.

Cần thử tắt hoặc gây timeout RAG/LLM rồi xác nhận cảnh báo thỏa policy vẫn được
lưu và hiển thị. Người dùng có thể chưa có hướng dẫn bảo trì, nhưng không được
mất cảnh báo chỉ vì phần sinh hướng dẫn đang lỗi.

### FR-04 — Hiển thị tình trạng thiết bị trên dashboard

**Nguồn: P093–094.** Dashboard cần cho người dùng xem các máy, số đo cảm biến,
chất lượng dữ liệu, kết quả ML và các cảnh báo cần xử lý. Business chuẩn bị dữ
liệu qua REST/SSE; Main UI trình bày chúng cùng độ mới và trạng thái tương ứng.

Cần kiểm tra trên trình duyệt rằng người dùng phân biệt được số đo raw chưa kiểm
tra, prediction hợp lệ gần nhất và reading mới đang chờ hoặc đã lỗi. Prediction
cũ phải giữ định danh và tuổi riêng, tránh khiến người xem hiểu nó được tính từ
số đo mới nhất khi hai dữ liệu thực tế thuộc hai input khác nhau.

### FR-05 — Quản lý toàn bộ vòng đời cảnh báo

**Nguồn: P095–096.** Hệ thống hỗ trợ tạo cảnh báo, xác nhận đã nhận (ACK), bắt đầu
xử lý, đánh dấu cảnh báo sai, giải quyết và chuyển cấp. Business Consumer/API
quản lý các thao tác này trong `business_db`.

Cần kiểm chứng các chuyển trạng thái hợp lệ, cách hạn chế cảnh báo trùng và kết
quả khi một request bị gửi lại. Mỗi thao tác phải truy được người thực hiện và
lịch sử thay đổi. Đánh dấu “đã nhận” không đồng nghĩa “đã sửa xong”.

### FR-06 — Cung cấp Maintenance Brief và checklist có nguồn

**Nguồn: P097–098.** Người dùng có thể yêu cầu bản tóm tắt bảo trì và danh sách
việc cần kiểm tra. Business điều phối tác vụ; RAG tìm tài liệu trong kho tri thức
và sinh nội dung dựa trên các nguồn tìm được.

Cần kiểm chứng citation tồn tại, trỏ đúng tài liệu/phiên bản và thực sự hỗ trợ
hành động được đề xuất. Chỉ có đường link đi kèm một câu trả lời chưa chứng minh
nội dung đó có căn cứ. Khi không đủ bằng chứng, hệ thống phải nói rõ giới hạn.

### FR-07 — Ghi nhận quyết định và kết quả kiểm tra của kỹ thuật viên

**Nguồn: P099–100.** Người dùng cần xem, chấp nhận hoặc từ chối khuyến nghị, ghi
chú và nhập kết quả kiểm tra thực tế. Main UI thu thập thao tác; Business lưu dữ
liệu gắn với người dùng, alert và brief liên quan để có thể truy vết.

Quyết định chấp nhận/từ chối cần được ghi nhận rõ, tách với điểm đánh giá độ hữu
ích và ghi chú tự do. Ví dụ, một kỹ thuật viên có thể thấy brief hữu ích nhưng
vẫn từ chối một thao tác do điều kiện máy chưa phù hợp. Khi triển khai, nhóm
cần chốt dữ liệu trao đổi và hành vi UI cho hai loại phản hồi này; phần giao
tiếp cụ thể hiện còn cần hoàn thiện trước nghiệm thu.

### FR-08 — Biểu diễn lỗi và thiếu bằng chứng một cách rõ ràng

**Nguồn: P101–102.** Dữ liệu sai, ML không khả dụng, LLM lỗi hoặc thiếu manual đều
phải có cách xử lý phù hợp. Thành phần phát hiện lỗi giữ đúng trạng thái theo
quy ước trạng thái; Business và UI phải trình bày được trạng thái đó. Trong báo
cáo, `INVALID` nghĩa dữ liệu không hợp lệ, `ML_UNAVAILABLE` nghĩa chưa có dịch vụ
ML phục vụ được và `UNKNOWN` nghĩa chưa đủ cơ sở kết luận tình trạng máy.

Cần có các tình huống thử lỗi để xác nhận `INVALID`, `UNKNOWN`, `ML_UNAVAILABLE`
hoặc thông báo không đủ bằng chứng được dùng đúng nguyên nhân. Thiếu prediction
không được biến thành kết luận `NORMAL`, vì “chưa biết” khác với “đã đánh giá bình thường”.

### FR-09 — Truy vết được dữ liệu và quyết định

**Nguồn: P103–104.** Từ một prediction hoặc alert, nhóm phải tìm lại được input,
kết quả, phiên bản model/policy và lịch sử liên quan. Với brief, cần biết tài
liệu nào và phiên bản nào đã được sử dụng; với thao tác, cần biết ai đã thực hiện.

Processing quản lý lịch sử xử lý; Storage lưu raw archive; Business giữ alert và
lịch sử nghiệp vụ; ML/RAG cung cấp thông tin nguồn gốc artifact. Cần kiểm chứng
các liên kết bằng định danh và mã băm thật. Chỉ lưu alias có thể đổi như `latest`
không đủ để tái hiện chính xác một quyết định trong quá khứ.

### FR-10 — Xác thực, phân quyền và bảo vệ truy cập

**Nguồn: P105–106.** Hệ thống cần biết người gọi là ai và được làm gì, bảo vệ khóa
truy cập và các chức năng quản trị. Dự án sử dụng nhà cung cấp danh tính bên ngoài
(IdP); từng API kiểm token, role và phạm vi máy mà người dùng được phép truy cập.

Cần kiểm tra trường hợp chưa đăng nhập (`401`), thiếu quyền (`403`) và truy cập
máy ngoài phạm vi. Quyền điều khiển Simulator phải được tách khỏi quyền nghiệp
vụ thông thường. Token/secret không được nằm trong cấu hình UI công khai hoặc log.
Pause/Resume chỉ điều khiển dữ liệu giả lập, không phải quyền điều khiển máy thật.

## 4. Yêu cầu riêng đối với dữ liệu, ML và kho tri thức

| Mã nội bộ | Nội dung cần thực hiện | Nguồn |
| --- | --- | --- |
| ML-01 | Tự huấn luyện mô hình ML phân loại nhị phân Machine Failure trên AI4I 2020. Đây là mô hình dự đoán, tách với LLM sinh ngôn ngữ. | P035–046, P146–147, P152 |
| ML-02 | Dùng sáu nhóm đầu vào: loại sản phẩm, nhiệt độ không khí, nhiệt độ quá trình, tốc độ quay, mô-men xoắn và độ mòn dụng cụ. | P038–044 |
| ML-03 | Có ít nhất một baseline đơn giản và một giải pháp ML khác để so sánh, cùng cách chia train/validation/test và cùng quy trình đánh giá. | P048–050 |
| ML-04 | So sánh thêm với phương án cảnh báo bằng các luật đơn giản để thấy giá trị của ML. | P051 |
| ML-05 | Xử lý mất cân bằng lớp; chọn ngưỡng bằng validation và báo kết quả trên test set chung. | P052–054 |
| ML-06 | Không dùng Machine failure, TWF, HDF, PWF, OSF hoặc RNF làm features. Không dùng các ID như tín hiệu dự đoán tùy tiện. | P054 |
| ML-07 | Phân loại từng loại hỏng là phần tùy chọn, không thay thế bài toán phân loại nhị phân bắt buộc. | P055 |
| SIM-01 | Phát lại AI4I thành luồng dữ liệu, gán máy và thời gian, chỉnh tốc độ và tạo các trường hợp lỗi để thử hệ thống. | P056–064 |
| KB-01 | Dùng kho tri thức chung đã thống nhất với giảng viên; tài liệu có ID, phiên bản, nội dung và nguồn có thể kiểm tra. | P065–077 |
| KB-02 | Không kết luận chắc chắn nguyên nhân hỏng chỉ từ score. Không bịa thông số hay thao tác; nếu thiếu chứng cứ thì yêu cầu kiểm tra thủ công. | P078–082 |

Phương án rule-based là một đối tượng so sánh trong đánh giá. Yêu cầu đó không
mặc nhiên cho phép dùng luật để tạo kết quả ML giả khi model không khả dụng.

Dữ liệu dùng để demo và dữ liệu dùng để đánh giá chất lượng model có mục đích
khác nhau. Nhóm có thể phát lại toàn bộ AI4I trong demo để chứng minh các luồng
hoạt động. Tuy nhiên, nếu model đã được huấn luyện hoặc lựa chọn dựa trên những
hàng dữ liệu đó, kết quả demo không phải phép đánh giá độc lập. Chỉ số ML vẫn
phải lấy từ test split chung; không điều chỉnh ngưỡng bằng test hoặc bằng các
đoạn demo được chọn trước.

## 5. Chất lượng và tiêu chí nghiệm thu

Những tiêu chí dưới đây được đo trên bộ dữ liệu, tình huống và môi trường đã
thống nhất. Các mục tiêu “100%” có phạm vi là bộ kiểm thử được nêu trong đề;
chúng không phải cam kết phát hiện mọi lỗi có thể xảy ra ở nhà máy thực tế.

### 5.1. Chất lượng ML và mức độ gây cảnh báo

| Mã / nguồn | Tiêu chí | Cách ghi nhận |
| --- | --- | --- |
| QR-01 / P113–114 | Báo Recall, Precision, F1, PR-AUC, confusion matrix và FNR cho lớp Failure; so sánh với baseline. | Dùng cùng test set và lưu phiên bản split, model, ngưỡng cùng artifact prediction. |
| QR-02 / P115–116 | Mục tiêu Recall_failure ≥ 0,85 và Precision_failure ≥ 0,50. | Chọn ngưỡng trên validation, báo số đo thật kể cả khi chưa đạt mục tiêu. |
| QR-03 / P117–118 | Báo số cảnh báo sai trên 1.000 reading hợp lệ và tỷ lệ cần con người kiểm tra. | Nêu cách xác định cảnh báo sai; phân biệt model dự đoán sai với số alert sau khi Business chống trùng. |

Mẫu số **1.000 reading hợp lệ** của QR-03 được giữ theo đề bài. Nó là cách chuẩn
hóa một chỉ số đánh giá, không liên quan đến giả định hệ thống ban đầu có 100 máy.

### 5.2. Chất lượng dữ liệu, RAG và an toàn

| Mã / nguồn | Tiêu chí | Cách ghi nhận |
| --- | --- | --- |
| QR-04 / P119–120 | Phát hiện 100% lỗi dữ liệu nghiêm trọng trong bộ test; không coi chúng là dữ liệu hợp lệ cho kết quả Normal. | Liệt kê fault cases, DQ output và bằng chứng các mẫu INVALID không gọi ML. |
| QR-05 / P121–122 | Cảnh báo đã được xác định vẫn được tạo và hiển thị khi RAG không khả dụng. | Gây timeout hoặc ngừng RAG rồi kiểm tra alert tại DB, SSE và UI. |
| QR-06 / P123–124 | Đánh giá Retrieval Recall@k trên bộ câu hỏi có nhãn chung. | Chốt k và cách chấm trong quy trình chung; đề chưa ấn định một giá trị k cụ thể. |
| QR-07 / P125–126 | 100% checklist có hướng dẫn hành động phải có nguồn kiểm tra được; mục tiêu ít nhất 90% khuyến nghị có căn cứ và phù hợp. | Kiểm citation và nội dung nguồn, chấm theo cùng tiêu chí thay vì chỉ đếm số link. |
| QR-08 / P127–128 | Không tự điều khiển máy và không đưa hướng dẫn quan trọng thiếu căn cứ trong bộ test an toàn. | Không có kết nối điều khiển PLC/actuator; trả lời thiếu bằng chứng khi không có nguồn phù hợp. |

### 5.3. Độ trễ, truy vết và hiệu quả sử dụng

| Mã / nguồn | Tiêu chí | Cách ghi nhận |
| --- | --- | --- |
| QR-09 / P129–130 | Mục tiêu p95 ≤ 2 giây từ lúc nhận reading hợp lệ đến khi hiển thị alert; không tính thời gian sinh brief. | Đo đến trình duyệt. Metric dừng ở lúc ghi DB chỉ phản ánh một đoạn của toàn luồng. |
| QR-10 / P131–132 | Mọi prediction/alert trong integration tests truy được input, model, policy và trạng thái; brief truy được manual. | Kiểm các liên kết bằng ID và mã băm thực tế của lần xử lý. |
| QR-11 / P133–134 | Lỗi Sensor, model, API hoặc LLM có trạng thái rõ và cách xử lý phù hợp. | Thử lỗi và phục hồi; xác nhận hệ thống không dùng NORMAL thay cho unavailable. |
| QR-12 / P135–136 | Đo thời gian nhận biết, đánh giá và xử lý; so việc tra cứu có và không có Copilot. | Thử với người dùng trên tình huống mô phỏng; không suy ra mức giảm downtime thật của nhà máy. |

p95 ≤ 2 giây có nghĩa ít nhất 95% các mẫu đo thuộc phạm vi đánh giá có độ trễ
không vượt quá 2 giây; các trường hợp timeout hoặc mất alert phải được báo riêng.
Môi trường, thời lượng và profile tải cần đi kèm kết quả. Chạy đạt ở 5 máy không
chứng minh đạt ở 100 máy hoặc X > 100. Đề cũng không cam kết cùng độ trễ cho mọi
quy mô, mọi sự cố hoặc mọi laptop. Cách đo chi tiết nằm trong file 02, mục 9.

## 6. Giả định của đề và việc nhóm phải kiểm chứng

| Mã gốc | Giả định tại mục 5.1 của đề | Việc nhóm cần làm | Nguồn |
| --- | --- | --- | --- |
| ASM-01 | AI4I phù hợp làm prototype, nhưng chưa chứng minh tổng quát hóa sang máy thật. | Ghi rõ định danh máy, thời gian và diễn biến scenario là giả lập. | P140 |
| ASM-02 | Schema, đơn vị và ràng buộc của dữ liệu đã biết. | Chốt danh sách features và các trường hợp DQ; loại nhãn/ID khỏi input model. | P141 |
| ASM-03 | Có manual phù hợp với metadata đủ để kiểm nguồn. | Chốt Common KB và nguồn thật; không tạo manual production giả để lấp khoảng trống. | P142 |
| ASM-04 | Kỹ thuật viên có khả năng xử lý cảnh báo, nhưng tính kịp thời cần được đánh giá. | Đo quy trình xử lý và nguy cơ quá tải cảnh báo thay vì chỉ đo tốc độ ML. | P143 |
| ASM-05 | Tài nguyên và ngân sách LLM đủ cho thử nghiệm, có xử lý khi không khả dụng. | Công khai ngân sách, tải, hàng đợi, timeout và trạng thái lỗi. | P144 |

## 7. Phần ngoài phạm vi và các quyết định mở rộng

Theo P025–032 và P148–152, hệ thống không bao gồm điều khiển PLC/actuator thật,
tự khởi động/dừng/sửa máy, dự đoán RUL chính xác, mua phụ tùng hoặc tối ưu sản
xuất. Phân loại từng loại hỏng không phải yêu cầu bắt buộc. Dự án cũng chưa đặt
mục tiêu chứng minh giảm downtime/chi phí thực tế hoặc đạt chứng nhận an toàn
công nghiệp. Pause/Resume ở Simulator UI chỉ thay đổi luồng dữ liệu demo.

Các lựa chọn sau thuộc quyết định thiết kế của nhóm:

| Mã | Quyết định dự án | Mục đích |
| --- | --- | --- |
| EXT-01 | Dùng HTTP 202, Kafka và các consumer group độc lập. | Tách tiếp nhận khỏi xử lý, cung cấp cùng dữ liệu cho nhiều nhánh và hỗ trợ đọc lại khi phục hồi. |
| EXT-02 | Processing xử lý tuần tự theo máy, có cơ chế chặn worker cũ và Outbox giữ thứ tự gửi. | Cho các máy chạy song song mà vẫn giữ đúng dữ liệu khi retry hoặc Kafka phân công lại partition. |
| EXT-03 | Business giữ dữ liệu phục vụ UI; Processing giữ lịch sử gần và archive lịch sử lên object storage. | Phục vụ truy vấn nhanh và lưu dài hạn theo trách nhiệm của từng owner. |
| EXT-04 | Có bốn UI; ML training, inference và MLflow Server dùng image riêng; Drift chạy theo đợt. | Tách người dùng, tài nguyên và thời điểm phát hành của những công việc khác nhau. |
| EXT-05 | Dùng Kubernetes trên laptop thông qua k3d và lệnh triển khai; chuẩn bị thiết kế AWS/GitOps riêng cho cloud. | Phát triển và kiểm chứng local trước khi sử dụng hạ tầng cloud. |
| EXT-06 | Dataset được duyệt mới chạy training batch; model qua đánh giá rồi phát hành bằng PR GitOps. | Kiểm soát phiên bản và quyết định đưa model vào sử dụng. |

Đề có nêu Data Drift như một biện pháp kiểm soát rủi ro tại P168–170. Yêu cầu đó
hướng tới giám sát và đánh giá lại khi phân bố dữ liệu thay đổi. Nó không bắt
buộc tự động retrain hoặc promote model; Drift vẫn nằm ngoài luồng cảnh báo.
Các đánh đổi của EXT-01 đến EXT-06 được giải thích trong file 04.

## 8. Những phần cần hoàn thiện trước khi nghiệm thu

Nhóm còn cần hoàn thiện giao tiếp ghi nhận accept/reject của FR-07, kho tri thức
và quy trình đánh giá chung, phép đo độ trễ đến trình duyệt, API, logic nghiệp
vụ và xác thực/phân quyền. Phần 03, mục 8 tổng hợp phạm vi đã triển khai riêng
và các bước còn lại trước khi đánh giá ứng dụng tích hợp.

Các chỉ số chất lượng ML, tốc độ 100 readings/s, độ trễ đến UI và khả năng xử lý
cảnh báo của con người chỉ được đánh dấu đạt sau khi có bằng chứng tương ứng.
Kết quả nghiệm thu cần nêu cấu hình, môi trường, dữ liệu và lần chạy cụ thể để
người đọc có thể phân biệt mục tiêu thiết kế với kết quả thực nghiệm.
