# 04 — Biện luận lựa chọn kiến trúc và các đánh đổi

**Phiên bản:** 1.2.0 — 23/10/2026. Căn cứ:
[yêu cầu](01_REQUIREMENTS.md), [giả định về tải](02_ASSUMPTIONS_AND_CAPACITY.md),
[kiến trúc](03_SYSTEM_ARCHITECTURE.md). Các quy tắc về thứ tự, lưu bền vững và
trách nhiệm dữ liệu được giải thích trực tiếp trong phần 03 và dưới đây.

Tài liệu này giải thích mỗi lựa chọn đem lại điều gì, phải trả chi phí gì và
cần kiểm chứng ở đâu. Các phân tích lỗi mô tả hành vi thiết kế cần đạt; chúng
không được xem là kết quả chạy thử toàn bộ ứng dụng.

## 1. Vì sao hệ thống có kiến trúc như hiện tại?

MaintAI phải làm nhiều việc có đặc điểm khác nhau: nhận dữ liệu liên tục, gọi
ML, tạo cảnh báo, phục vụ UI, truy vấn lịch sử, sinh hướng dẫn và huấn luyện model.
Một lần inference thường ngắn nhưng diễn ra liên tục; một brief có thể mất lâu
hơn; một lượt training có thể sử dụng nhiều CPU/RAM trong thời gian dài. Nếu
những việc này cùng giữ tài nguyên trong một luồng request, một phần chậm có
thể kéo chậm những phần không cần phụ thuộc vào nó.

Nhóm chọn tách các runtime theo công việc và nhu cầu vận hành, đồng thời giữ
các module cần cùng dữ liệu/transaction trong một owner. Ví dụ, DQ và risk cùng
nằm ở Processing; Worker và Read API cùng owner nhưng chạy riêng vì một bên
xử lý luồng, một bên phục vụ truy vấn. Cách tổ chức này cho phép tối ưu đúng
thành phần khi có số đo, đồng thời giới hạn số ranh giới phân tán phải quản lý.

Baseline là **100 máy, mỗi máy 1 reading/s**. Năm máy là profile chạy thử nhỏ.
Kiến trúc **scale-ready** có nghĩa khi X > 100, nhóm có thể tính lại và tăng
năng lực của từng nhánh để giữ độ trễ mục tiêu. Các công thức và ví dụ nằm ở
[phần 02, mục 11](02_ASSUMPTIONS_AND_CAPACITY.md#11-scale-ready-tính-và-tối-ưu-latency-khi-x100).

Chính số lượng 100 máy không bắt buộc một công nghệ hoặc số Deployment cụ thể.
Lựa chọn hiện tại còn phục vụ các yêu cầu về nhiều nhánh dữ liệu, phục hồi sau
lỗi và truy vết phiên bản. Đổi lại, nhóm phải vận hành Kafka, nhiều tiến trình,
network, quyền truy cập và cơ chế retry. Các lợi ích này cần được chứng minh
bằng kiểm thử và số đo tương ứng với độ phức tạp đã thêm vào.

## 2. Tóm tắt các quyết định và đánh đổi

| Mã | Quyết định và căn cứ | Lợi ích mong muốn | Chi phí cần chấp nhận/kiểm chứng |
| --- | --- | --- | --- |
| D01 | Tách Ingestion khỏi Business, theo FR-01/04/08 và profile burst. | Tiếp nhận Sensor không phải chờ request UI, history hoặc RAG. | Thêm API/auth/config; cần đo ACK, giới hạn tải và retry. |
| D02 | Dùng Kafka và các group độc lập, theo FR-03/09 và nhu cầu nhiều nhánh. | Cùng dữ liệu đến Processing, Storage và Business; mỗi nhánh đọc lại theo tiến độ riêng. | Phải vận hành broker, retention, ACL và lag; Kafka không lưu vô hạn. |
| D03 | Worker xử lý theo key, commit offset an toàn và chặn worker cũ. | Khác máy chạy song song, cùng máy giữ thứ tự qua retry và phân công lại partition. | Hàng đợi, bộ nhớ, offset chưa hoàn tất và migration phức tạp hơn. |
| D04 | Lưu Outbox cùng kết quả, dùng Relay có thứ tự, theo FR-03/05/09. | Không mất ý định gửi event khi DB đã commit nhưng Kafka chưa nhận. | Chấp nhận gửi trễ/gửi trùng; Relay cần năng lực và cơ chế phục hồi riêng. |
| D05 | Read API chạy riêng, các module cùng transaction giữ chung owner. | Phục vụ truy vấn và xử lý luồng bằng pool/replica khác nhau, giữ quyền sở hữu DB rõ. | Thêm một lượt HTTP; cùng DB vẫn có thể tranh tài nguyên. |
| D06 | Business giữ projection mới nhất và cập nhật UI bằng SSE, theo FR-04/05, QR-09 và giả định A-UI. | Query UI nhanh; quản lý alert, quyền và audit tại một owner. | Phải đồng bộ bản sao dữ liệu, chống trùng, giữ thứ tự và phục hồi kết nối. |
| D07 | Lịch sử gần ở DB, lịch sử cũ ở archive đã xác minh, theo FR-09 và ước lượng dung lượng. | Tách nhu cầu truy vấn nhanh khỏi lưu dài hạn. | Cần index/manifest, xác minh độ đầy đủ và kiểm tra điều kiện trước cleanup. |
| D08 | RAG, Drift và training nằm ngoài đường tạo alert, theo FR-03/06/08 và QR-05. | Alert không chờ LLM hoặc batch job. | UI phải thể hiện brief/report đang chờ hoặc không khả dụng. |
| D09 | Training, inference và MLflow dùng image riêng. | Mỗi phần có dependencies, tài nguyên và lịch cập nhật phù hợp. | Cần kiểm soát tương thích và truy vết model bundle/image. |
| D10 | Có bốn UI và kiểm quyền ở từng provider, theo FR-04/05/10. | Phục vụ đúng nhóm người dùng; Business và Simulator có thể phát hành riêng. | Thêm origins/config/access policies; hạ tầng dùng chung vẫn có thể lỗi chung. |
| D11 | Quản lý chung cấu hình tạo cluster và triển khai ứng dụng local. | Dễ chuẩn bị môi trường laptop và kiểm tra trước khi dùng cloud. | CLI apply chưa có GitOps tự đồng bộ; local chưa chứng minh tính sẵn sàng cao hoặc tương đương AWS. |

## 3. So sánh với backend xử lý đồng bộ trước đây

Trong phương án cũ dùng để so sánh, Sensor gửi request tới backend; backend
thực hiện DQ → ML → risk → alert trên đường xử lý request, đồng thời phục vụ UI.
Backend gộp vẫn có thể được viết tốt, có persistence, retry và nhiều replica.
Sự khác biệt chính là đơn vị chạy/cập nhật và công việc nào phải chờ công việc
nào trong một request.

| Khía cạnh | Backend gộp/xử lý đồng bộ | Kiến trúc mục tiêu hiện tại |
| --- | --- | --- |
| Sensor nhận phản hồi khi nào? | Request thường chờ inference và xử lý rủi ro xong. | Ingestion chờ Kafka ACK rồi trả 202; kết quả xử lý đến sau. |
| Khi ML chậm | Request giữ worker hoặc connection lâu hơn nếu không có cơ chế tách/giới hạn tốt. | Processing có số task hữu hạn; Kafka có thể giữ phần chưa xử lý trong giới hạn tài nguyên. |
| Khi UI/history tăng tải | Các request có thể tranh tài nguyên với ingest và inference trong cùng runtime. | Business API, Read API và Worker có thể đặt pool/replica riêng. |
| Lưu raw archive | Cần gọi storage trong request hoặc bổ sung writer/job của backend. | Storage Consumer đọc group riêng, ML không phải chờ raw archive. |
| Retry và mất response | Vẫn cần định danh và chống tạo hiệu ứng trùng. | Có Inbox/Outbox/cursor và kiểm thứ tự; nhiều ranh giới hơn cũng tạo thêm điểm phải xử lý duplicate. |
| Cập nhật phiên bản | Đơn vị rollout lớn hơn; vẫn có thể rolling nếu đủ replica và tương thích. | Rollout từng role nhỏ hơn, nhưng phải drain, chặn worker cũ và giữ tương thích DB/event. |
| Chi phí vận hành | Ít tiến trình, network và cấu hình hơn. | Cần broker, metrics, secrets, health và xử lý sự cố xuyên nhiều runtime. |
| Chẩn đoán lỗi | Ít bước truyền, thường dễ theo dõi trong một process. | Có lag và ranh giới dữ liệu rõ, nhưng cần ID để nối log/trace giữa các bước. |
| Dữ liệu UI | Có thể trả kết quả sau transaction ngay trong request. | Raw, DQ và prediction có thể đến khác lúc; UI phải chỉ rõ input tương ứng, tuổi và trạng thái. |

Ở tải chạy thử 5/s, chi phí nhiều runtime có thể lớn so với nhu cầu tức thời.
Baseline 100/s giúp nhóm đánh giá các điểm tranh tài nguyên, chưa chứng minh
phương án cũ chắc chắn thất bại hoặc phương án mới đã đáp ứng được 100/s.

Nếu thực tế cần một profile nhỏ hơn, có thể review việc giảm replica hoặc gộp
runtime cùng owner. Khi đó vẫn phải giữ quy tắc transaction, thứ tự, health và
release. Việc gộp không chỉ là xóa một Deployment mà bỏ qua cơ chế phục hồi.

## 4. Giải thích chi tiết các lựa chọn quan trọng

### 4.1. Vì sao Ingestion không làm toàn bộ DQ và ML?

FR-01 yêu cầu kiểm dữ liệu trước khi inference. Nhóm chia việc kiểm tra thành
hai lớp: Ingestion kiểm auth, envelope và giới hạn request để bảo vệ điểm nhận;
Processing kiểm các điều kiện dữ liệu sâu theo schema/model/policy đang dùng.
Như vậy, dữ liệu chỉ vào ML sau khi đạt DQ, dù Kafka đã tiếp nhận nó trước đó.

Ví dụ, ML có một đợt chậm ngắn. Nếu Ingestion chỉ phải publish Kafka, nó vẫn có
thể nhận thêm reading khi broker và tài nguyên còn đủ. Worker xử lý phần tồn
đọng sau đó. Nếu Kafka ngừng hoạt động, Ingestion không trả 202 như thể đã lưu
thành công; Sensor giữ dữ liệu trong spool và retry theo backoff hữu hạn.

Đánh đổi là kết quả không có ngay sau 202. UI/status cần thể hiện đang chờ,
INVALID hoặc lỗi xử lý. Tách tiếp nhận không biến bộ nhớ đệm thành vô hạn và
không giữ được mục tiêu độ trễ khi backlog kéo dài.

### 4.2. Vì sao dùng 12 partition và xử lý song song theo máy?

Mười hai partition là baseline kỹ thuật đã chọn để có dư địa phân công group
members. Không có phép tính nào từ 100 máy suy ra duy nhất con số 12. Khi key
là `machine_id` và cách phân vùng không đổi, readings của cùng máy vào cùng
partition; một partition vẫn chứa nhiều máy.

Worker giữ hàng đợi theo máy. Nếu máy A đang chờ ML, task của máy B có thể chạy
khi còn slot, kể cả hai máy ở cùng partition. Reading kế tiếp của A vẫn đợi A
hoàn tất. Cơ chế commit giữ phần chưa xong ở đầu tiến độ partition để restart
không bỏ sót dữ liệu; fence ngăn worker đã mất quyền tiếp tục ghi DB.

Với 1 replica, 8 active keys, task trung bình 20 ms và hệ số sử dụng 70%, phép
tính sơ bộ cho 280/s. Đây là giả định để tính, không phải benchmark. Khi X tăng,
nhóm phải đo cả ML, DB, Business và Storage. Tăng partition có thể đổi mapping
máy; cần kế hoạch chuyển đổi được review, không sửa count trực tiếp rồi giả
định thứ tự vẫn tự được bảo toàn.

### 4.3. Outbox giải quyết lỗi gì và vì sao Relay chạy riêng?

Giả sử Worker đã ghi outcome vào DB rồi mới gửi Kafka. Nếu tiến trình chết giữa
hai việc, DB có kết quả nhưng bên Business không nhận được event. Đảo thứ tự
cũng có vấn đề: Kafka có thể nhận event nhưng transaction DB sau đó thất bại.
Hai thao tác thuộc hai hệ thống nên không tự thành một transaction duy nhất.

Outbox giải quyết bằng cách ghi **kết quả và ý định gửi event trong cùng
transaction DB**. Relay đọc ý định đã lưu, gửi Kafka rồi cập nhật tiến độ sau
ACK. Khi Kafka lỗi, ý định vẫn còn để gửi lại; không phải đoán từ log xem Worker
đã gửi hay chưa.

Relay chạy riêng vì chờ broker, retry và tốc độ publish khác với DQ/inference.
Một lần cập nhật Relay không cần restart Worker. Trong cùng topic/máy, Relay
chỉ gửi đầu hàng đợi; head đang retry hoặc chưa biết kết quả ACK thì successor
phải chờ. Các replica có thể xử lý key khác nhau nhờ cơ chế thuê quyền có hạn
(lease). Mỗi runner hiện vẫn publish tuần tự, chưa có pool publish song song.

Nếu broker đã nhận nhưng ACK bị mất, Relay có thể gửi lại. Bản trùng đến muộn
hoặc event ở hai topic đến khác thứ tự vẫn phải được Business xử lý bằng Inbox
và kiểm thứ tự cập nhật. Outbox bảo vệ ý định gửi, không cam kết exactly-once
cho toàn pipeline; ML cũng có thể được gọi lại sau crash trước khi kết quả được lưu.

### 4.4. Vì sao tách Read API nhưng giữ DQ và risk thành module?

Worker xử lý luồng liên tục, còn Read API phục vụ người xem lịch sử, trạng thái
và cửa sổ dữ liệu. Hai công việc có kiểu query, connection pool và nhịp tải khác
nhau. Tách runtime giúp điều chỉnh chúng riêng, dù vẫn cùng owner Processing.

Read API đọc kết quả đã commit vào DB/archive. Nó không hỏi bộ nhớ Worker về
task đang chạy, nên restart Worker không xóa lịch sử và mở trang lịch sử không
kích hoạt inference lại. Business/Drift dùng interface này thay vì đọc chéo DB.

Ngược lại, DQ, risk, history, Inbox, cursor và Outbox phối hợp để tạo một kết quả
cuối cùng bền vững. Giữ các phần đó cùng owner làm rõ transaction và giảm số
giao diện trao đổi phân tán cho nhóm 4 người. Tách mỗi hàm thành service sẽ cần thêm cơ
chế truyền context, retry và nhất quán giữa chúng.

Business alert có người thao tác, vòng đời và dữ liệu riêng nên khác với risk
signal của Processing. DQ/risk chỉ nên tách tiếp khi có nhu cầu độc lập về người
phụ trách, tài nguyên, SLA hoặc tái sử dụng được chứng minh bằng thực tế.

### 4.5. Vì sao Business nhận Kafka thay vì dựng màn hình từ object storage?

Object storage phù hợp giữ raw, archive và artifacts. Màn hình nghiệp vụ còn
cần trả lời “trạng thái hiện tại của máy là gì?”, “alert nào đang mở?” và “ai
đã xác nhận alert này?”. Business tạo **projection**, tức bản dữ liệu đã chuẩn
bị để truy vấn, bằng cách nhận event và cập nhật trạng thái mới nhất trong DB.

Ví dụ, máy có nhiều reading nhưng trang danh sách chỉ cần snapshot mới nhất
cùng alert đang active. Nếu mỗi lần refresh phải đọc nhiều file để tìm latest,
backend sẽ phải tái dựng trạng thái, kiểm thứ tự và ghép quyền liên tục. Projection
giữ phần việc đó sau mỗi event, đồng thời cho phép ghi alert/action/audit bằng
transaction nghiệp vụ.

Truy vấn object storage vẫn có chỗ dùng. Khi xem lịch sử cũ hoặc export, Business
gọi Processing Read API để đọc qua index/manifest và logic DB/archive. Nếu sau
này cần analytics lớn, nhóm có thể bổ sung query engine theo số đo. Dashboard
analytics không tự thay được quy trình ACK, xử lý và truy vết một alert.

### 4.6. Vì sao lịch sử không được dồn hết vào Business DB?

Ở baseline 100/s, chạy liên tục tạo khoảng 8,64 triệu reading/ngày. Business cần
snapshot mới nhất theo máy và dữ liệu nghiệp vụ; nếu giữ toàn bộ telemetry tại
đây, các index, backup và cleanup của luồng UI cũng tăng theo lượng dữ liệu đó.

Processing giữ lịch sử kết quả vì nó là nơi biết DQ, model, policy và outcome.
Storage giữ raw archive độc lập; Processing quản lý archive lịch sử đã xử lý.
Dữ liệu gần có thể đọc nhanh từ DB, còn lịch sử dài hạn được đưa vào object
storage với manifest và index phù hợp.

Cách chia này làm việc truy vấn xuyên DB/archive và cleanup phức tạp hơn. Nhóm
phải xác minh từng bản ghi đã có trong archive, kiểm pin và yêu cầu giữ bằng
chứng trước khi xóa dữ liệu nóng. Nếu chưa làm xong, cần giữ dữ liệu hoặc báo
phạm vi lịch sử chưa đầy đủ; việc có một bucket không chứng minh đã archive thành công.

### 4.7. Vì sao ML, RAG, Drift và training không chung một luồng?

ML inference là bước online cần thiết cho reading VALID. RAG hỗ trợ giải thích
và hướng dẫn, có thể mất lâu hoặc thiếu nguồn. Drift so sánh phân bố theo cửa
sổ, còn training chạy trên dataset đã được duyệt. Chỉ ML inference nằm trong
đường đánh giá reading; các phần còn lại không được làm alert phải chờ.

Drift phát hiện thay đổi phân bố chưa cho biết model mới nào tốt hơn. Việc phát
hành cần dataset/split, kết quả đánh giá, model/preprocessor/reference và policy
có phiên bản, sau đó review. Vì vậy Drift không tự retrain hoặc promote.

Training, inference và MLflow Server cũng có dependencies, thời gian sống và
lịch nâng cấp khác nhau nên dùng image riêng. MLflow cung cấp tracking/registry,
không chạy thay inference. Theo thiết kế pin/cache, inference đã nạp đủ bundle
có thể tiếp tục khi Tracking Server tạm lỗi; startup thiếu bundle phải chưa
sẵn sàng phục vụ. Hành vi này cần được triển khai và kiểm thử thực tế.

### 4.8. Vì sao có bốn UI và dùng IdP?

Business UI phục vụ kỹ thuật viên, Simulator UI phục vụ người demo. Tách hai
frontend giúp sửa một giao diện mà không bắt giao diện còn lại phải rollout,
miễn API vẫn tương thích. Grafana và MLflow đã cung cấp UI công cụ cho vận hành
và ML; app cung cấp dữ liệu/metrics phù hợp thay vì tự viết lại các giao diện đó.

Hai image frontend riêng vẫn có thể cùng lỗi khi cluster, Ingress hoặc IdP chung
gặp sự cố. Các HTML prototype minh họa chức năng cũng không chứng minh đã có
thêm runtime đang triển khai.

IdP xác thực danh tính; API xác minh token và kiểm role/phạm vi máy theo hành
động. Không cần thêm một auth service tự viết để mọi request hỏi mật khẩu hoặc
credential đồng bộ. Cơ chế lấy khóa xác minh JWKS, làm mới khóa, token hết hạn
và thu hồi vẫn cần thiết kế/kiểm chứng. Quyền ADMIN của ứng dụng không mặc nhiên
là quyền quản trị Grafana, MLflow hoặc điều khiển Simulator.

### 4.9. Vì sao quản lý chung hạ tầng và triển khai ứng dụng ở local?

Mục tiêu ban đầu là phát triển và demo trên laptop. Quản lý cùng bộ cấu hình
tạo cluster, Kafka/DB/storage và manifests ứng dụng giúp giảm số bước chuẩn bị,
đồng thời dễ tạo lại môi trường thử nghiệm. Phiên bản mã nguồn, image và cấu
hình vẫn cần được ghi nhận để tái hiện một lần chạy.

Đánh đổi là local hiện triển khai khi người vận hành chạy lệnh apply; chưa có
Argo CD liên tục so sánh trạng thái mong muốn trong Git với cluster để tự đồng
bộ. Thiết kế GitOps cho cloud có quy trình phát hành riêng, chưa được nối thành
cơ chế deploy local. Nếu sau này cần kiểm chứng GitOps trên laptop, nhóm phải
bổ sung bộ điều khiển và quy trình cập nhật/cấp quyền tương ứng.

Phần 03, mục 7 mô tả hai môi trường và cách ghi phiên bản. Việc gom cấu hình
local phục vụ tiện lợi khi phát triển; nó không loại bỏ yêu cầu tách quyền, bảo
vệ bí mật và duyệt bản phát hành khi chuyển sang môi trường production.

## 5. Sự cố và rolling update ảnh hưởng đến người dùng ra sao?

### 5.1. Phân biệt giữ dữ liệu với phục vụ kịp thời

Bốn điều cần đánh giá riêng là: dữ liệu còn được giữ hay không (**durability**),
chức năng có gọi được hay không (**availability**), thông tin đang xem mới đến
đâu (**freshness**) và mất bao lâu để thấy kết quả (**latency**). Một nhánh có
thể tiếp tục lưu dữ liệu trong khi màn hình chưa có cảnh báo mới.

Ví dụ, Business API dừng nhưng Consumer vẫn ghi alert vào DB thì kết quả không
bị mất tại bước đó. Tuy nhiên, kỹ thuật viên chưa thấy alert cho đến khi API
hoặc replica khác phục vụ lại. Nếu Consumer dừng, ML có thể đã xong nhưng alert
mới còn chờ đọc event và ghi vào Business. “Các nhánh khác vẫn chạy” giúp giữ
tiến độ và tránh dừng cả hệ thống; nó chưa có nghĩa chức năng cuối đã hoàn tất.

### 5.2. Các tình huống cần kiểm chứng

Bảng giả sử cơ chế tương ứng đã được triển khai đúng, tài nguyên còn đủ và dữ
liệu chưa hết retention. Đây là kế hoạch kiểm tra lỗi, chưa phải báo cáo chaos test.

| Sự cố | Phần có thể tiếp tục | Ảnh hưởng và cách phục hồi cần có |
| --- | --- | --- |
| Business API dừng hoặc rollout | Ingestion, Processing, Storage và Business Consumer. | REST/SSE của instance mất; alert có thể đã lưu nhưng chưa hiện. Cần replica khỏe hoặc reconnect và REST snapshot sau phục hồi. |
| Business Consumer dừng | Sensor → Kafka → Processing/Outbox và archive raw. | Projection/alert mới chưa được ghi, UI cũ dần. Consumer phải đọc bù sau restart; độ trễ 2 giây có thể không còn đạt. |
| Worker chết trước transaction cuối | Kafka giữ record chưa xác nhận; nhánh telemetry/raw vẫn độc lập. | Đọc lại record; ML có thể được gọi lại. Inbox/fence phải ngăn hiệu ứng trùng hoặc ghi từ worker đã mất quyền. |
| Worker chết sau DB commit nhưng trước commit offset | History và Outbox đã được lưu. | Đọc lại nhận ra duplicate, giữ kết quả đã có và không tạo hiệu ứng nghiệp vụ trùng; mốc offset an toàn vẫn được bảo vệ. |
| Relay chết hoặc mất ACK | Worker có thể ghi thêm history/Outbox khi DB còn chỗ. | Event đến trễ hoặc trùng. Head đang chờ phải chặn successor cùng key; phục hồi bằng Outbox/cursor và kiểm thứ tự ở bên nhận. |
| Read API dừng | Worker, Relay và cập nhật Business online. | Lịch sử/status/window qua API này có thể không khả dụng; trả lỗi rõ và có timeout, không truy SQL chéo owner để đi vòng. |
| Storage hoặc MinIO dừng | Luồng ML/risk và projection có thể tiếp tục trong giới hạn các dependency còn lại. | Archive lag tăng, offset nhánh archive chưa commit. Nếu dữ liệu hết retention trước phục hồi, có thể không archive lại đầy đủ. |
| ML dừng | Ingestion, telemetry và raw archive trong budget cho phép. | Không có kết quả ML hợp lệ mới; hiển thị trạng thái không khả dụng/chưa thể kết luận, không thay bằng Normal. |
| RAG/LLM dừng | Luồng tạo và hiển thị alert. | Brief hiển thị pending hoặc unavailable phù hợp; nếu thiếu bằng chứng thì báo thiếu, không sinh checklist vô căn cứ. |
| Kafka dừng | Business có thể phục vụ snapshot/alert đã lưu nếu DB/API vẫn hoạt động. | Ingestion không có broker ACK để xác nhận 202; Sensor dùng spool/backoff hữu hạn. Các consumer và Relay bị chặn. |
| PostgreSQL instance hoặc laptop dừng | Không có bảo đảm tách lỗi vật lý chỉ nhờ năm logical DB hoặc nhiều Deployment. | Nhiều chức năng có thể dừng cùng lúc. Cần kế hoạch restore/HA riêng; laptop một node/broker không có khả năng chịu lỗi như cluster nhiều máy. |

### 5.3. Khi cập nhật phiên bản cần xử lý gì?

**Rolling update** thay các replica cũ bằng replica mới từng phần. Muốn giảm gián
đoạn, ngoài số replica còn phải giữ tương thích API/event/DB và đưa instance mới
vào phục vụ sau khi kiểm tra trạng thái sẵn sàng đã đạt.

- **Worker:** ngừng nhận thêm việc phù hợp với quá trình dừng, cho task đang chạy
  hoàn tất trong deadline hữu hạn. Khi mất quyền partition hoặc hết thời gian,
  fence phải chặn worker cũ ghi DB/offset; dữ liệu chưa xác nhận sẽ được đọc lại.
- **Relay:** giữ lease theo key và mốc tiến độ theo ACK. Dừng giữa publish và cập
  nhật DB có thể tạo duplicate, nên cơ chế phục hồi và chống trùng vẫn cần thiết.
- **Business API:** kết nối SSE có thể bị ngắt khi instance bị thay. Browser cần
  kết nối lại và lấy REST snapshot để không lệ thuộc vào kết nối cũ.
- **Sensor:** state SQLite hiện dùng một controller/replica và chiến lược Recreate.
  Chấp nhận khoảng dừng khi thay Pod, không chạy chồng hai controller cùng sửa
  một state chỉ để có hình thức rolling update.

Backend gộp cũng có thể rolling khi có đủ replica. Việc tách runtime giúp phạm
vi mỗi lần cập nhật nhỏ hơn; khả năng không gián đoạn vẫn phải được kiểm chứng
trên cấu hình cụ thể.

## 6. Khi nào cần xem lại cách tổ chức hoặc tăng năng lực?

| Quan sát thực tế | Hướng xử lý cần xem xét |
| --- | --- |
| Source không tạo đủ tải mục tiêu. | Sửa Simulator/load driver trước khi đánh giá capacity của downstream. |
| ML/DB chậm và tuổi record chờ Worker tăng. | Đo pool, khóa DB, task duration và lệch tải; tối ưu hoặc scale đúng dependency. |
| Worker theo kịp nhưng Relay/Storage/Business lag tăng. | Điều chỉnh chính nhánh chậm; review batching/concurrency nếu cần thay cách xử lý. |
| Có hot key hoặc lệch partition, đã dùng hết 12 member có assignment. | Review phân vùng và kế hoạch chuyển đổi giữ thứ tự, hoặc cô lập workload đặc biệt. |
| Consumer theo kịp nhưng dashboard chậm. | Kiểm phạm vi SSE, cách gộp telemetry, phân trang, fan-out và render trên browser. |
| History/export tranh tài nguyên với online. | Tối ưu query/index, pool đọc và quota; cân nhắc read replica/query engine khi có bằng chứng. |
| DQ/risk thực sự có owner, tài nguyên hoặc SLA độc lập. | Cân nhắc service riêng và thiết kế giao tiếp, lưu bền vững, chuyển đổi dữ liệu tương ứng. |
| Chi phí vận hành quá lớn cho demo nhỏ. | Review profile ít replica hoặc gộp runtime cùng owner, giữ đúng các quy tắc dữ liệu. |

Mức X mới phải đi kèm tốc độ r và cơ cấu request. Hai hệ thống cùng có 500 máy
nhưng một hệ gửi 1 reading/phút, hệ kia 10 readings/giây có tải rất khác nhau.
Quyết định scale hoặc tách tiếp cần dựa vào tải và độ trễ đo được.

## 7. Những bằng chứng cần có để bảo vệ lựa chọn thiết kế

1. **Chất lượng mô hình và tri thức:** chốt dataset/split và quy trình đánh giá
   chung, Common KB, baseline và phương án so sánh. Lưu kết quả chọn ngưỡng và
   các chỉ số thật trên test, kể cả những mục chưa đạt.
2. **Giao tiếp giữa các thành phần:** thống nhất dữ liệu ghi nhận accept/reject
   của FR-07, timestamp tiếp nhận gắn đúng alert, phạm vi cập nhật UI và cách
   đo tới trình duyệt. Kiểm tra cả bên gửi và bên nhận để tránh hiểu khác nhau
   về cùng một trường dữ liệu.
3. **Code và cấu hình chạy:** hoàn thiện Ingestion, nghiệp vụ Processing/Business,
   ML/RAG/UI, domain migrations, JWT/IdP, trust, probes và định danh image/model/reference.
4. **Tính đúng xuyên suốt:** chạy một lát cắt ứng dụng và các tình huống lỗi để
   kiểm lineage, alert lifecycle, RAG fallback, hướng dẫn an toàn và phục hồi.
5. **Năng lực ở từng profile:** đo nguồn, ML, DB, Relay, Storage và UI, cùng latency
   toàn luồng. Công bố throughput, lag/tuổi dữ liệu, tỷ lệ alert hiển thị, thời
   gian phục hồi và tài nguyên của LOAD-D5, LOAD-N100 rồi LOAD-X.
6. **Vận hành dài hơn và cloud:** review retention, số object/batching, restore,
   tương thích schema và kế hoạch chuyển bản phát hành/cấu hình production
   trước khi kích hoạt môi trường cloud.

Thư viện Kafka và các kiểm thử độc lập hiện có giúp kiểm chứng một phần cơ chế
truyền thông điệp, giữ thứ tự và lưu bền vững. Kết quả Recall/Precision, chất
lượng RAG, p95 đến UI hoặc năng lực 100 readings/s vẫn cần bằng chứng riêng của
ứng dụng tích hợp. Phạm vi hiện thực được tổng hợp ngay tại phần 03, mục 8;
kết quả thực nghiệm sau này cần gắn với bài kiểm thử và môi trường cụ thể.
