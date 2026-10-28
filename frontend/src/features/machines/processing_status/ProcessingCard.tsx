import type { Processing } from "../../../types/live";
import { Badge, date, Empty, Panel, Source } from "../../../shared/ui";

export function ProcessingCard({ value }: { value: Processing | null }) {
  return (
    <Panel
      title="Latest Processing"
      eyebrow="03 / CURRENT READING STATUS"
      action={value && <Badge value={value.processing_status} />}
    >
      {!value ? (
        <Empty>Chưa có trạng thái xử lý.</Empty>
      ) : (
        <div className="processing-grid">
          <Source label="Event đang xử lý" value={value.source_event_id} />
          <Source label="Kiểm tra dữ liệu" value={value.validation_status} />
          <Source label="Cập nhật lúc" value={date(value.updated_at)} />
          {value.failure_code && (
            <div className="notice error">
              Inference unavailable · {value.failure_code}
            </div>
          )}
          <p className="hint">
            Trạng thái xử lý của reading mới không xóa prediction hợp lệ trước
            đó.
          </p>
        </div>
      )}
    </Panel>
  );
}
