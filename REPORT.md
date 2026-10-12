# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `<điền: Họ và tên của bạn>` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602978 |
| GitHub | ductrieunguyen897-code |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `e36471b76b8f`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `759 / 118 / 142` (tổng 1019)
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Dựa trên kết quả thực thi lệnh `scripts/lab.sh step 1 --reset`, 4 khác biệt chính giữa telemetry nguồn A (3021 dòng) và nguồn B (3234 dòng):
1. **Tên cột**: Nguồn A dùng `ts_utc`, `speed_mps`, `heading_deg`, `lat`, `lon`, `alt_m`, `gas`, `brake`, `steer`; trong khi nguồn B dùng `timestamp_ms`, `spd_kmh`, `hdg`, `latitude`, `longitude`, `altitude`, `throttle_pct`, `brake_pct`, `steering`.
2. **Đơn vị đo lường**: Tốc độ ở nguồn A là m/s (`speed_mps`), nguồn B là km/h (`spd_kmh`). Bàn đạp ga/phanh ở A theo thang chuẩn hóa [0..1] (`gas`, `brake`), ở B theo phần trăm [0..100%] (`throttle_pct`, `brake_pct`).
3. **Định dạng thời gian**: Nguồn A dùng chuỗi ISO 8601 UTC (`2025-10-07T08:00:00.014Z`), nguồn B dùng số nguyên epoch timestamp dạng milliseconds (`1759933801509`).
4. **Đồng hồ cảm biến**: Nguồn B có độ lệch đồng hồ (clock offset) so với video cần được hiệu chỉnh, trong khi nguồn A đã khớp thời gian với video.

**Vì sao pipeline giữ mỗi nguồn một bảng bronze và để mọi cột ở dạng chữ:**
- **Bảo toàn dữ liệu thô nguyên bản (Raw Ingestion / Auditability)**: Theo kiến trúc Medallion (Slide 14), lớp Bronze có nhiệm vụ lưu đúng và đủ 100% những gì thiết bị gửi lên mà không can thiệp biến đổi để phục vụ kiểm toán, truy vết nguồn gốc và debug.
- **Tránh mất mát dữ liệu do Schema Drift & Type Casting**: Nếu ép kiểu (cast) ngay tại Bronze, các bản ghi dị thường (ví dụ text trong cột số, chuỗi thời gian sai format, giá trị rỗng bất thường) sẽ gây lỗi sập pipeline hoặc bị âm thầm ép thành `NULL` làm mất dữ liệu gốc. Giữ kiểu text đảm bảo ingestion luôn thành công.
- **Tách rời sự phụ thuộc (Decoupling)**: Các thiết bị/nhà cung cấp telemetry khác nhau có tần suất cập nhật firmware và schema khác nhau. Tách riêng bảng bronze cho từng nguồn giúp việc ingest độc lập, không làm hỏng pipeline chung khi một thiết bị thay đổi cấu trúc.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

**Cách pipeline đưa hai nguồn về một schema:**
- **Đơn vị tốc độ**: Nguồn B đo bằng km/h được chuyển đổi sang m/s (`spd_kmh / 3.6`) để đồng nhất với nguồn A.
- **Kiểu thời gian**: Nguồn B đo bằng epoch ms được đổi sang UTC timestamp (`to_timestamp(timestamp_ms / 1000.0)`).
- **Độ lệch đồng hồ**: Telemetry nguồn B bị lệch 1.5 giây so với video được tự động trừ đi giá trị `telemetry_clock_offset_ms` (1500 ms) khai báo trong `sources.json`.
- **Bằng chứng đã đúng**: Thời điểm bắt đầu của telemetry B sau hiệu chỉnh là `2025-10-08 14:30:00.009000+00:00`, khớp chính xác với thời điểm bắt đầu của video B (`2025-10-08 14:30:00`). Tốc độ tối đa hợp lý: Drive A đạt 71.0 km/h, Drive B đạt 100.0 km/h (đều ≤ 130 km/h).

**Bảng quarantine (dòng bị cách ly theo lý do):**
```
drive_id  reject_reason       so_dong
--------  ------------------  -------
drive_a   duplicate           20     
drive_a   speed_missing       30     
drive_a   speed_out_of_range  3      
drive_b   duplicate           20     
drive_b   speed_missing       30     
drive_b   speed_out_of_range  3      
```
Tổng cộng có 106 dòng bị cách ly (53 dòng mỗi drive). Dữ liệu được bảo toàn tuyệt đối:
- Drive A: 2968 dòng sạch + 53 dòng cách ly = 3021 dòng raw.
- Drive B: 3181 dòng sạch + 53 dòng cách ly = 3234 dòng raw.

**Vì sao dòng lỗi vào `rejects` thay vì bị xoá:**
Tránh tình trạng mất dữ liệu im lặng (silent data loss). Việc lưu vào bảng `silver.silver_telemetry_rejects` kèm cột `reject_reason` giúp đội ngũ kỹ sư giám sát được tỉ lệ suy giảm chất lượng dữ liệu (data quality SLA), biết được cảm biến nào đang lỗi để bảo trì, và có thể audit hoặc replay/sửa lại dữ liệu khi cần thiết mà không phá hủy thông tin gốc.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

- **Vì sao `max_gap_ms` là `warn` còn `in_range` là `error`**:
  - `max_gap_ms` (khoảng cách giữa các mẫu telemetry > 1000 ms) đặt mức `warn` vì trong thực tế xe di chuyển ngoài môi trường thực, hiện tượng cảm biến rớt tín hiệu tạm thời (dropout) là khiếm khuyết được chấp nhận (tolerated defect); chỉ báo để theo dõi chứ không được dừng cả pipeline sản xuất.
  - `in_range` (tốc độ trong khoảng 0..70 m/s, tức tối đa 252 km/h) đặt mức `error` vì đây là điều kiện vật lý bắt buộc của xe; nếu tốc độ âm hoặc vượt quá 252 km/h thì đó là dữ liệu rác/hư hại nặng (corrupt data). Dữ liệu này nếu lọt vào Gold sẽ phá hỏng mô hình thị giác/tự lái (downstream ML), do đó bắt buộc phải dừng pipeline để bảo vệ chất lượng dữ liệu.

- **Kết quả thí nghiệm `break-gate` (`break_gate.json`)**:
  - Khi nới luật làm sạch từ 70 lên 1000 m/s, cổng chất lượng phát hiện lỗi ngay: test `in_range_silver_telemetry_speed_mps__70__0` bị **FAIL** và bắt được đúng **6 dòng** vi phạm (mỗi drive 3 dòng).
  - Tổng kết test dbt chuyển thành: `PASS=5 WARN=1 ERROR=1 SKIP=0`. Khi có `ERROR=1`, toàn bộ các task xử lý sau trong Airflow DAG sẽ bị chặn lại (`upstream_failed`).
  - Sau khi khôi phục luật chuẩn: test trở lại trạng thái `PASS=6 WARN=1 ERROR=0 SKIP=0`.
  - **Ý nghĩa của Cổng chất lượng (Quality Gate)**: Đóng vai trò như một "chốt chặn an toàn" độc lập giữa các tầng dữ liệu. Ngay cả khi code biến đổi dữ liệu (ETL/cleaning logic) bị thay đổi ngoài ý muốn hoặc bị nới lỏng lỗi thời, Quality Gate vẫn chặn đứng dữ liệu bẩn trước khi nó kịp lọt vào tầng Gold và Warehouse.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
