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

- **Số liệu từ `idempotency.json` (thử nghiệm trên `trip-00003` có 60 khung hình):**
  - Trước khi chạy lại: 1246 dòng, dấu vân tay `6c7480dfde7e`.
  - Sau 2 lần chạy lại theo cơ chế của pipeline (Delta overwrite theo partition `trip_id`): vẫn đúng **1246 dòng**, dấu vân tay **`6c7480dfde7e`** (`unchanged: true`, không thay đổi bất kỳ byte nào).
  - Khi mô phỏng cách ghi sai (`append` liên tiếp): partition có 60 khung hình bị nhân đôi thành **120 dòng** (2x) sau 2 lần chạy, gây trùng lặp dữ liệu nghiêm trọng.

- **Vì sao Delta `overwrite` theo partition an toàn hơn xoá rồi ghi lại:**
  - **Tính nguyên tử (ACID Atomic Commit)**: Delta Lake ghi các file parquet mới trước, sau đó commit một phiên bản mới vào Delta Log trong một giao dịch nguyên tử (all-or-nothing).
  - **Khả năng chống chịu lỗi (Crash Resilience & Snapshot Isolation)**: Nếu dùng cơ chế truyền thống "xoá rồi ghi lại" (DELETE rồi INSERT), khi tiến trình bị sập (kill task, đứt mạng, cúp điện) giữa hai thao tác hoặc đang ghi dở, partition sẽ bị mất trắng dữ liệu hoặc rơi vào trạng thái hỏng một phần (partial write). Với Delta overwrite, nếu tác vụ gặp sự cố giữa chừng, transaction bị hủy bỏ và không được commit; các truy vấn của độc giả (readers) vẫn luôn đọc được snapshot toàn vẹn của phiên bản trước đó mà không bị gián đoạn hay mất dữ liệu.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

- **Khung đen vs Khung tối và cách xử lý:**
  - *Khung đen (`is_black`)*: Độ sáng trung bình gần như bằng 0 (lỗi che camera, hỏng cảm biến, mất tín hiệu decode). Không chứa bất kỳ thông tin thị giác nào, pipeline **loại bỏ hoàn toàn** khỏi dataset huấn luyện ở tầng Gold (Drive A có 6 khung, Drive B có 4 khung).
  - *Khung tối (`is_dark` / `dark_kept`)*: Khung hình thiếu sáng nhưng vẫn chứa đặc trưng hình học môi trường (xe chạy qua đường hầm, dưới bóng râm, chập tối). Drive B có tới 61 khung tối. Pipeline **gắn cờ chất lượng nhưng vẫn giữ lại (`dark_kept`)** đưa vào dataset.
  - *Đồng ý với cách xử lý*: Hoàn toàn đồng ý. Xe tự lái trong thế giới thực phải vận hành được trong cả điều kiện thiếu sáng. Giữ lại khung tối giúp mô hình học cách thích ứng với môi trường ánh sáng yếu; trong khi khung đen là lỗi rác phần cứng, nếu giữ lại sẽ làm giảm độ chính xác mô hình.

- **Bảng số khung trùng theo ngưỡng dHash (khoảng cách bit khác biệt tối đa):**

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

- **Lựa chọn ngưỡng và sự phụ thuộc vào tốc độ xe:**
  - **Chọn ngưỡng 2 (ngưỡng pipeline sử dụng)**:
    - Ngưỡng 0 (chỉ trùng tuyệt đối): Quá khắt khe, bỏ sót nhiều khung thực tế là trùng (ví dụ Drive A có camera đứng hình 6s và xe chạy chậm tạo ra 149 khung gần như tĩnh nhưng ngưỡng 0 chỉ bắt được 61 khung do nhiễu hạt cảm biến - sensor noise).
    - Ngưỡng 6 (cho phép khác 6 bit): Quá lỏng, loại tới 363 khung ở A và 244 khung ở B (hơn một nửa tổng số khung), gây nguy cơ loại bỏ nhầm cả các khung xe đang di chuyển thực sự có thay đổi bối cảnh nhẹ.
    - Ngưỡng 2 là điểm cân bằng tối ưu giữa việc lọc khung đứng hình/chờ đèn đỏ và bảo toàn độ đa dạng của dữ liệu huấn luyện.
  - **Phụ thuộc vào tốc độ xe**:
    - Khi xe chạy tốc độ cao (cao tốc): Sự thay đổi giữa các khung hình liên tiếp rất lớn (khoảng cách dHash cao), hầu như không có khung nào bị nhận nhầm là trùng lặp.
    - Khi xe chạy chậm hoặc dừng chờ (đô thị, kẹt xe): Khung hình liên tiếp có sai khác thị giác rất nhỏ (0–2 bit). Do đó, phân tích trùng lặp dHash hiệu quả nhất khi kết hợp với tín hiệu tốc độ xe từ telemetry (`speed_mps ≈ 0`).

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

- **Kết quả từ `leakage.json` (tổng 1019 khung hình đạt chuẩn thuộc 26 trip):**
  - **Chia theo khung hình (ngẫu nhiên 70/15/15)**: Có tới **26/26 trip bị chia cắt** (`per_frame_split_trips_straddling = 26`), và **26/26 trip có khung hình xuất hiện ở cả tập train lẫn tập test** (`per_frame_split_trips_in_both_train_and_test = 26`).
  - **Chia theo trip (phương pháp của pipeline)**: **0 trip bị chia cắt** (`per_trip_split_trips_straddling = 0`). Mỗi trip nằm trọn vẹn trong duy nhất 1 split (train: 18 trip, val: 4 trip, test: 4 trip).

- **Vì sao chia theo khung hình làm điểm kiểm thử đẹp một cách giả tạo:**
  - Video được lấy mẫu liên tục (2 fps, mỗi khung cách nhau 0.5s). Các khung hình liền kề trong cùng một chuyến đi có mối tương quan thời gian cực kỳ cao (temporal correlation) — cùng bối cảnh cung đường, thời tiết, ánh sáng, vật thể xung quanh gần như giống hệt nhau.
  - Nếu chia ngẫu nhiên theo khung hình, mô hình học máy ở tập Train đã "học thuộc lòng" các bối cảnh của tập Test (rò rỉ dữ liệu / Data Leakage). Điều này giống như học sinh đi thi làm lại chính các câu hỏi đã được làm ở nhà. Điểm kiểm thử (accuracy/mAP) trên tập Test sẽ cao giả tạo, nhưng khi đưa mô hình ra triển khai trên xe thực tế gặp một cung đường mới lạ, mô hình sẽ phán đoán sai nghiêm trọng.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

- **Vì sao là băm nội dung (Content-addressed Hash):**
  - Đảm bảo **tính tất định và idempotent**: `dataset_version` được tính bằng 12 ký tự đầu mã băm SHA256 của toàn bộ danh sách `(frame_id, split)` sắp xếp theo thứ tự. Nếu cùng dữ liệu và cùng cách phân chia, dù chạy lại bao nhiêu lần thì mã băm vẫn bất biến.
  - Tránh lãng phí và nhầm lẫn: Nếu dùng ngày giờ (`2026-10-12`) hay số đếm (`v1, v2`), mỗi lần kích hoạt pipeline lại sinh ra một version mới dù dữ liệu không hề thay đổi. Ngược lại, với băm nội dung, chỉ cần thay đổi 1 khung hình hoặc đổi split của 1 trip thì mã băm lập tức đổi, phản ánh chính xác sự thay đổi của tập dữ liệu.
- **Hai lần chạy liên tiếp cho kết quả gì:**
  - Cả hai lần đều cho ra chính xác cùng một mã băm `dataset_version` là **`e36471b76b8f`**.
- **Vì sao `dataset_version` khác bạn bè dù cùng dữ liệu:**
  - Thuật toán phân chia split của pipeline sử dụng `LAB_STUDENT_ID` (MSSV = `2A202602978`) làm muối băm (salt) trong hàm băm MD5 để sắp xếp ngẫu nhiên có kiểm soát các trip trước khi gán vào train/val/test.
  - Mỗi sinh viên có MSSV riêng nên thứ tự phân chia trip vào các split là duy nhất, dẫn đến tập các cặp `(frame_id, split)` khác nhau, sinh ra `dataset_version` hoàn toàn riêng biệt.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

- **Dataset này được tạo từ những dữ liệu nào (theo trường `lineage` trong `manifest.json`):**
  - 2 file video gốc: `drive_a` và `drive_b` được định danh bằng mã băm SHA256 nguyên bản trong `input_video_sha256`.
  - Bảng metadata 26 chuyến đi: `bronze.trips_raw` (version 0).
  - Hai bảng telemetry gốc: `bronze.raw_telemetry_a` và `bronze.raw_telemetry_b` (version 0).
  - Bảng video metadata: `bronze.video_files` (version 0).
  - Các bảng tầng Silver: `silver.frames` (1246 frames, version 0), `silver.telemetry` (6149 dòng sạch, version 0), `silver.frames_flagged` (version 0), `silver.frame_telemetry` (version 0).
  - Áp dụng các quy tắc lọc ở `rules`: loại bỏ các khung có cờ `is_black`, `is_blurry`, `is_near_dup`, và `sync_lost`.

- **Thông tin giúp tái tạo lại chính xác dataset này sau 3 tháng:**
  1. Mã định danh phiên bản `dataset_version` (`e36471b76b8f`) và mã `manifest_sha256`.
  2. Hai video gốc có đúng mã `sha256` đã ghi nhận trong lineage.
  3. Số hiệu phiên bản (commit version) của từng bảng Delta ở Bronze và Silver được ghi trong `lineage` (dùng tính năng Delta Lake Time Travel để truy xuất chính xác trạng thái dữ liệu tại commit đó).
  4. MSSV `2A202602978` và luật chia split quy định trong `rules` (`per trip, stratified by drive, md5 order`).

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
