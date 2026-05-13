# Hướng dẫn test end-to-end Databricks Order Pipeline

Tài liệu này là runbook kiểm thử từ đầu đến cuối cho project `databricks-order-pipeline`. Mục tiêu là đi qua toàn bộ các tính năng đã implement: Bronze ingestion mode, schema contract, Silver validation/quarantine, mutable order merge, Gold incremental aggregate, stage audit, alert, maintenance best-effort, retention policy và Databricks Asset Bundle.

## 1. Phạm vi kiểm thử

E2E test cần xác nhận các nhóm tính năng sau:

| Nhóm | Cần kiểm tra |
|---|---|
| Build | Project compile và package được bằng Maven |
| Unit/integration test | Test class hiện có chạy pass hoặc skip hợp lệ trên Windows thiếu `winutils` |
| Bronze local/test | `pipeline.ingestion.mode=json` dùng Spark JSON reader |
| Bronze Databricks | `pipeline.ingestion.mode=cloudFiles` dùng Databricks Auto Loader |
| Schema contract | Order event có `orderId`, `customerId`, `amount`, `timestamp`, `updatedAt`, `eventVersion` |
| Silver validation | Trim string, validate ID, parse timestamp, tách valid/invalid |
| Quarantine | Invalid records được ghi vào quarantine kèm `rejection_reason` |
| Mutable order | Update theo `updatedAt` / `eventVersion`, event cũ không overwrite event mới |
| Gold incremental | Chỉ aggregate ngày bị ảnh hưởng, merge theo `order_date`, partition theo `order_date` |
| Observability | Count input/output/rejected/rescued từng stage, audit table, alert bất thường |
| Maintenance | `OPTIMIZE` / `VACUUM` best-effort, audit riêng, retention policy rõ ràng |
| Security | Table name trusted, validate identifier, chặn override table name tùy ý |
| Bundle | Validate/deploy/run Databricks Asset Bundle |

## 2. Điều kiện tiên quyết

### 2.1. Local machine

Cần có:

- Java 17.
- Maven.
- PowerShell.
- Databricks CLI nếu muốn test bundle/prod.
- Git chỉ cần nếu muốn commit/push thay đổi.

Kiểm tra nhanh:

```powershell
java -version
mvn -version
databricks -v
```

Nếu `databricks -v` không có, vẫn có thể chạy local test, chỉ bỏ qua phần Databricks.

### 2.2. Lưu ý Windows và Delta Lake

Một số test Delta local có thể bị skip trên Windows nếu thiếu `HADOOP_HOME` hoặc `winutils.exe`. Đây là behavior đã được xử lý trong test hiện tại.

Nếu muốn chạy đầy đủ local Delta test trên Windows, cần cấu hình Hadoop native utilities. Nếu không, ưu tiên chạy Databricks E2E cho phần Delta integration.

## 3. Cấu trúc stage cần test

Ứng dụng nhận stage đầu tiên từ command line:

| Stage | Ý nghĩa |
|---|---|
| `bronze` | Đọc raw JSON vào Bronze Delta table |
| `silver` | Validate Bronze, tách valid/invalid, merge valid vào Silver |
| `gold` | Aggregate Silver theo ngày vào Gold |
| `maintenance` | Chạy maintenance best-effort cho các Delta tables |
| `all` | Chạy tuần tự `bronze -> silver -> gold -> maintenance` |

Nếu không truyền stage, app dùng mặc định `all`.

Entrypoint chính:

- `src/main/java/com/company/orderpipeline/OrderPipelineApplication.java`

Service chính:

- `src/main/java/com/company/orderpipeline/service/OrderTransformationService.java`

## 4. Property quan trọng trong test

### 4.1. Bronze ingestion mode

| Property | Giá trị | Ý nghĩa |
|---|---|---|
| `pipeline.ingestion.mode` | `json` | Local/test mode, dùng Spark JSON reader |
| `pipeline.ingestion.mode` | `cloudFiles` | Databricks mode, dùng Auto Loader |

### 4.2. Core table paths/names

| Property | Ý nghĩa |
|---|---|
| `pipeline.orders.raw.dir` | Raw input directory |
| `pipeline.orders.bronze.table` | Bronze Delta table |
| `pipeline.orders.silver.table` | Silver Delta table |
| `pipeline.orders.gold.table` | Gold Delta table |
| `pipeline.orders.quarantine.table` | Quarantine/bad records table |
| `pipeline.checkpoint.bronze` | Structured Streaming checkpoint cho Bronze |

### 4.3. Contract và production controls

| Property | Ý nghĩa |
|---|---|
| `pipeline.production.mode` | Bật/tắt production behavior |
| `pipeline.contract.require-updated-at` | Bắt buộc `updatedAt` |
| `pipeline.contract.require-event-version` | Bắt buộc `eventVersion` |
| `pipeline.alert.rescued-record-threshold` | Ngưỡng alert rescued records |

### 4.4. Stage audit và anomaly alert

| Property | Ý nghĩa |
|---|---|
| `pipeline.stage.audit.table` | Table ghi audit từng stage |
| `pipeline.stage.alert.on-anomaly` | Bật alert count bất thường |
| `pipeline.stage.alert.min-output-ratio` | Ngưỡng output/input tối thiểu |
| `pipeline.stage.alert.max-rejected-count` | Ngưỡng rejected records tối đa |

### 4.5. Maintenance policy

| Property | Ý nghĩa |
|---|---|
| `pipeline.maintenance.audit.table` | Table audit maintenance |
| `pipeline.maintenance.alert.on-failure` | Alert nếu maintenance fail |
| `pipeline.maintenance.vacuum-retention-hours` | Retention cho VACUUM |
| `pipeline.maintenance.audit-replay-required` | Có yêu cầu audit/replay không |
| `pipeline.maintenance.slow-downstream-max-lag-hours` | Downstream lag tối đa cần bảo vệ |

## 5. Chuẩn bị thư mục test local

Chạy từ repo root:

```powershell
$Root = "C:\Users\thanh\databricks-order-pipeline"
$E2E = "C:\tmp\order-pipeline-e2e"

Remove-Item $E2E -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force "$E2E\raw" | Out-Null
New-Item -ItemType Directory -Force "$E2E\raw_update" | Out-Null
New-Item -ItemType Directory -Force "$E2E\checkpoints" | Out-Null

Copy-Item "$Root\orders.json" "$E2E\raw\orders.json" -Force
Copy-Item "$Root\orders1.json" "$E2E\raw\orders1.json" -Force
```

Tạo thêm invalid records để test quarantine:

```powershell
@'
{"orderId":"   ","customerId":"c_bad_1","amount":10.5,"timestamp":"2026-05-12T10:00:00Z","updatedAt":"2026-05-12T10:05:00Z","eventVersion":1}
{"orderId":"o_bad_customer","customerId":"   ","amount":20.0,"timestamp":"2026-05-12T11:00:00Z","updatedAt":"2026-05-12T11:05:00Z","eventVersion":1}
{"orderId":"o_bad_ts","customerId":"c_bad_3","amount":30.0,"timestamp":"not-a-timestamp","updatedAt":"2026-05-12T12:05:00Z","eventVersion":1}
{"orderId":"o_missing_version","customerId":"c_bad_4","amount":40.0,"timestamp":"2026-05-12T13:00:00Z","updatedAt":"2026-05-12T13:05:00Z"}
'@ | Set-Content "$E2E\raw\invalid_orders.json"
```

Tạo records để test mutable order:

```powershell
@'
{"orderId":"o_update_1","customerId":"c_update","amount":100.0,"timestamp":"2026-05-13T09:00:00Z","updatedAt":"2026-05-13T09:05:00Z","eventVersion":2}
{"orderId":"o_update_1","customerId":"c_update","amount":999.0,"timestamp":"2026-05-13T09:00:00Z","updatedAt":"2026-05-13T09:01:00Z","eventVersion":1}
{"orderId":"o_update_1","customerId":"c_update","amount":150.0,"timestamp":"2026-05-13T09:00:00Z","updatedAt":"2026-05-13T09:10:00Z","eventVersion":3}
'@ | Set-Content "$E2E\raw_update\updates.json"
```

## 6. Build và test tự động hiện có

### 6.1. Compile

```powershell
mvn -q -DskipTests compile
```

Kỳ vọng:

- Exit code `0`.
- Không có lỗi compile.

### 6.2. Chạy test class chính

```powershell
mvn -q -Dtest=OrderTransformationServiceTest test
```

Kỳ vọng:

- Test pass.
- Hoặc một số Delta local test skip hợp lệ trên Windows nếu thiếu Hadoop native utilities.

### 6.3. Package

```powershell
mvn clean package
```

Kỳ vọng:

- Có JAR trong `target/`.
- Dùng được file `target\orderpipeline-0.0.1-SNAPSHOT.jar` cho các bước sau.

## 7. Test Bronze local JSON mode

Chạy Bronze với `pipeline.ingestion.mode=json`:

```powershell
java -jar target\orderpipeline-0.0.1-SNAPSHOT.jar bronze `
  --spring.profiles.active=local `
  --pipeline.ingestion.mode=json `
  --pipeline.orders.raw.dir=file:///C:/tmp/order-pipeline-e2e/raw/ `
  --pipeline.orders.bronze.table=default.bronze_orders_e2e `
  --pipeline.checkpoint.bronze=file:///C:/tmp/order-pipeline-e2e/checkpoints/bronze `
  --pipeline.stage.audit.table=default.stage_audit_e2e
```

Kỳ vọng:

- Log có thông tin dùng local/test JSON ingestion mode.
- Bronze table `default.bronze_orders_e2e` được tạo hoặc append.
- Record có thêm `ingest_timestamp`.
- Stage audit có row cho stage `bronze`.
- `output_records` lớn hơn `0` nếu input được đọc.

Điểm cần lưu ý:

- Bronze là streaming write với `Trigger.AvailableNow()`.
- Nếu dùng lại checkpoint cũ, file đã xử lý có thể không được đọc lại.
- Khi muốn re-run từ đầu, xóa checkpoint hoặc dùng checkpoint path mới.

## 8. Test Silver validation và quarantine

Chạy Silver với production contract strict để test đủ validation:

```powershell
java -jar target\orderpipeline-0.0.1-SNAPSHOT.jar silver `
  --spring.profiles.active=local `
  --pipeline.production.mode=true `
  --pipeline.contract.require-updated-at=true `
  --pipeline.contract.require-event-version=true `
  --pipeline.orders.bronze.table=default.bronze_orders_e2e `
  --pipeline.orders.silver.table=default.silver_orders_e2e `
  --pipeline.orders.quarantine.table=default.quarantine_orders_e2e `
  --pipeline.stage.audit.table=default.stage_audit_e2e `
  --pipeline.alert.rescued-record-threshold=1 `
  --pipeline.stage.alert.on-anomaly=true `
  --pipeline.stage.alert.max-rejected-count=0
```

Kỳ vọng Silver:

- Valid records được merge vào `default.silver_orders_e2e`.
- String fields được trim.
- `orderId` không được blank.
- `customerId` không được blank.
- `timestamp` được parse rõ ràng trước khi tạo `order_date`.
- Records timestamp invalid bị loại sau parse.
- `updatedAt` được parse thành `updated_at`.
- `eventVersion` được map thành `event_version`.
- Nếu cùng `orderId`, chỉ latest event được giữ theo update semantics.

Kỳ vọng Quarantine:

- Invalid records được ghi vào `default.quarantine_orders_e2e`.
- Có `rejection_reason`.
- Có `rejection_category`.
- Có `quarantined_at`.
- Các record test invalid nên tạo ra reason tương ứng với blank `orderId`, blank `customerId`, invalid timestamp hoặc missing `eventVersion`.

Kỳ vọng Observability:

- Log có stage summary.
- Stage audit có row `silver`.
- Vì `pipeline.stage.alert.max-rejected-count=0`, nếu có rejected records thì log phải có alert rejected count.

## 9. Test mutable order update

Bước này kiểm tra event mới hơn thắng event cũ hơn.

### 9.1. Ingest update batch vào Bronze

Dùng checkpoint khác để chắc chắn đọc batch update:

```powershell
java -jar target\orderpipeline-0.0.1-SNAPSHOT.jar bronze `
  --spring.profiles.active=local `
  --pipeline.ingestion.mode=json `
  --pipeline.orders.raw.dir=file:///C:/tmp/order-pipeline-e2e/raw_update/ `
  --pipeline.orders.bronze.table=default.bronze_orders_e2e `
  --pipeline.checkpoint.bronze=file:///C:/tmp/order-pipeline-e2e/checkpoints/bronze_update `
  --pipeline.stage.audit.table=default.stage_audit_e2e
```

### 9.2. Chạy lại Silver

```powershell
java -jar target\orderpipeline-0.0.1-SNAPSHOT.jar silver `
  --spring.profiles.active=local `
  --pipeline.production.mode=true `
  --pipeline.contract.require-updated-at=true `
  --pipeline.contract.require-event-version=true `
  --pipeline.orders.bronze.table=default.bronze_orders_e2e `
  --pipeline.orders.silver.table=default.silver_orders_e2e `
  --pipeline.orders.quarantine.table=default.quarantine_orders_e2e `
  --pipeline.stage.audit.table=default.stage_audit_e2e
```

Kỳ vọng:

- Với `orderId = o_update_1`, latest state là event version `3`.
- `amount = 150.0`.
- Event version `1` với amount `999.0` không được overwrite version `3`.
- `updated_at` tương ứng timestamp mới nhất `2026-05-13T09:10:00Z`.

## 10. Test Gold incremental aggregate

Chạy Gold:

```powershell
java -jar target\orderpipeline-0.0.1-SNAPSHOT.jar gold `
  --spring.profiles.active=local `
  --pipeline.orders.silver.table=default.silver_orders_e2e `
  --pipeline.orders.gold.table=default.gold_daily_metrics_e2e `
  --pipeline.stage.audit.table=default.stage_audit_e2e `
  --pipeline.stage.alert.on-anomaly=true `
  --pipeline.stage.alert.min-output-ratio=0.1
```

Kỳ vọng:

- Gold table `default.gold_daily_metrics_e2e` được tạo hoặc merge.
- Gold có cột `order_date`, không dùng `timestamp` cho daily metric date.
- Aggregate theo ngày.
- Nếu Gold table được tạo mới, partition theo `order_date`.
- Khi chạy lại sau update chỉ thuộc một số ngày, pipeline chỉ recompute affected dates.
- Merge vào Gold theo `order_date`, không append duplicate cùng ngày.

Các case cần nhìn bằng SQL/Spark shell/Databricks:

```sql
SELECT *
FROM default.gold_daily_metrics_e2e
ORDER BY order_date;
```

Kỳ vọng có ngày từ sample input và ngày `2026-05-13` từ `o_update_1`.

## 11. Test stage audit table

Kiểm tra audit table:

```sql
SELECT stage_name,
       input_records,
       output_records,
       rejected_records,
       rescued_records,
       status,
       details
FROM default.stage_audit_e2e
ORDER BY run_timestamp DESC;
```

Kỳ vọng:

- Có stage `bronze`.
- Có stage `silver`.
- Có stage `gold`.
- `bronze.input_records` có thể là `-1` vì streaming source không có count trực tiếp.
- `silver.rejected_records` phản ánh số record invalid/quarantine.
- `rescued_records` phản ánh rescued/corrupt behavior tùy mode.
- `status = SUCCESS` cho stage chạy thành công.

## 12. Test alert count bất thường

Có hai cách test alert nhanh.

### 12.1. Alert rejected count

Đã bật ở bước Silver:

```properties
pipeline.stage.alert.max-rejected-count=0
```

Nếu có ít nhất một rejected record, log phải có alert dạng rejected count exceeded threshold.

### 12.2. Alert output ratio thấp

Dùng input toàn invalid, rồi chạy Silver với:

```properties
pipeline.stage.alert.min-output-ratio=0.9
```

Kỳ vọng nếu input nhiều nhưng output valid thấp, log có alert output ratio below threshold.

## 13. Test maintenance best-effort

Chạy maintenance:

```powershell
java -jar target\orderpipeline-0.0.1-SNAPSHOT.jar maintenance `
  --spring.profiles.active=local `
  --pipeline.orders.bronze.table=default.bronze_orders_e2e `
  --pipeline.orders.silver.table=default.silver_orders_e2e `
  --pipeline.orders.gold.table=default.gold_daily_metrics_e2e `
  --pipeline.orders.quarantine.table=default.quarantine_orders_e2e `
  --pipeline.maintenance.audit.table=default.maintenance_audit_e2e `
  --pipeline.maintenance.alert.on-failure=true `
  --pipeline.maintenance.vacuum-retention-hours=168 `
  --pipeline.maintenance.audit-replay-required=true `
  --pipeline.maintenance.slow-downstream-max-lag-hours=24
```

Kỳ vọng:

- Maintenance là best-effort.
- Lỗi `OPTIMIZE` hoặc `VACUUM` không làm hỏng toàn bộ pipeline chính.
- Audit table `default.maintenance_audit_e2e` được ghi.
- Mỗi table maintenance có audit row riêng.
- `status` có thể là `SUCCESS`, `SKIPPED` hoặc `FAILED`.
- Nếu thiếu quyền/lệnh không hỗ trợ local, phải được log/audit thay vì im lặng.

Kiểm tra audit:

```sql
SELECT table_name,
       status,
       permission_failure,
       retention_policy_risk,
       vacuum_retention_hours,
       audit_replay_required,
       slow_downstream_max_lag_hours,
       error_class,
       error_message
FROM default.maintenance_audit_e2e
ORDER BY run_timestamp DESC;
```

## 14. Test retention policy risk

Chạy maintenance với retention thấp:

```powershell
java -jar target\orderpipeline-0.0.1-SNAPSHOT.jar maintenance `
  --spring.profiles.active=local `
  --pipeline.orders.bronze.table=default.bronze_orders_e2e `
  --pipeline.orders.silver.table=default.silver_orders_e2e `
  --pipeline.orders.gold.table=default.gold_daily_metrics_e2e `
  --pipeline.orders.quarantine.table=default.quarantine_orders_e2e `
  --pipeline.maintenance.audit.table=default.maintenance_audit_e2e `
  --pipeline.maintenance.alert.on-failure=true `
  --pipeline.maintenance.vacuum-retention-hours=12 `
  --pipeline.maintenance.audit-replay-required=true `
  --pipeline.maintenance.slow-downstream-max-lag-hours=24
```

Kỳ vọng:

- Audit ghi `vacuum_retention_hours = 12`.
- `retention_policy_risk = true` nếu policy này vi phạm yêu cầu audit/replay/downstream lag.
- Có log cảnh báo nếu alert bật.

## 15. Test table name safety

Ứng dụng chỉ nên nhận table names từ config trusted và validate format identifier.

### 15.1. Test command line override bị chặn

Thử override core table name từ CLI:

```powershell
java -jar target\orderpipeline-0.0.1-SNAPSHOT.jar silver `
  --spring.profiles.active=local `
  --pipeline.orders.silver.table=default.orders_bad
```

Kỳ vọng:

- App fail sớm vì core table names không được override tùy ý từ external CLI.
- Không chạy SQL merge vào table override này.

### 15.2. Test invalid trusted table name trong maintenance

Chạy lại unit test:

```powershell
mvn -q -Dtest=OrderTransformationServiceTest#testPerformMaintenanceAuditsInvalidTrustedTableName test
```

Kỳ vọng:

- Invalid identifier bị audit `FAILED`.
- `error_class = IllegalArgumentException`.
- Error message nói về trusted table identifier.

## 16. Test full local flow bằng stage all

Sau khi đã test từng stage, chạy full flow:

```powershell
java -jar target\orderpipeline-0.0.1-SNAPSHOT.jar all `
  --spring.profiles.active=local `
  --pipeline.ingestion.mode=json `
  --pipeline.production.mode=true `
  --pipeline.contract.require-updated-at=true `
  --pipeline.contract.require-event-version=true `
  --pipeline.orders.raw.dir=file:///C:/tmp/order-pipeline-e2e/raw/ `
  --pipeline.orders.bronze.table=default.bronze_orders_e2e_all `
  --pipeline.orders.silver.table=default.silver_orders_e2e_all `
  --pipeline.orders.gold.table=default.gold_daily_metrics_e2e_all `
  --pipeline.orders.quarantine.table=default.quarantine_orders_e2e_all `
  --pipeline.checkpoint.bronze=file:///C:/tmp/order-pipeline-e2e/checkpoints/bronze_all `
  --pipeline.stage.audit.table=default.stage_audit_e2e_all `
  --pipeline.stage.alert.on-anomaly=true `
  --pipeline.stage.alert.min-output-ratio=0.1 `
  --pipeline.stage.alert.max-rejected-count=1000 `
  --pipeline.maintenance.audit.table=default.maintenance_audit_e2e_all `
  --pipeline.maintenance.alert.on-failure=true `
  --pipeline.maintenance.vacuum-retention-hours=168 `
  --pipeline.maintenance.audit-replay-required=true `
  --pipeline.maintenance.slow-downstream-max-lag-hours=24
```

Kỳ vọng:

- Chạy đủ thứ tự `bronze -> silver -> gold -> maintenance`.
- Tạo đủ Bronze/Silver/Gold/Quarantine/Stage Audit/Maintenance Audit.
- Không có duplicate bất thường ở Gold theo cùng `order_date`.
- Maintenance lỗi permission/local support nếu có phải được audit riêng.

## 17. Test Databricks Asset Bundle validate

Validate bundle với profile mặc định:

```powershell
databricks bundle validate --profile DEFAULT
```

Validate theo target:

```powershell
databricks bundle validate -t dev --profile DEFAULT
databricks bundle validate -t prod --profile DEFAULT
databricks bundle validate -t prod_sql_mv --profile DEFAULT
```

Kỳ vọng:

- Bundle validate pass.
- Job chính có tasks `ingest_bronze`, `process_silver`, `aggregate_gold`.
- Job maintenance tách riêng khỏi job chạy mỗi 5 phút.
- Main class là `com.company.orderpipeline.OrderPipelineApplication`.

Files cần đối chiếu:

- `databricks.yml`
- `resources/jobs.yml`
- `resources/jobs_sql_mv.yml`
- `resources/sql/gold_daily_metrics_mv.sql`

## 18. Test Databricks deploy/run

### 18.1. Build JAR

```powershell
mvn clean package
```

### 18.2. Deploy bundle

```powershell
databricks bundle deploy -t dev --profile DEFAULT
```

### 18.3. Copy/upload JAR nếu cần

Job đang tham chiếu JAR ở Volume path:

```text
/Volumes/workspace/sales/artifacts/orderpipeline-0.0.1-SNAPSHOT.jar
```

Tùy workspace, có thể cần upload/copy JAR vào Volume đó trước khi run job.

Ví dụ nếu Databricks CLI hỗ trợ path tương ứng:

```powershell
databricks fs cp target/orderpipeline-0.0.1-SNAPSHOT.jar dbfs:/Volumes/workspace/sales/artifacts/orderpipeline-0.0.1-SNAPSHOT.jar --overwrite --profile DEFAULT
```

Nếu lệnh trên không phù hợp với Unity Catalog Volume trong workspace của bạn, upload JAR bằng UI hoặc dùng đúng lệnh CLI tương ứng với version Databricks CLI đang cài.

### 18.4. Run job chính

```powershell
databricks bundle run order_pipeline_job -t dev --profile DEFAULT
```

Kỳ vọng:

- `ingest_bronze` chạy trước.
- `process_silver` chạy sau Bronze.
- `aggregate_gold` chạy sau Silver.
- Không chạy maintenance trong job chính.

### 18.5. Run maintenance job riêng

```powershell
databricks bundle run order_pipeline_maintenance_job -t dev --profile DEFAULT
```

Kỳ vọng:

- Maintenance chạy riêng theo lịch daily trong bundle.
- `OPTIMIZE` / `VACUUM` không chạy mỗi 5 phút.
- Maintenance audit được ghi riêng.

## 19. Test Databricks cloudFiles mode

Trong production profile:

```properties
pipeline.ingestion.mode=cloudFiles
pipeline.production.mode=true
pipeline.contract.require-updated-at=true
pipeline.contract.require-event-version=true
```

Chạy Bronze trên Databricks qua bundle/job.

Kỳ vọng:

- Log có thông tin dùng Databricks Auto Loader.
- Reader dùng format `cloudFiles`.
- `cloudFiles.format=json`.
- `cloudFiles.schemaEvolutionMode=rescue`.
- Schema location nằm dưới checkpoint path.
- Extra fields được đưa vào `_rescued_data` nếu có schema drift.

Test rescued data bằng input có extra field:

```json
{"orderId":"o_rescue_1","customerId":"c_rescue","amount":10.0,"timestamp":"2026-05-14T10:00:00Z","updatedAt":"2026-05-14T10:05:00Z","eventVersion":1,"unexpectedField":"should_go_to_rescued_data"}
```

Chạy với threshold thấp:

```properties
pipeline.alert.rescued-record-threshold=0
```

Kỳ vọng:

- Rescued count > 0.
- Log có alert nếu rescued records vượt threshold.
- Stage audit ghi `rescued_records` tương ứng ở stage liên quan.

## 20. SQL kiểm tra trên Databricks

### 20.1. Count các bảng chính

```sql
SELECT COUNT(*) AS bronze_count FROM workspace.sales.bronze_orders;
SELECT COUNT(*) AS silver_count FROM workspace.sales.silver_orders;
SELECT COUNT(*) AS gold_count FROM workspace.sales.gold_daily_metrics;
SELECT COUNT(*) AS quarantine_count FROM workspace.sales.quarantine_orders;
SELECT COUNT(*) AS stage_audit_count FROM workspace.sales.stage_audit;
SELECT COUNT(*) AS maintenance_audit_count FROM workspace.sales.maintenance_audit;
```

### 20.2. Kiểm tra Gold schema

```sql
DESCRIBE TABLE workspace.sales.gold_daily_metrics;
```

Kỳ vọng:

- Có `order_date`.
- Không còn column tên `timestamp` với type date cho daily metric.
- Nếu table được tạo mới bởi pipeline, partition là `order_date`.

### 20.3. Kiểm tra quarantine summary

```sql
SELECT rejection_category,
       rejection_reason,
       COUNT(*) AS records
FROM workspace.sales.quarantine_orders
GROUP BY rejection_category, rejection_reason
ORDER BY records DESC;
```

Kỳ vọng:

- Có reason rõ ràng cho từng lỗi validation.
- Business/data quality team đọc được lý do reject.

### 20.4. Kiểm tra stage audit

```sql
SELECT stage_name,
       input_records,
       output_records,
       rejected_records,
       rescued_records,
       status,
       details
FROM workspace.sales.stage_audit
ORDER BY run_timestamp DESC;
```

Kỳ vọng:

- Có audit từng stage.
- Có count input/output/rejected/rescued.
- Các bất thường có thể trace qua log và audit.

### 20.5. Kiểm tra mutable order

```sql
SELECT orderId,
       customerId,
       amount,
       order_date,
       updated_at,
       event_version
FROM workspace.sales.silver_orders
WHERE orderId = 'o_update_1';
```

Kỳ vọng:

- Chỉ có latest state.
- `event_version` cao nhất thắng.
- Event cũ không overwrite event mới.

### 20.6. Kiểm tra Gold daily metric

```sql
SELECT *
FROM workspace.sales.gold_daily_metrics
ORDER BY order_date DESC;
```

Kỳ vọng:

- Mỗi ngày có một dòng aggregate hoặc đúng grain đã thiết kế.
- Re-run không tạo duplicate cùng `order_date`.

### 20.7. Kiểm tra maintenance audit

```sql
SELECT table_name,
       status,
       permission_failure,
       retention_policy_risk,
       vacuum_retention_hours,
       audit_replay_required,
       slow_downstream_max_lag_hours,
       error_class,
       error_message
FROM workspace.sales.maintenance_audit
ORDER BY run_timestamp DESC;
```

Kỳ vọng:

- Maintenance result minh bạch.
- Lỗi permission hoặc command fail không bị nuốt im lặng.
- Retention risk được flag khi policy không an toàn.

## 21. Test SQL materialized view target

Nếu muốn dùng materialized view cho Gold thay cho Java aggregate job:

```powershell
databricks bundle validate -t prod_sql_mv --profile DEFAULT
databricks bundle deploy -t prod_sql_mv --profile DEFAULT
```

Kiểm tra file SQL:

- `resources/sql/gold_daily_metrics_mv.sql`

Kỳ vọng:

- MV aggregate daily từ Silver.
- Phù hợp khi muốn Databricks quản lý incremental refresh.
- Không chạy đồng thời hai cơ chế Gold nếu chưa thống nhất ownership để tránh lệch logic.

## 22. Checklist pass cuối cùng

E2E được xem là pass nếu tất cả điều kiện sau đúng:

### 22.1. Bronze

- Local/test chạy được với `pipeline.ingestion.mode=json`.
- Databricks chạy được với `pipeline.ingestion.mode=cloudFiles`.
- Bronze có `ingest_timestamp`.
- Auto Loader rescue schema drift vào `_rescued_data` trên Databricks.

### 22.2. Silver

- Valid records vào Silver.
- Invalid records vào Quarantine.
- `orderId` được trim và không blank.
- `customerId` được trim và không blank.
- `timestamp` parse rõ ràng trước khi filter.
- Invalid timestamp bị reject.
- `updatedAt` và `eventVersion` hỗ trợ update semantics.
- Event cũ không overwrite event mới.

### 22.3. Quarantine

- Có `rejection_reason`.
- Có `rejection_category`.
- Có `quarantined_at`.
- Reason đủ rõ để debug dữ liệu lỗi.

### 22.4. Gold

- Aggregate theo `order_date`.
- Gold không dùng tên cột `timestamp` cho daily metric date.
- Chỉ recompute affected dates.
- Merge theo `order_date`.
- Không duplicate rows cùng ngày.
- Partition theo `order_date` khi tạo mới.

### 22.5. Observability

- Stage audit có Bronze/Silver/Gold.
- Audit ghi input/output/rejected/rescued counts.
- Log có data quality summary.
- Alert xuất hiện khi count bất thường vượt threshold.

### 22.6. Maintenance

- Maintenance chạy job riêng.
- Best-effort, không che giấu lỗi.
- Maintenance audit ghi status/error/policy fields.
- Retention policy rõ ràng.
- Không chạy `VACUUM` mỗi 5 phút trong main ingestion job.

### 22.7. Security/config

- Core table names không nhận override tùy ý từ CLI external input.
- Trusted table identifier được validate.
- Invalid table name bị reject/audit.

### 22.8. Databricks bundle

- `databricks bundle validate` pass.
- Deploy pass.
- Main job chạy pass.
- Maintenance job chạy pass hoặc fail best-effort có audit rõ ràng.

## 23. Thứ tự chạy khuyến nghị ngắn gọn

Nếu chỉ muốn checklist nhanh:

```powershell
mvn -q -DskipTests compile
mvn -q -Dtest=OrderTransformationServiceTest test
mvn clean package
```

Sau đó chạy lần lượt:

1. Bronze local `json`.
2. Silver strict contract.
3. Bronze update batch.
4. Silver update merge.
5. Gold aggregate.
6. Maintenance.
7. Table-name safety negative test.
8. Bundle validate.
9. Databricks deploy/run.
10. SQL verification.

## 24. Troubleshooting nhanh

| Triệu chứng | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| Bronze local không đọc lại file | Checkpoint đã ghi nhận file cũ | Xóa checkpoint hoặc dùng checkpoint path mới |
| Local Delta test skip trên Windows | Thiếu `HADOOP_HOME` / `winutils` | Chấp nhận skip hoặc cấu hình Hadoop native utilities |
| Silver không có invalid records | Input chưa có bad data hoặc contract chưa strict | Bật `pipeline.production.mode=true`, `require-updated-at=true`, `require-event-version=true` |
| Missing `eventVersion` không bị reject | Contract version chưa bật | Set `pipeline.contract.require-event-version=true` |
| Gold duplicate theo ngày | Merge condition hoặc table cũ sai schema | Drop/recreate test Gold table, kiểm tra merge theo `order_date` |
| Maintenance fail local | `OPTIMIZE`/`VACUUM` không hỗ trợ hoặc thiếu quyền | Kiểm tra maintenance audit, đây là best-effort |
| Bundle validate fail | Sai Databricks profile/target/variable | Kiểm tra `databricks.yml`, `resources/jobs.yml`, profile CLI |
| cloudFiles fail local | Auto Loader chỉ chạy Databricks | Local dùng `pipeline.ingestion.mode=json` |
