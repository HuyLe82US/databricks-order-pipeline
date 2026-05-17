# Databricks Dev E2E Testing Guide

File này hướng dẫn chạy end-to-end test cho `databricks-order-pipeline` trên Databricks target `dev`: chuẩn bị JSON raw data, build/deploy bundle, chạy job, và kiểm tra kết quả Bronze/Silver/Gold/Quarantine/Audit.

Giả định mặc định của repo:

- Catalog/schema: `workspace.sales`
- Raw input: `/Volumes/workspace/sales/raw/`
- Artifact/checkpoint: `/Volumes/workspace/sales/artifacts/`
- JAR path job dùng: `/Volumes/workspace/sales/artifacts/orderpipeline-0.0.1-SNAPSHOT.jar`
- Tables: `workspace.sales.bronze_orders`, `workspace.sales.silver_orders`, `workspace.sales.gold_daily_metrics`, `workspace.sales.quarantine_orders`

## 1. Mục Tiêu E2E

Luồng cần xác nhận:

1. Bronze đọc raw JSON từ Volume bằng Auto Loader `cloudFiles`.
2. Silver validate records, ghi invalid records vào quarantine, và merge latest order state vào Silver.
3. Gold aggregate daily metrics từ Silver theo `order_date`.
4. Stage audit ghi log cho `bronze`, `silver`, `gold`.
5. Maintenance job chạy được và ghi maintenance audit.

Expected chính với dataset mẫu trong guide này:

| Kiểm tra | Kỳ vọng |
|---|---:|
| Bronze rows | `7` |
| Silver rows | `2` |
| Final orders in Silver | `o100`, `o101` |
| Quarantine rows | `3` |
| Gold row for `2026-05-15` | `totalOrders = 2`, `totalRevenue = 230.50` |
| Stage audit | Có `bronze`, `silver`, `gold` |

## 2. Prerequisites

Kiểm tra local tools:

```powershell
java -version
mvn -version
databricks -v
databricks auth profiles
```

Nếu dùng profile khác `DEFAULT`, thay `--profile DEFAULT` trong các command bên dưới bằng profile tương ứng.

Đăng nhập Databricks nếu cần:

```powershell
databricks auth login --host https://dbc-f1a90017-00ba.cloud.databricks.com/ --profile DEFAULT
```

## 3. Chuẩn Bị Databricks Objects

Chạy trong Databricks SQL Editor hoặc Notebook SQL:

```sql
CREATE CATALOG IF NOT EXISTS workspace;
CREATE SCHEMA IF NOT EXISTS workspace.sales;

CREATE VOLUME IF NOT EXISTS workspace.sales.raw;
CREATE VOLUME IF NOT EXISTS workspace.sales.artifacts;
```

Kiểm tra Volume paths:

```sql
LIST '/Volumes/workspace/sales/';
LIST '/Volumes/workspace/sales/raw/';
LIST '/Volumes/workspace/sales/artifacts/';
```

## 4. Clean Environment Trước Khi Test

Dùng khi muốn test lại từ đầu. Bước này xóa tables, raw file mẫu và checkpoint liên quan.

SQL cleanup:

```sql
DROP TABLE IF EXISTS workspace.sales.bronze_orders;
DROP TABLE IF EXISTS workspace.sales.silver_orders;
DROP TABLE IF EXISTS workspace.sales.gold_daily_metrics;
DROP TABLE IF EXISTS workspace.sales.quarantine_orders;
DROP TABLE IF EXISTS workspace.sales.stage_audit;
DROP TABLE IF EXISTS workspace.sales.stage_audit_dev;
DROP TABLE IF EXISTS workspace.sales.maintenance_audit;
DROP TABLE IF EXISTS workspace.sales.maintenance_audit_dev;
```

CLI cleanup:

```powershell
databricks fs rm dbfs:/Volumes/workspace/sales/raw/e2e-orders-batch-001.json --profile DEFAULT
databricks fs rm dbfs:/Volumes/workspace/sales/raw/e2e-orders-batch-002.json --profile DEFAULT
databricks fs rm dbfs:/Volumes/workspace/sales/artifacts/checkpoints/bronze --recursive --profile DEFAULT
```

Nếu file/path chưa tồn tại, lỗi `not found` có thể bỏ qua.

## 5. Tạo Raw JSON Test Data

Tạo file local `e2e-orders-batch-001.json` ở root repo:

```powershell
@'
[
  {
    "orderId": "o100",
    "customerId": "c100",
    "amount": 120.50,
    "timestamp": "2026-05-15T10:00:00Z",
    "updatedAt": "2026-05-15T10:05:00Z",
    "eventVersion": 1
  },
  {
    "orderId": "o101",
    "customerId": "c101",
    "amount": 80.00,
    "timestamp": "2026-05-15T11:00:00Z",
    "updatedAt": "2026-05-15T11:05:00Z",
    "eventVersion": 1
  },
  {
    "orderId": "o100",
    "customerId": "c100",
    "amount": 150.50,
    "timestamp": "2026-05-15T12:00:00Z",
    "updatedAt": "2026-05-15T12:05:00Z",
    "eventVersion": 2
  },
  {
    "orderId": "o101",
    "customerId": "c101",
    "amount": 70.00,
    "timestamp": "2026-05-15T09:00:00Z",
    "updatedAt": "2026-05-15T09:05:00Z",
    "eventVersion": 0
  },
  {
    "orderId": "o102",
    "customerId": "c102",
    "amount": -5.00,
    "timestamp": "2026-05-15T13:00:00Z",
    "updatedAt": "2026-05-15T13:05:00Z",
    "eventVersion": 1
  },
  {
    "orderId": "o103",
    "customerId": "",
    "amount": 40.00,
    "timestamp": "2026-05-15T14:00:00Z",
    "updatedAt": "2026-05-15T14:05:00Z",
    "eventVersion": 1
  },
  {
    "orderId": "o104",
    "customerId": "c104",
    "amount": 30.00,
    "timestamp": "2026-05-15T15:00:00Z",
    "updatedAt": null,
    "eventVersion": 1
  }
]
'@ | Set-Content -Encoding UTF8 e2e-orders-batch-001.json
```

Ý nghĩa dataset:

| Record | Kỳ vọng |
|---|---|
| `o100` version 1 | Valid, nhưng bị version 2 thay thế trong Silver |
| `o101` version 1 | Valid, giữ lại trong Silver |
| `o100` version 2 | Valid, là final state của `o100` trong Silver |
| `o101` version 0 | Valid, nhưng cũ hơn version 1 nên không overwrite Silver |
| `o102` | Invalid vì `amount <= 0`, vào quarantine với `NON_POSITIVE_AMOUNT` |
| `o103` | Invalid vì `customerId` rỗng, vào quarantine với `MISSING_CUSTOMER_ID` |
| `o104` | Invalid nếu bật contract `require-updated-at=true`, vào quarantine với `MISSING_UPDATED_AT` |

Upload raw file vào Databricks Volume:

```powershell
databricks fs cp e2e-orders-batch-001.json dbfs:/Volumes/workspace/sales/raw/e2e-orders-batch-001.json --overwrite --profile DEFAULT
databricks fs ls dbfs:/Volumes/workspace/sales/raw/ --profile DEFAULT
```

## 6. Build Và Upload JAR

Build local:

```powershell
mvn clean package
```

Kỳ vọng:

- Command kết thúc `BUILD SUCCESS`.
- File tồn tại: `target/orderpipeline-0.0.1-SNAPSHOT.jar`.

Kiểm tra nhanh:

```powershell
Test-Path target/orderpipeline-0.0.1-SNAPSHOT.jar
```

Upload JAR tới path mà job bundle đang dùng:

```powershell
databricks fs cp target/orderpipeline-0.0.1-SNAPSHOT.jar dbfs:/Volumes/workspace/sales/artifacts/orderpipeline-0.0.1-SNAPSHOT.jar --overwrite --profile DEFAULT
databricks fs ls dbfs:/Volumes/workspace/sales/artifacts/ --profile DEFAULT
```

## 7. Validate Và Deploy Databricks Asset Bundle

Target `dev` trong `databricks.yml` mặc định set `contract_require_updated_at=false` và `contract_require_event_version=false`. Để dataset `o104` bị quarantine vì thiếu `updatedAt`, override contract sang `true` trước khi validate/deploy/run:

```powershell
$env:DATABRICKS_BUNDLE_VAR_contract_require_updated_at="true"
$env:DATABRICKS_BUNDLE_VAR_contract_require_event_version="true"
```

Validate:

```powershell
databricks bundle validate -t dev --profile DEFAULT
```

Deploy:

```powershell
databricks bundle deploy -t dev --profile DEFAULT
```

Kỳ vọng:

- `validate` không báo lỗi cấu hình.
- `deploy` tạo/cập nhật jobs `Order Pipeline Batch Job` và `Order Pipeline Maintenance Job`.

## 8. Chạy E2E Job Chính

Chạy Bronze -> Silver -> Gold:

```powershell
databricks bundle run order_pipeline_job -t dev --profile DEFAULT
```

Kỳ vọng trên Databricks job run:

| Task | Kỳ vọng |
|---|---|
| `ingest_bronze` | Success, đọc JSON từ `/Volumes/workspace/sales/raw/` |
| `process_silver` | Success, ghi valid rows vào Silver và invalid rows vào quarantine |
| `aggregate_gold` | Success, tạo/cập nhật daily metrics |

Nếu job fail ở `process_silver` vì rescued/corrupt threshold, kiểm tra raw JSON có bị malformed không. Dataset mẫu ở trên là valid JSON, nên không kỳ vọng fail.

## 9. Kiểm Tra Kết Quả Bằng SQL

Chạy trong Databricks SQL Editor hoặc Notebook SQL.

### 9.1. Count Tổng Quan

```sql
SELECT 'bronze' AS table_name, COUNT(*) AS row_count FROM workspace.sales.bronze_orders
UNION ALL
SELECT 'silver', COUNT(*) FROM workspace.sales.silver_orders
UNION ALL
SELECT 'gold', COUNT(*) FROM workspace.sales.gold_daily_metrics
UNION ALL
SELECT 'quarantine', COUNT(*) FROM workspace.sales.quarantine_orders;
```

Kỳ vọng:

| table_name | row_count |
|---|---:|
| bronze | `7` |
| silver | `2` |
| gold | `1` |
| quarantine | `3` |

Nếu chạy lại cùng raw file nhưng không xóa checkpoint/table trước đó, count có thể khác kỳ vọng. Hãy quay lại bước clean environment.

### 9.2. Bronze Raw Data

```sql
SELECT
  orderId,
  customerId,
  amount,
  timestamp,
  updatedAt,
  eventVersion,
  ingest_timestamp
FROM workspace.sales.bronze_orders
ORDER BY orderId, eventVersion;
```

Kỳ vọng:

- Có đủ 7 input records.
- Có cột `ingest_timestamp` được app thêm ở Bronze.
- Raw fields vẫn ở dạng input ban đầu, ví dụ `updatedAt` camelCase.

### 9.3. Silver Final State

```sql
SELECT
  orderId,
  customerId,
  amount,
  timestamp,
  updated_at,
  event_version,
  order_date
FROM workspace.sales.silver_orders
ORDER BY orderId;
```

Kỳ vọng chính xác:

| orderId | customerId | amount | event_version | order_date |
|---|---|---:|---:|---|
| `o100` | `c100` | `150.50` | `2` | `2026-05-15` |
| `o101` | `c101` | `80.00` | `1` | `2026-05-15` |

Các điểm cần kiểm tra:

- Không có `o102`, `o103`, `o104` trong Silver.
- `o100` giữ version mới nhất `2`, không giữ amount `120.50`.
- `o101` không bị record cũ version `0` overwrite.
- Column output dùng snake_case: `updated_at`, `event_version`, `order_date`.

### 9.4. Quarantine Records

```sql
SELECT
  orderId,
  customerId,
  amount,
  timestamp,
  updatedAt,
  eventVersion,
  rejection_reason,
  rejection_category,
  quarantined_at
FROM workspace.sales.quarantine_orders
ORDER BY orderId, rejection_reason;
```

Kỳ vọng:

| orderId | rejection_reason | rejection_category |
|---|---|---|
| `o102` | `NON_POSITIVE_AMOUNT` | `BUSINESS_RULE` |
| `o103` | `MISSING_CUSTOMER_ID` | `SCHEMA` |
| `o104` | `MISSING_UPDATED_AT` | `SCHEMA` |

Nếu không thấy `o104` trong quarantine, kiểm tra lại biến bundle:

```powershell
$env:DATABRICKS_BUNDLE_VAR_contract_require_updated_at
```

Giá trị cần là `true`, sau đó chạy lại `bundle deploy` và clean/re-run E2E.

### 9.5. Gold Daily Metrics

```sql
SELECT
  order_date,
  totalOrders,
  totalRevenue,
  source_max_ingest_timestamp,
  gold_updated_at
FROM workspace.sales.gold_daily_metrics
ORDER BY order_date;
```

Kỳ vọng:

| order_date | totalOrders | totalRevenue |
|---|---:|---:|
| `2026-05-15` | `2` | `230.50` |

Giải thích: Gold aggregate từ final Silver state, không aggregate toàn bộ Bronze. Vì vậy doanh thu là `o100 version 2 = 150.50` + `o101 version 1 = 80.00`.

### 9.6. Stage Audit

Target `dev` mặc định ghi stage audit vào `workspace.sales.stage_audit_dev` theo `databricks.yml`.

```sql
SELECT
  stage_name,
  status,
  input_records,
  output_records,
  rejected_records,
  rescued_records,
  details,
  audit_timestamp
FROM workspace.sales.stage_audit_dev
ORDER BY audit_timestamp DESC;
```

Kỳ vọng:

- Có ít nhất 3 rows cho job run mới nhất: `bronze`, `silver`, `gold`.
- `status = 'SUCCESS'`.
- Silver có `rejected_records = 3`.
- Silver `details` có dạng `cleanRecords=4,quarantineRows=3`.
- Gold `details` có dạng `affectedDates=1`.

Nếu override `DATABRICKS_BUNDLE_VAR_stage_audit_table`, query table override đó thay vì `stage_audit_dev`.

## 10. Chạy Và Kiểm Tra Maintenance Job

Chạy maintenance:

```powershell
databricks bundle run order_pipeline_maintenance_job -t dev --profile DEFAULT
```

Target `dev` mặc định dùng `workspace.sales.maintenance_audit_dev`.

```sql
SELECT
  table_name,
  status,
  operation,
  retention_hours,
  details,
  audit_timestamp
FROM workspace.sales.maintenance_audit_dev
ORDER BY audit_timestamp DESC;
```

Kỳ vọng:

- Có rows cho các table chính: Bronze, Silver, Gold.
- `status` thường là `SUCCESS` nếu cluster/runtime hỗ trợ `OPTIMIZE` và `VACUUM` trên table đó.
- Có thể có `SKIPPED` nếu table không tồn tại hoặc chính sách retention không cho phép vacuum; đọc cột `details` để biết lý do.

## 11. Test Incremental Update Tùy Chọn

Mục tiêu: xác nhận Silver merge và Gold refresh theo affected date.

Tạo batch 2 với update mới cho `o101` và order mới `o105`:

```powershell
@'
[
  {
    "orderId": "o101",
    "customerId": "c101",
    "amount": 95.00,
    "timestamp": "2026-05-15T16:00:00Z",
    "updatedAt": "2026-05-15T16:05:00Z",
    "eventVersion": 2
  },
  {
    "orderId": "o105",
    "customerId": "c105",
    "amount": 25.00,
    "timestamp": "2026-05-16T09:00:00Z",
    "updatedAt": "2026-05-16T09:05:00Z",
    "eventVersion": 1
  }
]
'@ | Set-Content -Encoding UTF8 e2e-orders-batch-002.json

databricks fs cp e2e-orders-batch-002.json dbfs:/Volumes/workspace/sales/raw/e2e-orders-batch-002.json --overwrite --profile DEFAULT
databricks bundle run order_pipeline_job -t dev --profile DEFAULT
```

Kiểm tra Silver:

```sql
SELECT orderId, amount, event_version, order_date
FROM workspace.sales.silver_orders
WHERE orderId IN ('o100', 'o101', 'o105')
ORDER BY orderId;
```

Kỳ vọng:

| orderId | amount | event_version | order_date |
|---|---:|---:|---|
| `o100` | `150.50` | `2` | `2026-05-15` |
| `o101` | `95.00` | `2` | `2026-05-15` |
| `o105` | `25.00` | `1` | `2026-05-16` |

Kiểm tra Gold:

```sql
SELECT order_date, totalOrders, totalRevenue
FROM workspace.sales.gold_daily_metrics
ORDER BY order_date;
```

Kỳ vọng sau batch 2:

| order_date | totalOrders | totalRevenue |
|---|---:|---:|
| `2026-05-15` | `2` | `245.50` |
| `2026-05-16` | `1` | `25.00` |

## 12. Troubleshooting Nhanh

| Lỗi | Nguyên nhân thường gặp | Cách xử lý |
|---|---|---|
| `Table or view not found` | Chưa chạy stage trước đó hoặc đã drop table | Chạy lại từ bước clean + upload + bundle run |
| `Path does not exist` | Chưa tạo Volume hoặc upload raw/JAR sai path | Kiểm tra `databricks fs ls dbfs:/Volumes/workspace/sales/...` |
| Bronze count `0` | Auto Loader checkpoint đã ghi nhận file cũ | Xóa checkpoint `/Volumes/workspace/sales/artifacts/checkpoints/bronze` và chạy lại |
| `o104` không vào quarantine | Dev target mặc định contract có thể là `false` | Set `DATABRICKS_BUNDLE_VAR_contract_require_updated_at=true`, deploy lại |
| Job không thấy JAR | Chưa upload JAR đúng path | Upload lại vào `/Volumes/workspace/sales/artifacts/orderpipeline-0.0.1-SNAPSHOT.jar` |
| Bundle validate fail | Sai Databricks auth/profile/host | Chạy `databricks auth profiles` và validate với đúng `--profile` |

## 13. Checklist Pass/Fail

E2E dev được xem là pass khi:

- `mvn clean package` thành công.
- `databricks bundle validate -t dev` thành công.
- `databricks bundle deploy -t dev` thành công.
- `order_pipeline_job` chạy success cả 3 tasks.
- Bronze có 7 rows sau batch 1.
- Silver có đúng 2 final rows: `o100`, `o101`.
- Quarantine có đúng 3 rejection reasons: `NON_POSITIVE_AMOUNT`, `MISSING_CUSTOMER_ID`, `MISSING_UPDATED_AT`.
- Gold ngày `2026-05-15` có `totalOrders = 2`, `totalRevenue = 230.50`.
- `stage_audit_dev` có audit rows cho `bronze`, `silver`, `gold`.
- Maintenance job chạy được và có rows trong `maintenance_audit_dev` hoặc có lý do skipped rõ ràng.
