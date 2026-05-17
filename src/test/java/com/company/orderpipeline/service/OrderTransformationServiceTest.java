package com.company.orderpipeline.service;

import com.company.orderpipeline.service.OrderTransformationService.SilverProcessingOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

public class OrderTransformationServiceTest {

    private static SparkSession spark;
    private static OrderTransformationService service;

    @BeforeAll
    public static void setUp() {
        spark = SparkSession.builder()
                .appName("OrderTransformationServiceTest")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.permissions.enabled", "false")
                // We use an in-memory derby database for tests instead of hive to avoid
                // winutils requirement
                .config("spark.sql.warehouse.dir", "target/spark-warehouse")
                .getOrCreate();

        service = new OrderTransformationService(spark);
    }

    @AfterAll
    public static void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testCleanOrders() {
        StructType schema = new StructType(new StructField[] {
                new StructField("orderId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("customerId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.StringType, true, Metadata.empty()),
                new StructField("updatedAt", DataTypes.StringType, true, Metadata.empty()),
                new StructField("eventVersion", DataTypes.LongType, true, Metadata.empty())
        });

        List<Row> data = Arrays.asList(
                RowFactory.create(" 1 ", " c1 ", 100.0, " 2023-10-27T10:00:00Z ", " 2023-10-27T10:30:00Z ", 1L),
                RowFactory.create("2", "c2", -50.0, "2023-10-27T11:00:00Z", null, null),
                RowFactory.create(null, "c3", 200.0, "2023-10-27T12:00:00Z", null, null),
                RowFactory.create("3", null, 180.0, "2023-10-27T13:00:00Z", null, null),
                RowFactory.create("5", "c5", 125.0, "not-a-date", null, null),
                RowFactory.create("4", "c4", 150.0, "2023-10-28T09:00:00Z", null, 2L));

        Dataset<Row> rawOrders = spark.createDataFrame(data, schema);
        Dataset<Row> cleanedOrders = service.cleanOrders(rawOrders);

        assertEquals(2, cleanedOrders.count());
        Row cleanedOrder = cleanedOrders.filter("orderId = '1'").first();
        assertEquals("c1", cleanedOrder.getAs("customerId"));
        assertEquals(java.sql.Date.valueOf("2023-10-27"), cleanedOrder.getAs("order_date"));
        assertNotNull(cleanedOrder.getAs("updated_at"));
        assertEquals(1L, ((Number) cleanedOrder.getAs("event_version")).longValue());
    }

    @Test
    public void testAggregateDailyMetrics() {
        StructType schema = new StructType(new StructField[] {
                new StructField("orderId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("order_date", DataTypes.DateType, true, Metadata.empty())
        });

        List<Row> data = Arrays.asList(
                RowFactory.create("1", 100.0, java.sql.Date.valueOf("2023-10-27")),
                RowFactory.create("2", 150.0, java.sql.Date.valueOf("2023-10-27")),
                RowFactory.create("3", 200.0, java.sql.Date.valueOf("2023-10-28")));

        Dataset<Row> cleanedOrders = spark.createDataFrame(data, schema);
        Dataset<Row> aggregatedMetrics = service.aggregateDailyMetrics(cleanedOrders);

        assertEquals(2, aggregatedMetrics.count());

        Row day1 = aggregatedMetrics
                .filter(aggregatedMetrics.col("order_date").equalTo(java.sql.Date.valueOf("2023-10-27"))).first();
        assertEquals(2L, (Long) day1.getAs("totalOrders"));
        assertEquals(250.0, (Double) day1.getAs("totalRevenue"));
    }

    @Test
    public void testResolveIngestionModeDefaultsToCloudFiles() {
        assertEquals("cloudfiles", service.resolveIngestionMode(null));
    }

    @Test
    public void testResolveIngestionModeSupportsJsonCaseInsensitive() {
        assertEquals("json", service.resolveIngestionMode(" JSON "));
    }

    @Test
    public void testResolveIngestionModeFallsBackForUnsupportedValue() {
        assertEquals("cloudfiles", service.resolveIngestionMode("xml"));
    }

    @Test
    public void testProcessSilverRejectsUntrustedTableName() {
        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> service.processSilver(
                        "default.bronze_orders",
                        "default.silver_orders; DROP TABLE default.x",
                        "default.quarantine_orders",
                        false,
                        0));

        assertTrue(exception.getMessage().contains("trusted table identifier"));
    }

    @Test
    public void testProcessSilverWritesCleanRecordsAndQuarantinesBadRecords() {
        assumeFalse(isWindowsWithoutHadoopHome(), "Delta local saveAsTable requires winutils/HADOOP_HOME on Windows.");

        String suffix = uniqueTableSuffix();
        String bronzeTable = "default.bronze_orders_" + suffix;
        String silverTable = "default.silver_orders_" + suffix;
        String quarantineTable = "default.quarantine_orders_" + suffix;

        StructType schema = new StructType(new StructField[] {
                new StructField("orderId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("customerId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.StringType, true, Metadata.empty()),
                new StructField("updatedAt", DataTypes.StringType, true, Metadata.empty()),
                new StructField("eventVersion", DataTypes.LongType, true, Metadata.empty()),
                new StructField("_rescued_data", DataTypes.StringType, true, Metadata.empty())
        });

        List<Row> data = Arrays.asList(
                RowFactory.create("1", "c1", 100.0, "2023-10-27T10:00:00Z", "2023-10-27T10:05:00Z", 1L, null),
                RowFactory.create(" ", "c2", -10.0, "not-a-date", null, null, null),
                RowFactory.create("3", "c3", 300.0, "2023-10-27T12:00:00Z", "2023-10-27T12:01:00Z", 1L, "{\"extra\":\"value\"}"));

        spark.createDataFrame(data, schema).write().format("delta").saveAsTable(bronzeTable);

        service.processSilver(bronzeTable, silverTable, quarantineTable, false, 0);

        assertEquals(1, spark.read().table(silverTable).count());
        assertEquals(4, spark.read().table(quarantineTable).count());

        assertEquals(1, spark.read().table(quarantineTable)
                .filter("customerId = 'c2' AND rejection_reason = 'MISSING_ORDER_ID'")
                .count());
        assertEquals(1, spark.read().table(quarantineTable)
                .filter("customerId = 'c2' AND rejection_reason = 'MISSING_ORDER_ID' AND rejection_category = 'SCHEMA'")
                .count());
        assertEquals(1, spark.read().table(quarantineTable)
                .filter("customerId = 'c2' AND rejection_reason = 'NON_POSITIVE_AMOUNT'")
                .count());
        assertEquals(1, spark.read().table(quarantineTable)
                .filter("customerId = 'c2' AND rejection_reason = 'NON_POSITIVE_AMOUNT' AND rejection_category = 'BUSINESS_RULE'")
                .count());
        assertEquals(1, spark.read().table(quarantineTable)
                .filter("customerId = 'c2' AND rejection_reason = 'INVALID_TIMESTAMP'")
                .count());
        assertEquals(1, spark.read().table(quarantineTable)
                .filter("customerId = 'c2' AND rejection_reason = 'INVALID_TIMESTAMP' AND rejection_category = 'SCHEMA'")
                .count());

        Row rescuedRecord = spark.read().table(quarantineTable)
                .filter("orderId = '3' AND rejection_reason = 'RESCUED_DATA_PRESENT'")
                .first();
        assertEquals("RESCUED_DATA_PRESENT", rescuedRecord.getAs("rejection_reason"));
        assertEquals("INGESTION", rescuedRecord.getAs("rejection_category"));
    }

    @Test
    public void testProcessSilverFailsInProductionWhenRescuedRecordsExceedThreshold() {
        assumeFalse(isWindowsWithoutHadoopHome(), "Delta local saveAsTable requires winutils/HADOOP_HOME on Windows.");

        String suffix = uniqueTableSuffix();
        String bronzeTable = "default.bronze_orders_" + suffix;
        String silverTable = "default.silver_orders_" + suffix;
        String quarantineTable = "default.quarantine_orders_" + suffix;

        StructType schema = new StructType(new StructField[] {
                new StructField("orderId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("customerId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.StringType, true, Metadata.empty()),
                new StructField("updatedAt", DataTypes.StringType, true, Metadata.empty()),
                new StructField("eventVersion", DataTypes.LongType, true, Metadata.empty()),
                new StructField("_rescued_data", DataTypes.StringType, true, Metadata.empty())
        });

        List<Row> data = List.of(
                RowFactory.create("1", "c1", 100.0, "2023-10-27T10:00:00Z", "2023-10-27T10:05:00Z", 1L, "{\"extra\":\"value\"}"));

        spark.createDataFrame(data, schema).write().format("delta").saveAsTable(bronzeTable);

        assertThrows(
                IllegalStateException.class,
                () -> service.processSilver(bronzeTable, silverTable, quarantineTable, true, 0));
    }

    @Test
    public void testProcessSilverRequiresUpdatedAtInProductionMode() {
        assumeFalse(isWindowsWithoutHadoopHome(), "Delta local saveAsTable requires winutils/HADOOP_HOME on Windows.");

        String suffix = uniqueTableSuffix();
        String bronzeTable = "default.bronze_orders_missing_updated_at_" + suffix;
        String silverTable = "default.silver_orders_missing_updated_at_" + suffix;
        String quarantineTable = "default.quarantine_orders_missing_updated_at_" + suffix;

        StructType schema = new StructType(new StructField[] {
                new StructField("orderId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("customerId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.StringType, true, Metadata.empty()),
                new StructField("updatedAt", DataTypes.StringType, true, Metadata.empty()),
                new StructField("eventVersion", DataTypes.LongType, true, Metadata.empty())
        });

        List<Row> data = Arrays.asList(
                RowFactory.create("1", "c1", 100.0, "2023-10-27T10:00:00Z", null, 1L),
                RowFactory.create("2", "c2", 150.0, "2023-10-27T11:00:00Z", "2023-10-27T11:05:00Z", 1L));

        spark.createDataFrame(data, schema).write().format("delta").saveAsTable(bronzeTable);

        service.processSilver(bronzeTable, silverTable, quarantineTable, true, 100);

        assertEquals(1, spark.read().table(silverTable).count());
        assertEquals(1, spark.read().table(quarantineTable)
                .filter("orderId = '1' AND rejection_reason = 'MISSING_UPDATED_AT' AND rejection_category = 'SCHEMA'")
                .count());
    }

    @Test
    public void testProcessSilverRequiresEventVersionInProductionMode() {
        assumeFalse(isWindowsWithoutHadoopHome(), "Delta local saveAsTable requires winutils/HADOOP_HOME on Windows.");

        String suffix = uniqueTableSuffix();
        String bronzeTable = "default.bronze_orders_missing_event_version_" + suffix;
        String silverTable = "default.silver_orders_missing_event_version_" + suffix;
        String quarantineTable = "default.quarantine_orders_missing_event_version_" + suffix;

        StructType schema = new StructType(new StructField[] {
                new StructField("orderId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("customerId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.StringType, true, Metadata.empty()),
                new StructField("updatedAt", DataTypes.StringType, true, Metadata.empty()),
                new StructField("eventVersion", DataTypes.LongType, true, Metadata.empty())
        });

        List<Row> data = Arrays.asList(
                RowFactory.create("1", "c1", 100.0, "2023-10-27T10:00:00Z", "2023-10-27T10:05:00Z", null),
                RowFactory.create("2", "c2", 150.0, "2023-10-27T11:00:00Z", "2023-10-27T11:05:00Z", 1L));

        spark.createDataFrame(data, schema).write().format("delta").saveAsTable(bronzeTable);

        service.processSilver(bronzeTable, silverTable, quarantineTable, true, 100);

        assertEquals(1, spark.read().table(silverTable).count());
        assertEquals(1, spark.read().table(quarantineTable)
                .filter("orderId = '1' AND rejection_reason = 'MISSING_EVENT_VERSION' AND rejection_category = 'SCHEMA'")
                .count());
    }

    @Test
    public void testProcessSilverAllowsDisablingEventVersionRequirement() {
        assumeFalse(isWindowsWithoutHadoopHome(), "Delta local saveAsTable requires winutils/HADOOP_HOME on Windows.");

        String suffix = uniqueTableSuffix();
        String bronzeTable = "default.bronze_orders_optional_event_version_" + suffix;
        String silverTable = "default.silver_orders_optional_event_version_" + suffix;
        String quarantineTable = "default.quarantine_orders_optional_event_version_" + suffix;

        StructType schema = new StructType(new StructField[] {
                new StructField("orderId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("customerId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.StringType, true, Metadata.empty()),
                new StructField("updatedAt", DataTypes.StringType, true, Metadata.empty()),
                new StructField("eventVersion", DataTypes.LongType, true, Metadata.empty())
        });

        List<Row> data = List.of(
                RowFactory.create("1", "c1", 100.0, "2023-10-27T10:00:00Z", "2023-10-27T10:05:00Z", null));

        spark.createDataFrame(data, schema).write().format("delta").saveAsTable(bronzeTable);

        service.processSilver(
                bronzeTable,
                silverTable,
                quarantineTable,
                SilverProcessingOptions.defaults(true, 100)
                        .requireUpdatedAt(true)
                        .requireEventVersion(false));

        assertEquals(1, spark.read().table(silverTable).count());
        assertFalse(spark.catalog().tableExists(quarantineTable));
    }

    @Test
    public void testAggregateGoldIncrementallyRefreshesAffectedDates() {
        assumeFalse(isWindowsWithoutHadoopHome(), "Delta local saveAsTable requires winutils/HADOOP_HOME on Windows.");

        String suffix = uniqueTableSuffix();
        String silverTable = "default.silver_orders_" + suffix;
        String goldTable = "default.gold_daily_metrics_" + suffix;

        StructType schema = new StructType(new StructField[] {
                new StructField("orderId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("customerId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("order_date", DataTypes.DateType, true, Metadata.empty()),
                new StructField("ingest_timestamp", DataTypes.TimestampType, true, Metadata.empty())
        });

        List<Row> initialData = Arrays.asList(
                RowFactory.create("1", "c1", 100.0, java.sql.Date.valueOf("2023-10-27"), java.sql.Timestamp.valueOf("2023-10-27 10:00:00")),
                RowFactory.create("2", "c2", 150.0, java.sql.Date.valueOf("2023-10-28"), java.sql.Timestamp.valueOf("2023-10-28 10:00:00")));

        spark.createDataFrame(initialData, schema).write().format("delta").saveAsTable(silverTable);

        service.aggregateGold(silverTable, goldTable);

        assertEquals(2, spark.read().table(goldTable).count());
        Row day2Before = spark.read().table(goldTable)
                .filter("order_date = DATE '2023-10-28'")
                .first();
        assertEquals(150.0, (Double) day2Before.getAs("totalRevenue"));
        assertNotNull(day2Before.getAs("source_max_ingest_timestamp"));
        assertNotNull(day2Before.getAs("gold_updated_at"));

        List<Row> incrementalData = Arrays.asList(
                RowFactory.create("3", "c3", 200.0, java.sql.Date.valueOf("2023-10-28"), java.sql.Timestamp.valueOf("2023-10-29 09:00:00")),
                RowFactory.create("4", "c4", 300.0, java.sql.Date.valueOf("2023-10-29"), java.sql.Timestamp.valueOf("2023-10-29 11:00:00")));

        spark.createDataFrame(incrementalData, schema).write().format("delta").mode("append").saveAsTable(silverTable);

        service.aggregateGold(silverTable, goldTable);

        assertEquals(3, spark.read().table(goldTable).count());

        Row day1After = spark.read().table(goldTable)
                .filter("order_date = DATE '2023-10-27'")
                .first();
        assertEquals(100.0, (Double) day1After.getAs("totalRevenue"));

        Row day2After = spark.read().table(goldTable)
                .filter("order_date = DATE '2023-10-28'")
                .first();
        assertEquals(350.0, (Double) day2After.getAs("totalRevenue"));
        assertEquals(2L, (Long) day2After.getAs("totalOrders"));

        Row day3After = spark.read().table(goldTable)
                .filter("order_date = DATE '2023-10-29'")
                .first();
        assertEquals(300.0, (Double) day3After.getAs("totalRevenue"));
    }

    @Test
    public void testProcessSilverKeepsLatestOrderUpdateByVersionAndUpdatedAt() {
        assumeFalse(isWindowsWithoutHadoopHome(), "Delta local saveAsTable requires winutils/HADOOP_HOME on Windows.");

        String suffix = uniqueTableSuffix();
        String bronzeTable = "default.bronze_orders_updates_" + suffix;
        String silverTable = "default.silver_orders_updates_" + suffix;
        String quarantineTable = "default.quarantine_orders_updates_" + suffix;

        StructType schema = new StructType(new StructField[] {
                new StructField("orderId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("customerId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("amount", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("timestamp", DataTypes.StringType, true, Metadata.empty()),
                new StructField("updatedAt", DataTypes.StringType, true, Metadata.empty()),
                new StructField("eventVersion", DataTypes.LongType, true, Metadata.empty())
        });

        List<Row> initialData = List.of(
                RowFactory.create("o1", "c1", 100.0, "2026-05-12T10:00:00Z", "2026-05-12T10:05:00Z", 2L));
        spark.createDataFrame(initialData, schema).write().format("delta").saveAsTable(bronzeTable);
        service.processSilver(bronzeTable, silverTable, quarantineTable, false, 100);

        List<Row> updates = Arrays.asList(
                RowFactory.create("o1", "c1", 999.0, "2026-05-12T10:00:00Z", "2026-05-12T10:01:00Z", 1L),
                RowFactory.create("o1", "c1", 150.0, "2026-05-12T10:00:00Z", "2026-05-12T10:10:00Z", 3L));
        spark.createDataFrame(updates, schema).write().format("delta").mode("overwrite").saveAsTable(bronzeTable);

        service.processSilver(bronzeTable, silverTable, quarantineTable, false, 100);

        Row silverRow = spark.read().table(silverTable).filter("orderId = 'o1'").first();
        assertEquals(1, spark.read().table(silverTable).count());
        assertEquals(150.0, (Double) silverRow.getAs("amount"));
        assertEquals(3L, ((Number) silverRow.getAs("event_version")).longValue());
        assertNotNull(silverRow.getAs("updated_at"));
    }

    @Test
    public void testPerformMaintenanceWritesAuditRowWithPolicyFields() {
        assumeFalse(isWindowsWithoutHadoopHome(), "Delta local saveAsTable requires winutils/HADOOP_HOME on Windows.");

        String suffix = uniqueTableSuffix();
        String auditTable = "default.maintenance_audit_" + suffix;
        String missingTable = "default.missing_maintenance_" + suffix;

        service.performMaintenance(auditTable, true, 168, true, 24, missingTable);

        Dataset<Row> auditRows = spark.read().table(auditTable);
        assertEquals(1, auditRows.count());

        Row auditRow = auditRows.first();
        assertNotNull(auditRow.getAs("maintenance_run_id"));
        assertNotNull(auditRow.getAs("run_timestamp"));
        assertEquals(missingTable, auditRow.getAs("table_name"));
        assertEquals("SKIPPED", auditRow.getAs("status"));
        assertFalse((Boolean) auditRow.getAs("permission_failure"));
        assertFalse((Boolean) auditRow.getAs("retention_policy_risk"));
        assertEquals(168, (int) auditRow.getAs("vacuum_retention_hours"));
        assertTrue((Boolean) auditRow.getAs("audit_replay_required"));
        assertEquals(24, (int) auditRow.getAs("slow_downstream_max_lag_hours"));
        assertEquals("Table does not exist", auditRow.getAs("error_message"));
    }

    @Test
    public void testPerformMaintenanceFlagsRetentionPolicyRiskInAudit() {
        assumeFalse(isWindowsWithoutHadoopHome(), "Delta local saveAsTable requires winutils/HADOOP_HOME on Windows.");

        String suffix = uniqueTableSuffix();
        String auditTable = "default.maintenance_audit_risk_" + suffix;
        String missingTable = "default.missing_maintenance_risk_" + suffix;

        service.performMaintenance(auditTable, true, 12, true, 24, missingTable);

        Row auditRow = spark.read().table(auditTable).first();
        assertEquals("SKIPPED", auditRow.getAs("status"));
        assertTrue((Boolean) auditRow.getAs("retention_policy_risk"));
        assertEquals(12, (int) auditRow.getAs("vacuum_retention_hours"));
    }

    @Test
    public void testPerformMaintenanceAuditsInvalidTrustedTableName() {
        assumeFalse(isWindowsWithoutHadoopHome(), "Delta local saveAsTable requires winutils/HADOOP_HOME on Windows.");

        String suffix = uniqueTableSuffix();
        String auditTable = "default.maintenance_audit_invalid_name_" + suffix;

        service.performMaintenance(auditTable, true, 168, true, 24, "default.orders;DROP_TABLE");

        Row auditRow = spark.read().table(auditTable).first();
        assertEquals("FAILED", auditRow.getAs("status"));
        assertEquals("default.orders;DROP_TABLE", auditRow.getAs("table_name"));
        assertEquals("IllegalArgumentException", auditRow.getAs("error_class"));
        assertTrue(((String) auditRow.getAs("error_message")).contains("trusted table identifier"));
    }

    private String uniqueTableSuffix() {
        return UUID.randomUUID().toString().replace("-", "_");
    }

    private boolean isWindowsWithoutHadoopHome() {
        String osName = System.getProperty("os.name", "").toLowerCase();
        return osName.contains("win")
                && System.getenv("HADOOP_HOME") == null
                && System.getProperty("hadoop.home.dir") == null;
    }
}
