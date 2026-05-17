package com.company.orderpipeline.service;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.streaming.DataStreamReader;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.array_compact;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.current_timestamp;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.size;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.to_date;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.when;

@Service
public class OrderTransformationService {

    private static final Logger log = LoggerFactory.getLogger(OrderTransformationService.class);

    private static final String INGESTION_MODE_CLOUDFILES = "cloudfiles";
    private static final String INGESTION_MODE_JSON = "json";

    private static final String RESCUED_DATA_COLUMN = "_rescued_data";
    private static final String CORRUPT_RECORD_COLUMN = "_corrupt_record";
    private static final String VALIDATION_ERRORS_COLUMN = "validation_errors";
    private static final String REJECTION_REASON_COLUMN = "rejection_reason";
    private static final String REJECTION_CATEGORY_COLUMN = "rejection_category";
    private static final String PARSED_TIMESTAMP_COLUMN = "parsed_timestamp";
    private static final String PARSED_UPDATED_AT_COLUMN = "parsed_updated_at";
    private static final String ORDER_DATE_COLUMN = "order_date";
    private static final String UPDATED_AT_COLUMN = "updated_at";
    private static final String EVENT_VERSION_COLUMN = "event_version";

    private static final String MAINTENANCE_STATUS_SUCCESS = "SUCCESS";
    private static final String MAINTENANCE_STATUS_SKIPPED = "SKIPPED";
    private static final String MAINTENANCE_STATUS_FAILED = "FAILED";

    private static final String STAGE_STATUS_SUCCESS = "SUCCESS";
    private static final String STAGE_BRONZE = "bronze";
    private static final String STAGE_SILVER = "silver";
    private static final String STAGE_GOLD = "gold";

    private static final Pattern TRUSTED_TABLE_IDENTIFIER_PATTERN = Pattern.compile(
            "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*){0,2}");

    public static final class SilverProcessingOptions {
        private final boolean productionMode;
        private final long rescuedRecordAlertThreshold;
        private boolean requireUpdatedAt;
        private boolean requireEventVersion;
        private String stageAuditTable = "";
        private boolean alertOnAnomaly = true;
        private double minOutputRatio = 0.1;
        private long maxRejectedCount = 1000;

        private SilverProcessingOptions(boolean productionMode, long rescuedRecordAlertThreshold) {
            this.productionMode = productionMode;
            this.rescuedRecordAlertThreshold = rescuedRecordAlertThreshold;
            this.requireUpdatedAt = productionMode;
            this.requireEventVersion = productionMode;
        }

        public static SilverProcessingOptions defaults(boolean productionMode, long rescuedRecordAlertThreshold) {
            return new SilverProcessingOptions(productionMode, rescuedRecordAlertThreshold);
        }

        public SilverProcessingOptions requireUpdatedAt(boolean requireUpdatedAt) {
            this.requireUpdatedAt = requireUpdatedAt;
            return this;
        }

        public SilverProcessingOptions requireEventVersion(boolean requireEventVersion) {
            this.requireEventVersion = requireEventVersion;
            return this;
        }

        public SilverProcessingOptions stageAuditTable(String stageAuditTable) {
            this.stageAuditTable = stageAuditTable;
            return this;
        }

        public SilverProcessingOptions alertOnAnomaly(boolean alertOnAnomaly) {
            this.alertOnAnomaly = alertOnAnomaly;
            return this;
        }

        public SilverProcessingOptions minOutputRatio(double minOutputRatio) {
            this.minOutputRatio = minOutputRatio;
            return this;
        }

        public SilverProcessingOptions maxRejectedCount(long maxRejectedCount) {
            this.maxRejectedCount = maxRejectedCount;
            return this;
        }
    }

    private static final StructType ORDER_EVENT_SCHEMA = new StructType(new StructField[] {
            DataTypes.createStructField("orderId", DataTypes.StringType, true),
            DataTypes.createStructField("customerId", DataTypes.StringType, true),
            DataTypes.createStructField("amount", DataTypes.DoubleType, true),
            DataTypes.createStructField("timestamp", DataTypes.StringType, true),
            DataTypes.createStructField("updatedAt", DataTypes.StringType, true),
            DataTypes.createStructField("eventVersion", DataTypes.LongType, true)
    });

    private static final StructType ORDER_EVENT_SCHEMA_WITH_CORRUPT = new StructType(new StructField[] {
            DataTypes.createStructField("orderId", DataTypes.StringType, true),
            DataTypes.createStructField("customerId", DataTypes.StringType, true),
            DataTypes.createStructField("amount", DataTypes.DoubleType, true),
            DataTypes.createStructField("timestamp", DataTypes.StringType, true),
            DataTypes.createStructField("updatedAt", DataTypes.StringType, true),
            DataTypes.createStructField("eventVersion", DataTypes.LongType, true),
            DataTypes.createStructField(CORRUPT_RECORD_COLUMN, DataTypes.StringType, true)
    });

    private final SparkSession sparkSession;

    public OrderTransformationService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    /**
     * BRONZE STAGE: Ingest raw JSON data incrementally using Databricks Auto Loader.
     */
    public void ingestBronze(String rawOrdersDir, String bronzeTable, String checkpointPath, String ingestionMode)
            throws TimeoutException, StreamingQueryException {
        ingestBronze(rawOrdersDir, bronzeTable, checkpointPath, ingestionMode, "", true, 0.1);
    }

    public void ingestBronze(
            String rawOrdersDir,
            String bronzeTable,
            String checkpointPath,
            String ingestionMode,
            String stageAuditTable,
            boolean alertOnAnomaly,
            double minOutputRatio)
            throws TimeoutException, StreamingQueryException {
        String trustedBronzeTable = requireTrustedTableName(bronzeTable, "bronzeTable");
        String trustedStageAuditTable = normalizeOptionalTrustedTableName(stageAuditTable, "stageAuditTable");
        String resolvedMode = resolveIngestionMode(ingestionMode);
        log.info("Ingesting Bronze from: {} to {} with mode: {}", rawOrdersDir, trustedBronzeTable, resolvedMode);

        long outputBefore = countTableIfExists(trustedBronzeTable);

        Dataset<Row> rawStream = buildBronzeInputStream(rawOrdersDir, checkpointPath, resolvedMode)
                .withColumn("ingest_timestamp", current_timestamp());

        StreamingQuery query = rawStream.writeStream()
                .format("delta")
                .option("checkpointLocation", checkpointPath)
                .trigger(org.apache.spark.sql.streaming.Trigger.AvailableNow())
                .toTable(trustedBronzeTable);

        query.awaitTermination();

        long outputAfter = countTableIfExists(trustedBronzeTable);
        long outputRecords = Math.max(0, outputAfter - outputBefore);
        logStageSummary(STAGE_BRONZE, -1, outputRecords, 0, 0, "inputUnavailable=streamingAutoLoader");
        maybeAlertStageAnomaly(STAGE_BRONZE, -1, outputRecords, 0, alertOnAnomaly, minOutputRatio, Long.MAX_VALUE);
        writeStageAudit(
                trustedStageAuditTable,
                STAGE_BRONZE,
                -1,
                outputRecords,
                0,
                0,
                STAGE_STATUS_SUCCESS,
                "inputUnavailable=streamingAutoLoader");
    }

    private Dataset<Row> buildBronzeInputStream(String rawOrdersDir, String checkpointPath, String ingestionMode) {
        String mode = resolveIngestionMode(ingestionMode);

        DataStreamReader reader = sparkSession.readStream()
                .option("multiLine", "true");

        if (INGESTION_MODE_JSON.equals(mode)) {
            log.info("Using local/test JSON ingestion mode with explicit order-event schema.");
            return reader
                    .format("json")
                    .schema(ORDER_EVENT_SCHEMA_WITH_CORRUPT)
                    .option("mode", "PERMISSIVE")
                    .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
                    .load(rawOrdersDir);
        }

        log.info("Using Databricks Auto Loader (cloudFiles) ingestion mode with explicit order-event schema.");
        return reader
                .format("cloudFiles")
                .schema(ORDER_EVENT_SCHEMA)
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaEvolutionMode", "rescue")
                .option("cloudFiles.schemaLocation", checkpointPath + "/_schema")
                .option("rescuedDataColumn", RESCUED_DATA_COLUMN)
                .load(rawOrdersDir);
    }

    String resolveIngestionMode(String ingestionMode) {
        if (ingestionMode == null) {
            return INGESTION_MODE_CLOUDFILES;
        }

        String normalizedMode = ingestionMode.trim().toLowerCase();
        if (INGESTION_MODE_JSON.equals(normalizedMode) || INGESTION_MODE_CLOUDFILES.equals(normalizedMode)) {
            return normalizedMode;
        }

        log.warn("Unsupported ingestion mode '{}', defaulting to '{}'.", ingestionMode, INGESTION_MODE_CLOUDFILES);
        return INGESTION_MODE_CLOUDFILES;
    }

    /**
     * SILVER STAGE: Validate bronze data, quarantine bad records, and MERGE clean records into Silver.
     */
    public void processSilver(
            String bronzeTable,
            String silverTable,
            String quarantineTable,
            boolean productionMode,
            long rescuedRecordAlertThreshold) {
        processSilver(
                bronzeTable,
                silverTable,
                quarantineTable,
                SilverProcessingOptions.defaults(productionMode, rescuedRecordAlertThreshold));
    }

    public void processSilver(
            String bronzeTable,
            String silverTable,
            String quarantineTable,
            SilverProcessingOptions options) {
        String trustedBronzeTable = requireTrustedTableName(bronzeTable, "bronzeTable");
        String trustedSilverTable = requireTrustedTableName(silverTable, "silverTable");
        String trustedQuarantineTable = requireTrustedTableName(quarantineTable, "quarantineTable");
        String trustedStageAuditTable = normalizeOptionalTrustedTableName(options.stageAuditTable, "stageAuditTable");
        log.info("Processing Silver from: {} to {}", trustedBronzeTable, trustedSilverTable);

        long outputBefore = countTableIfExists(trustedSilverTable);
        Dataset<Row> bronzeData = sparkSession.read().table(trustedBronzeTable);
        Dataset<Row> validatedOrders = buildValidatedOrders(
                bronzeData,
                options.requireUpdatedAt,
                options.requireEventVersion).cache();

        Dataset<Row> badRecords = buildBadRecords(validatedOrders).cache();
        Dataset<Row> cleanedOrders = buildCleanOrders(validatedOrders);

        long totalRecords = validatedOrders.count();
        long badRecordCount = badRecords.count();
        long rescuedRecordCount = badRecords
                .filter(col(RESCUED_DATA_COLUMN).isNotNull().or(col(CORRUPT_RECORD_COLUMN).isNotNull()))
                .count();
        long cleanRecordCount = totalRecords - badRecordCount;
        long quarantineRows = countQuarantineRows(badRecords);

        log.info(
                "Silver quality metrics | totalRecords={} cleanRecords={} badRecords={} rescuedRecords={}",
                totalRecords,
                cleanRecordCount,
                badRecordCount,
                rescuedRecordCount);

        if (badRecordCount > 0) {
            writeBadRecords(badRecords, trustedQuarantineTable);
        }

        if (options.productionMode && rescuedRecordCount > options.rescuedRecordAlertThreshold) {
            String errorMessage = String.format(
                    "Rescued/corrupt records (%d) exceeded threshold (%d).",
                    rescuedRecordCount,
                    options.rescuedRecordAlertThreshold);
            log.error("ALERT: {}", errorMessage);
            throw new IllegalStateException(errorMessage);
        }

        upsertSilver(cleanedOrders, trustedSilverTable);

        long outputAfter = countTableIfExists(trustedSilverTable);
        long outputRecords = Math.max(0, outputAfter - outputBefore);
        logStageSummary(STAGE_SILVER, totalRecords, outputRecords, badRecordCount, rescuedRecordCount,
                "cleanRecords=" + cleanRecordCount + ",quarantineRows=" + quarantineRows);
        maybeAlertStageAnomaly(
                STAGE_SILVER,
                totalRecords,
                cleanRecordCount,
                badRecordCount,
                options.alertOnAnomaly,
                options.minOutputRatio,
                options.maxRejectedCount);
        writeStageAudit(
                trustedStageAuditTable,
                STAGE_SILVER,
                totalRecords,
                outputRecords,
                badRecordCount,
                rescuedRecordCount,
                STAGE_STATUS_SUCCESS,
                "cleanRecords=" + cleanRecordCount + ",quarantineRows=" + quarantineRows);

        validatedOrders.unpersist();
        badRecords.unpersist();
    }

    private void writeBadRecords(Dataset<Row> badRecords, String quarantineTable) {
        String trustedQuarantineTable = requireTrustedTableName(quarantineTable, "quarantineTable");
        Dataset<Row> quarantineOutput = badRecords
                .withColumn(REJECTION_REASON_COLUMN, explode(col(VALIDATION_ERRORS_COLUMN)))
                .withColumn(REJECTION_CATEGORY_COLUMN, categorizeRejectionReason())
                .withColumn("quarantined_at", current_timestamp())
                .drop(VALIDATION_ERRORS_COLUMN, PARSED_TIMESTAMP_COLUMN, PARSED_UPDATED_AT_COLUMN);

        if (sparkSession.catalog().tableExists(trustedQuarantineTable)) {
            quarantineOutput.write().format("delta").mode("append").saveAsTable(trustedQuarantineTable);
            return;
        }

        quarantineOutput.write().format("delta").saveAsTable(trustedQuarantineTable);
    }

    private Column categorizeRejectionReason() {
        return when(col(REJECTION_REASON_COLUMN).isin(
                "MISSING_ORDER_ID",
                "MISSING_CUSTOMER_ID",
                "MISSING_AMOUNT",
                "MISSING_TIMESTAMP",
                "MISSING_UPDATED_AT",
                "MISSING_EVENT_VERSION",
                "INVALID_TIMESTAMP",
                "INVALID_UPDATED_AT",
                "INVALID_EVENT_VERSION"), lit("SCHEMA"))
                .when(col(REJECTION_REASON_COLUMN).isin("NON_POSITIVE_AMOUNT"), lit("BUSINESS_RULE"))
                .when(col(REJECTION_REASON_COLUMN).isin(
                        "RESCUED_DATA_PRESENT",
                        "CORRUPT_RECORD"), lit("INGESTION"))
                .otherwise(lit("UNKNOWN"));
    }

    private Dataset<Row> buildValidatedOrders(Dataset<Row> bronzeOrders) {
        return buildValidatedOrders(bronzeOrders, false, false);
    }

    private Dataset<Row> buildValidatedOrders(Dataset<Row> bronzeOrders, boolean requireUpdatedAt, boolean requireEventVersion) {
        Dataset<Row> normalizedOrders = ensureValidationColumns(bronzeOrders);

        return normalizedOrders
                .withColumn("orderId", trim(col("orderId")))
                .withColumn("customerId", trim(col("customerId")))
                .withColumn("timestamp", trim(col("timestamp")))
                .withColumn(
                        "updatedAt",
                        when(trim(col("updatedAt")).equalTo(""), lit(null)).otherwise(trim(col("updatedAt"))))
                .withColumn(PARSED_TIMESTAMP_COLUMN, parseEventTimestamp())
                .withColumn(
                        PARSED_UPDATED_AT_COLUMN,
                        requireUpdatedAt ? parseUpdatedAtTimestamp() : coalesce(parseUpdatedAtTimestamp(), col(PARSED_TIMESTAMP_COLUMN))
                )
                .withColumn(
                        VALIDATION_ERRORS_COLUMN,
                        array_compact(
                                array(
                                        when(col("orderId").isNull().or(trim(col("orderId")).equalTo("")), lit("MISSING_ORDER_ID"))
                                                .otherwise(lit(null)),
                                        when(col("customerId").isNull().or(trim(col("customerId")).equalTo("")), lit("MISSING_CUSTOMER_ID"))
                                                .otherwise(lit(null)),
                                        when(col("amount").isNull(), lit("MISSING_AMOUNT")).otherwise(lit(null)),
                                        when(col("amount").leq(0), lit("NON_POSITIVE_AMOUNT")).otherwise(lit(null)),
                                        when(col("timestamp").isNull().or(trim(col("timestamp")).equalTo("")), lit("MISSING_TIMESTAMP"))
                                                .otherwise(lit(null)),
                                        when(col("timestamp").isNotNull().and(col(PARSED_TIMESTAMP_COLUMN).isNull()), lit("INVALID_TIMESTAMP"))
                                                .otherwise(lit(null)),
                                        when(lit(requireUpdatedAt).and(col("updatedAt").isNull()), lit("MISSING_UPDATED_AT"))
                                                .otherwise(lit(null)),
                                        when(col("updatedAt").isNotNull().and(col(PARSED_UPDATED_AT_COLUMN).isNull()), lit("INVALID_UPDATED_AT"))
                                                .otherwise(lit(null)),
                                        when(lit(requireEventVersion).and(col("eventVersion").isNull()), lit("MISSING_EVENT_VERSION"))
                                                .otherwise(lit(null)),
                                        when(col("eventVersion").isNotNull().and(col("eventVersion").lt(0)), lit("INVALID_EVENT_VERSION"))
                                                .otherwise(lit(null)),
                                        when(col(RESCUED_DATA_COLUMN).isNotNull(), lit("RESCUED_DATA_PRESENT")).otherwise(lit(null)),
                                        when(col(CORRUPT_RECORD_COLUMN).isNotNull(), lit("CORRUPT_RECORD")).otherwise(lit(null))
                                )
                        )
                );
    }

    private Column parseEventTimestamp() {
        return coalesce(
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssX"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("timestamp"), "yyyy-MM-dd")
        );
    }

    private Column parseUpdatedAtTimestamp() {
        return coalesce(
                to_timestamp(col("updatedAt"), "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
                to_timestamp(col("updatedAt"), "yyyy-MM-dd'T'HH:mm:ssX"),
                to_timestamp(col("updatedAt"), "yyyy-MM-dd'T'HH:mm:ss"),
                to_timestamp(col("updatedAt"), "yyyy-MM-dd HH:mm:ss"),
                to_timestamp(col("updatedAt"), "yyyy-MM-dd")
        );
    }

    private Dataset<Row> ensureValidationColumns(Dataset<Row> bronzeOrders) {
        Dataset<Row> normalizedOrders = bronzeOrders;

        if (!hasColumn(normalizedOrders, RESCUED_DATA_COLUMN)) {
            normalizedOrders = normalizedOrders.withColumn(RESCUED_DATA_COLUMN, lit(null).cast(DataTypes.StringType));
        }

        if (!hasColumn(normalizedOrders, CORRUPT_RECORD_COLUMN)) {
            normalizedOrders = normalizedOrders.withColumn(CORRUPT_RECORD_COLUMN, lit(null).cast(DataTypes.StringType));
        }

        if (!hasColumn(normalizedOrders, "updatedAt")) {
            normalizedOrders = normalizedOrders.withColumn("updatedAt", lit(null).cast(DataTypes.StringType));
        }

        if (!hasColumn(normalizedOrders, "eventVersion")) {
            normalizedOrders = normalizedOrders.withColumn("eventVersion", lit(null).cast(DataTypes.LongType));
        }

        return normalizedOrders;
    }

    private boolean hasColumn(Dataset<Row> dataset, String columnName) {
        for (StructField field : dataset.schema().fields()) {
            if (field.name().equals(columnName)) {
                return true;
            }
        }
        return false;
    }

    private Dataset<Row> buildBadRecords(Dataset<Row> validatedOrders) {
        return validatedOrders.filter(size(col(VALIDATION_ERRORS_COLUMN)).gt(0));
    }

    private Dataset<Row> buildCleanOrders(Dataset<Row> validatedOrders) {
        return validatedOrders
                .filter(size(col(VALIDATION_ERRORS_COLUMN)).equalTo(0))
                .filter(col(PARSED_TIMESTAMP_COLUMN).isNotNull())
                .filter(col(PARSED_UPDATED_AT_COLUMN).isNotNull())
                .withColumn("timestamp", col(PARSED_TIMESTAMP_COLUMN))
                .withColumn(UPDATED_AT_COLUMN, col(PARSED_UPDATED_AT_COLUMN))
                .withColumn(EVENT_VERSION_COLUMN, col("eventVersion"))
                .withColumn(ORDER_DATE_COLUMN, to_date(col(PARSED_TIMESTAMP_COLUMN)))
                .drop(
                        VALIDATION_ERRORS_COLUMN,
                        PARSED_TIMESTAMP_COLUMN,
                        PARSED_UPDATED_AT_COLUMN,
                        RESCUED_DATA_COLUMN,
                        CORRUPT_RECORD_COLUMN,
                        "updatedAt",
                        "eventVersion");
    }

    private void upsertSilver(Dataset<Row> cleanedOrders, String silverTable) {
        String trustedSilverTable = requireTrustedTableName(silverTable, "silverTable");
        Dataset<Row> latestCleanedOrders = selectLatestOrderEvents(cleanedOrders);

        if (!sparkSession.catalog().tableExists(trustedSilverTable)) {
            latestCleanedOrders.write().format("delta").saveAsTable(trustedSilverTable);
            return;
        }

        ensureSilverMutableSchema(trustedSilverTable);

        DeltaTable silverDeltaTable = DeltaTable.forName(sparkSession, trustedSilverTable);
        silverDeltaTable.as("target")
                .merge(
                        latestCleanedOrders.as("source"),
                        "target.orderId = source.orderId"
                )
                .whenMatched(
                        "(source.event_version IS NOT NULL AND " +
                                "(target.event_version IS NULL OR source.event_version > target.event_version OR " +
                                "(source.event_version = target.event_version AND source.updated_at > target.updated_at))) " +
                                "OR (source.event_version IS NULL AND source.updated_at > target.updated_at)"
                ).updateAll()
                .whenNotMatched().insertAll()
                .execute();
    }

    private Dataset<Row> selectLatestOrderEvents(Dataset<Row> cleanedOrders) {
        WindowSpec orderWindow = Window.partitionBy("orderId")
                .orderBy(
                        col(UPDATED_AT_COLUMN).desc(),
                        col(EVENT_VERSION_COLUMN).desc_nulls_last(),
                        col("ingest_timestamp").desc_nulls_last(),
                        col("timestamp").desc()
                );

        return cleanedOrders
                .withColumn("row_number", org.apache.spark.sql.functions.row_number().over(orderWindow))
                .filter(col("row_number").equalTo(1))
                .drop("row_number");
    }

    private void ensureSilverMutableSchema(String silverTable) {
        String trustedSilverTable = requireTrustedTableName(silverTable, "silverTable");
        String quotedSilverTable = quoteTableIdentifier(trustedSilverTable);
        Dataset<Row> silverData = sparkSession.read().table(trustedSilverTable);

        if (!hasColumn(silverData, UPDATED_AT_COLUMN)) {
            sparkSession.sql("ALTER TABLE " + quotedSilverTable + " ADD COLUMNS (" + UPDATED_AT_COLUMN + " TIMESTAMP)");
        }

        if (!hasColumn(silverData, EVENT_VERSION_COLUMN)) {
            sparkSession.sql("ALTER TABLE " + quotedSilverTable + " ADD COLUMNS (" + EVENT_VERSION_COLUMN + " BIGINT)");
        }

        sparkSession.sql(
                "UPDATE " + quotedSilverTable + " SET " + UPDATED_AT_COLUMN + " = COALESCE(" + UPDATED_AT_COLUMN + ", CAST(timestamp AS TIMESTAMP), ingest_timestamp) " +
                        "WHERE " + UPDATED_AT_COLUMN + " IS NULL");
    }

    /**
     * Backward-compatible cleaning API used by tests and local transformations.
     */
    public Dataset<Row> cleanOrders(Dataset<Row> rawOrders) {
        Dataset<Row> validatedOrders = buildValidatedOrders(rawOrders);
        return buildCleanOrders(validatedOrders);
    }

    /**
     * Aggregates daily metrics from the cleaned orders dataset.
     */
    public Dataset<Row> aggregateDailyMetrics(Dataset<Row> cleanedOrders) {
        Dataset<Row> normalizedOrders = normalizeOrderDateColumn(cleanedOrders);
        return normalizedOrders
                .groupBy(ORDER_DATE_COLUMN)
                .agg(
                        count("orderId").alias("totalOrders"),
                        sum("amount").alias("totalRevenue")
                );
    }

    /**
     * GOLD STAGE: Recompute only affected dates from Silver and MERGE daily metrics into Gold.
     */
    public void aggregateGold(String silverTable, String goldTable) {
        aggregateGold(silverTable, goldTable, "", true, 0.1);
    }

    public void aggregateGold(
            String silverTable,
            String goldTable,
            String stageAuditTable,
            boolean alertOnAnomaly,
            double minOutputRatio) {
        String trustedSilverTable = requireTrustedTableName(silverTable, "silverTable");
        String trustedGoldTable = requireTrustedTableName(goldTable, "goldTable");
        String trustedStageAuditTable = normalizeOptionalTrustedTableName(stageAuditTable, "stageAuditTable");
        log.info("Incrementally aggregating Gold from: {} to {}", trustedSilverTable, trustedGoldTable);

        Dataset<Row> silverData = normalizeOrderDateColumn(sparkSession.read().table(trustedSilverTable));
        Dataset<Row> affectedDates = resolveAffectedGoldDates(silverData, trustedGoldTable).cache();
        long affectedDateCount = affectedDates.count();

        if (affectedDateCount == 0) {
            log.info("No affected Gold dates found. Skipping Gold aggregation.");
            affectedDates.unpersist();
            return;
        }

        log.info("Recomputing Gold metrics for {} affected date(s).", affectedDateCount);

        Dataset<Row> affectedSilverData = silverData.join(affectedDates, ORDER_DATE_COLUMN);
        long inputRecords = affectedSilverData.count();
        Dataset<Row> dailyMetrics = buildGoldDailyMetrics(affectedSilverData);
        long outputRecords = dailyMetrics.count();

        upsertGold(dailyMetrics, trustedGoldTable);
        logStageSummary(STAGE_GOLD, inputRecords, outputRecords, 0, 0, "affectedDates=" + affectedDateCount);
        maybeAlertStageAnomaly(STAGE_GOLD, inputRecords, outputRecords, 0, alertOnAnomaly, minOutputRatio, Long.MAX_VALUE);
        writeStageAudit(
                trustedStageAuditTable,
                STAGE_GOLD,
                inputRecords,
                outputRecords,
                0,
                0,
                STAGE_STATUS_SUCCESS,
                "affectedDates=" + affectedDateCount);
        affectedDates.unpersist();
    }

    private Dataset<Row> resolveAffectedGoldDates(Dataset<Row> silverData, String goldTable) {
        String trustedGoldTable = requireTrustedTableName(goldTable, "goldTable");
        Dataset<Row> silverDateState = silverData
                .groupBy(ORDER_DATE_COLUMN)
                .agg(max("ingest_timestamp").alias("source_max_ingest_timestamp"));

        if (!sparkSession.catalog().tableExists(trustedGoldTable)) {
            return silverDateState.select(ORDER_DATE_COLUMN);
        }

        Dataset<Row> goldData = normalizeOrderDateColumn(sparkSession.read().table(trustedGoldTable));
        if (!hasColumn(goldData, "source_max_ingest_timestamp")) {
            log.warn(
                    "Gold table {} does not contain source_max_ingest_timestamp. Recomputing all Silver dates once.",
                    trustedGoldTable);
            return silverDateState.select(ORDER_DATE_COLUMN);
        }

        Dataset<Row> goldDateState = goldData
                .select(ORDER_DATE_COLUMN, "source_max_ingest_timestamp");

        return silverDateState.as("silver")
                .join(
                        goldDateState.as("gold"),
                        col("silver." + ORDER_DATE_COLUMN).equalTo(col("gold." + ORDER_DATE_COLUMN)),
                        "left"
                )
                .filter(
                        col("gold." + ORDER_DATE_COLUMN).isNull()
                                .or(col("silver.source_max_ingest_timestamp").gt(col("gold.source_max_ingest_timestamp")))
                )
                .select(col("silver." + ORDER_DATE_COLUMN).alias(ORDER_DATE_COLUMN));
    }

    private Dataset<Row> buildGoldDailyMetrics(Dataset<Row> affectedSilverData) {
        return affectedSilverData
                .groupBy(ORDER_DATE_COLUMN)
                .agg(
                        count("orderId").alias("totalOrders"),
                        sum("amount").alias("totalRevenue"),
                        max("ingest_timestamp").alias("source_max_ingest_timestamp")
                )
                .withColumn("gold_updated_at", current_timestamp());
    }

    private void upsertGold(Dataset<Row> dailyMetrics, String goldTable) {
        String trustedGoldTable = requireTrustedTableName(goldTable, "goldTable");
        if (!sparkSession.catalog().tableExists(trustedGoldTable)) {
            dailyMetrics.write()
                    .format("delta")
                    .partitionBy(ORDER_DATE_COLUMN)
                    .saveAsTable(trustedGoldTable);
            return;
        }

        Dataset<Row> goldData = sparkSession.read().table(trustedGoldTable);
        boolean useLegacyTimestampKey = !hasColumn(goldData, ORDER_DATE_COLUMN) && hasColumn(goldData, "timestamp");

        DeltaTable goldDeltaTable = DeltaTable.forName(sparkSession, trustedGoldTable);
        if (useLegacyTimestampKey) {
            Dataset<Row> legacySource = dailyMetrics.withColumn("timestamp", col(ORDER_DATE_COLUMN));
            goldDeltaTable.as("target")
                    .merge(
                            legacySource.as("source"),
                            "target.timestamp = source.timestamp"
                    )
                    .whenMatched().updateAll()
                    .whenNotMatched().insertAll()
                    .execute();
            return;
        }

        goldDeltaTable.as("target")
                .merge(
                        dailyMetrics.as("source"),
                        "target." + ORDER_DATE_COLUMN + " = source." + ORDER_DATE_COLUMN
                )
                .whenMatched().updateAll()
                .whenNotMatched().insertAll()
                .execute();
    }

    private Dataset<Row> normalizeOrderDateColumn(Dataset<Row> dataset) {
        if (hasColumn(dataset, ORDER_DATE_COLUMN)) {
            return dataset;
        }

        if (hasColumn(dataset, "timestamp")) {
            log.warn("Dataset does not contain {}, deriving it from legacy timestamp column.", ORDER_DATE_COLUMN);
            return dataset.withColumn(ORDER_DATE_COLUMN, to_date(col("timestamp")));
        }

        return dataset;
    }

    /**
     * MAINTENANCE STAGE: Optimize and Vacuum Delta tables to maintain performance.
     */
    public void performMaintenance(String... tables) {
        performMaintenance("", true, 168, true, 24, tables);
    }

    /**
     * MAINTENANCE STAGE (Best-effort): runs OPTIMIZE/VACUUM, logs summary metrics, emits alerts,
     * and optionally writes maintenance audit rows to a Delta table.
     */
    public void performMaintenance(
            String maintenanceAuditTable,
            boolean alertOnFailure,
            int vacuumRetentionHours,
            boolean auditReplayRequired,
            int slowDownstreamMaxLagHours,
            String... tables) {
        String trustedMaintenanceAuditTable = normalizeOptionalTrustedTableName(maintenanceAuditTable, "maintenanceAuditTable");
        log.info(
                "Starting Delta Lake Maintenance... vacuumRetentionHours={} auditReplayRequired={} slowDownstreamMaxLagHours={}",
                vacuumRetentionHours,
                auditReplayRequired,
                slowDownstreamMaxLagHours);

        String maintenanceRunId = UUID.randomUUID().toString();
        Timestamp runTimestamp = new Timestamp(System.currentTimeMillis());
        List<Row> maintenanceAuditRows = new ArrayList<>();

        int requiredRetentionHours = Math.max(auditReplayRequired ? 168 : 0, slowDownstreamMaxLagHours);
        boolean retentionPolicyRisk = vacuumRetentionHours < requiredRetentionHours;

        int successCount = 0;
        int skippedCount = 0;
        int failureCount = 0;
        int permissionFailureCount = 0;

        for (String table : tables) {
            String trustedTable;
            try {
                trustedTable = requireTrustedTableName(table, "maintenanceTable");
            } catch (IllegalArgumentException illegalArgumentException) {
                failureCount++;
                maintenanceAuditRows.add(buildMaintenanceAuditRow(
                        maintenanceRunId,
                        runTimestamp,
                        table,
                        MAINTENANCE_STATUS_FAILED,
                        false,
                        retentionPolicyRisk,
                        vacuumRetentionHours,
                        auditReplayRequired,
                        slowDownstreamMaxLagHours,
                        illegalArgumentException.getClass().getSimpleName(),
                        illegalArgumentException.getMessage()));
                log.error("Skipping maintenance due to invalid trusted table identifier: {}", table, illegalArgumentException);
                continue;
            }

            boolean tableExists = sparkSession.catalog().tableExists(trustedTable);
            String quotedTrustedTable = quoteTableIdentifier(trustedTable);
            try {
                if (tableExists) {
                    log.info("Running OPTIMIZE on {}", trustedTable);
                    sparkSession.sql("OPTIMIZE " + quotedTrustedTable);

                    log.info("Running VACUUM on {} with RETAIN {} HOURS", trustedTable, vacuumRetentionHours);
                    sparkSession.sql("VACUUM " + quotedTrustedTable + " RETAIN " + vacuumRetentionHours + " HOURS");

                    successCount++;
                    maintenanceAuditRows.add(buildMaintenanceAuditRow(
                            maintenanceRunId,
                            runTimestamp,
                            trustedTable,
                            MAINTENANCE_STATUS_SUCCESS,
                            false,
                            retentionPolicyRisk,
                            vacuumRetentionHours,
                            auditReplayRequired,
                            slowDownstreamMaxLagHours,
                            null,
                            null));
                } else {
                    log.warn("Table {} does not exist, skipping maintenance.", trustedTable);

                    skippedCount++;
                    maintenanceAuditRows.add(buildMaintenanceAuditRow(
                            maintenanceRunId,
                            runTimestamp,
                            trustedTable,
                            MAINTENANCE_STATUS_SKIPPED,
                            false,
                            retentionPolicyRisk,
                            vacuumRetentionHours,
                            auditReplayRequired,
                            slowDownstreamMaxLagHours,
                            null,
                            "Table does not exist"));
                }
            } catch (Exception exception) {
                log.error("Failed to perform maintenance on table {}", trustedTable, exception);

                failureCount++;
                boolean permissionFailure = looksLikePermissionIssue(exception);
                if (permissionFailure) {
                    permissionFailureCount++;
                }

                maintenanceAuditRows.add(buildMaintenanceAuditRow(
                        maintenanceRunId,
                        runTimestamp,
                        trustedTable,
                        MAINTENANCE_STATUS_FAILED,
                        permissionFailure,
                        retentionPolicyRisk,
                        vacuumRetentionHours,
                        auditReplayRequired,
                        slowDownstreamMaxLagHours,
                        exception.getClass().getSimpleName(),
                        exception.getMessage()));
            }
        }

        writeMaintenanceAudit(trustedMaintenanceAuditTable, maintenanceAuditRows);

        log.info(
                "Maintenance summary | runId={} success={} skipped={} failed={} permissionFailures={}",
                maintenanceRunId,
                successCount,
                skippedCount,
                failureCount,
                permissionFailureCount);

        if (retentionPolicyRisk) {
            log.error(
                    "ALERT: Maintenance retention policy risk | runId={} vacuumRetentionHours={} requiredRetentionHours={} auditReplayRequired={} slowDownstreamMaxLagHours={}",
                    maintenanceRunId,
                    vacuumRetentionHours,
                    requiredRetentionHours,
                    auditReplayRequired,
                    slowDownstreamMaxLagHours);
        }

        if (alertOnFailure && failureCount > 0) {
            log.error(
                    "ALERT: Maintenance failures detected | runId={} failed={} permissionFailures={}",
                    maintenanceRunId,
                    failureCount,
                    permissionFailureCount);
        }

        if (alertOnFailure && permissionFailureCount > 0) {
            log.error(
                    "ALERT: Maintenance permission issue suspected (missing OPTIMIZE/VACUUM privileges) | runId={} permissionFailures={}",
                    maintenanceRunId,
                    permissionFailureCount);
        }

        log.info("Maintenance completed.");
    }

    private Row buildMaintenanceAuditRow(
            String runId,
            Timestamp runTimestamp,
            String tableName,
            String status,
            boolean permissionFailure,
            boolean retentionPolicyRisk,
            int vacuumRetentionHours,
            boolean auditReplayRequired,
            int slowDownstreamMaxLagHours,
            String errorClass,
            String errorMessage) {
        return org.apache.spark.sql.RowFactory.create(
                runId,
                runTimestamp,
                tableName,
                status,
                permissionFailure,
                retentionPolicyRisk,
                vacuumRetentionHours,
                auditReplayRequired,
                slowDownstreamMaxLagHours,
                errorClass,
                errorMessage);
    }

    private void writeMaintenanceAudit(String maintenanceAuditTable, List<Row> maintenanceAuditRows) {
        String trustedMaintenanceAuditTable = normalizeOptionalTrustedTableName(maintenanceAuditTable, "maintenanceAuditTable");
        if (trustedMaintenanceAuditTable == null) {
            return;
        }

        StructType maintenanceAuditSchema = new StructType(new StructField[] {
                DataTypes.createStructField("maintenance_run_id", DataTypes.StringType, false),
                DataTypes.createStructField("run_timestamp", DataTypes.TimestampType, false),
                DataTypes.createStructField("table_name", DataTypes.StringType, false),
                DataTypes.createStructField("status", DataTypes.StringType, false),
                DataTypes.createStructField("permission_failure", DataTypes.BooleanType, false),
                DataTypes.createStructField("retention_policy_risk", DataTypes.BooleanType, false),
                DataTypes.createStructField("vacuum_retention_hours", DataTypes.IntegerType, false),
                DataTypes.createStructField("audit_replay_required", DataTypes.BooleanType, false),
                DataTypes.createStructField("slow_downstream_max_lag_hours", DataTypes.IntegerType, false),
                DataTypes.createStructField("error_class", DataTypes.StringType, true),
                DataTypes.createStructField("error_message", DataTypes.StringType, true)
        });

        Dataset<Row> maintenanceAuditDataset = sparkSession.createDataFrame(maintenanceAuditRows, maintenanceAuditSchema);
        if (sparkSession.catalog().tableExists(trustedMaintenanceAuditTable)) {
            maintenanceAuditDataset.write().format("delta").mode("append").saveAsTable(trustedMaintenanceAuditTable);
            return;
        }

        maintenanceAuditDataset.write().format("delta").saveAsTable(trustedMaintenanceAuditTable);
    }

    private boolean looksLikePermissionIssue(Exception exception) {
        String message = exception.getMessage();
        if (message == null) {
            return false;
        }

        String normalized = message.toLowerCase();
        return normalized.contains("permission")
                || normalized.contains("not authorized")
                || normalized.contains("unauthorized")
                || normalized.contains("access denied")
                || normalized.contains("insufficient privileges");
    }

    private String normalizeOptionalTrustedTableName(String tableName, String configName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            return null;
        }

        return requireTrustedTableName(tableName, configName);
    }

    private String requireTrustedTableName(String tableName, String configName) {
        if (tableName == null || tableName.trim().isEmpty()) {
            throw new IllegalArgumentException(configName + " must be configured with a trusted table name.");
        }

        String normalizedTableName = tableName.trim();
        if (!TRUSTED_TABLE_IDENTIFIER_PATTERN.matcher(normalizedTableName).matches()) {
            throw new IllegalArgumentException(
                    configName + " must be a trusted table identifier with 1 to 3 dot-separated parts using only letters, digits, and underscores: "
                            + tableName);
        }

        return normalizedTableName;
    }

    private String quoteTableIdentifier(String tableName) {
        String trustedTableName = requireTrustedTableName(tableName, "tableName");
        String[] identifierParts = trustedTableName.split("\\.");
        List<String> quotedParts = new ArrayList<>();
        for (String identifierPart : identifierParts) {
            quotedParts.add("`" + identifierPart + "`");
        }
        return String.join(".", quotedParts);
    }

    private long countTableIfExists(String tableName) {
        try {
            String trustedTableName = normalizeOptionalTrustedTableName(tableName, "tableName");
            if (trustedTableName == null || !sparkSession.catalog().tableExists(trustedTableName)) {
                return 0;
            }
            return sparkSession.read().table(trustedTableName).count();
        } catch (Exception exception) {
            log.warn("Unable to count table {} for stage audit.", tableName, exception);
            return -1;
        }
    }

    private long countQuarantineRows(Dataset<Row> badRecords) {
        if (badRecords.isEmpty()) {
            return 0;
        }

        return badRecords
                .select(explode(col(VALIDATION_ERRORS_COLUMN)).alias(REJECTION_REASON_COLUMN))
                .count();
    }

    private void logStageSummary(
            String stageName,
            long inputRecords,
            long outputRecords,
            long rejectedRecords,
            long rescuedRecords,
            String details) {
        log.info(
                "Stage summary | stage={} inputRecords={} outputRecords={} rejectedRecords={} rescuedRecords={} details={}",
                stageName,
                inputRecords,
                outputRecords,
                rejectedRecords,
                rescuedRecords,
                details);
    }

    private void maybeAlertStageAnomaly(
            String stageName,
            long inputRecords,
            long outputRecords,
            long rejectedRecords,
            boolean alertOnAnomaly,
            double minOutputRatio,
            long maxRejectedCount) {
        if (!alertOnAnomaly) {
            return;
        }

        if (inputRecords > 0 && outputRecords == 0) {
            log.error(
                    "ALERT: Stage produced zero output for non-zero input | stage={} inputRecords={} rejectedRecords={}",
                    stageName,
                    inputRecords,
                    rejectedRecords);
        }

        if (inputRecords > 0 && outputRecords > 0) {
            double outputRatio = (double) outputRecords / (double) inputRecords;
            if (outputRatio < minOutputRatio) {
                log.error(
                        "ALERT: Stage output ratio below threshold | stage={} inputRecords={} outputRecords={} ratio={} minRatio={}",
                        stageName,
                        inputRecords,
                        outputRecords,
                        outputRatio,
                        minOutputRatio);
            }
        }

        if (rejectedRecords > maxRejectedCount) {
            log.error(
                    "ALERT: Stage rejected count exceeded threshold | stage={} rejectedRecords={} maxRejectedCount={}",
                    stageName,
                    rejectedRecords,
                    maxRejectedCount);
        }
    }

    private void writeStageAudit(
            String stageAuditTable,
            String stageName,
            long inputRecords,
            long outputRecords,
            long rejectedRecords,
            long rescuedRecords,
            String status,
            String details) {
        String trustedStageAuditTable = normalizeOptionalTrustedTableName(stageAuditTable, "stageAuditTable");
        if (trustedStageAuditTable == null) {
            return;
        }

        StructType stageAuditSchema = new StructType(new StructField[] {
                DataTypes.createStructField("stage_run_id", DataTypes.StringType, false),
                DataTypes.createStructField("run_timestamp", DataTypes.TimestampType, false),
                DataTypes.createStructField("stage_name", DataTypes.StringType, false),
                DataTypes.createStructField("input_records", DataTypes.LongType, false),
                DataTypes.createStructField("output_records", DataTypes.LongType, false),
                DataTypes.createStructField("rejected_records", DataTypes.LongType, false),
                DataTypes.createStructField("rescued_records", DataTypes.LongType, false),
                DataTypes.createStructField("status", DataTypes.StringType, false),
                DataTypes.createStructField("details", DataTypes.StringType, true)
        });

        List<Row> stageAuditRows = List.of(org.apache.spark.sql.RowFactory.create(
                UUID.randomUUID().toString(),
                new Timestamp(System.currentTimeMillis()),
                stageName,
                inputRecords,
                outputRecords,
                rejectedRecords,
                rescuedRecords,
                status,
                details));

        Dataset<Row> stageAuditDataset = sparkSession.createDataFrame(stageAuditRows, stageAuditSchema);
        if (sparkSession.catalog().tableExists(trustedStageAuditTable)) {
            stageAuditDataset.write().format("delta").mode("append").saveAsTable(trustedStageAuditTable);
            return;
        }

        stageAuditDataset.write().format("delta").saveAsTable(trustedStageAuditTable);
    }
}
