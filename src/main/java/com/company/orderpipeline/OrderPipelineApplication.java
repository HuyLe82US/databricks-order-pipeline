package com.company.orderpipeline;

import com.company.orderpipeline.service.OrderTransformationService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

@SpringBootApplication
public class OrderPipelineApplication {

    private static final Logger log = LoggerFactory.getLogger(OrderPipelineApplication.class);
    private static final List<String> EXTERNAL_TABLE_OVERRIDE_PREFIXES = Arrays.asList(
            "--pipeline.orders.bronze.table=",
            "--pipeline.orders.silver.table=",
            "--pipeline.orders.gold.table=",
            "--pipeline.orders.quarantine.table=");

    public static void main(String[] args) {
        SpringApplication.run(OrderPipelineApplication.class, args);
    }

    @org.springframework.beans.factory.annotation.Value("${pipeline.orders.raw.dir}")
    private String rawOrdersDir;

    @org.springframework.beans.factory.annotation.Value("${pipeline.orders.bronze.table}")
    private String bronzeTable;

    @org.springframework.beans.factory.annotation.Value("${pipeline.orders.silver.table}")
    private String silverTable;

    @org.springframework.beans.factory.annotation.Value("${pipeline.orders.quarantine.table}")
    private String quarantineTable;

    @org.springframework.beans.factory.annotation.Value("${pipeline.orders.gold.table}")
    private String goldTable;

    @org.springframework.beans.factory.annotation.Value("${pipeline.checkpoint.bronze}")
    private String bronzeCheckpoint;

    @org.springframework.beans.factory.annotation.Value("${pipeline.ingestion.mode:cloudFiles}")
    private String ingestionMode;

    @org.springframework.beans.factory.annotation.Value("${pipeline.production.mode:false}")
    private boolean productionMode;

    @org.springframework.beans.factory.annotation.Value("${pipeline.contract.require-updated-at:${pipeline.production.mode:false}}")
    private boolean requireUpdatedAt;

    @org.springframework.beans.factory.annotation.Value("${pipeline.contract.require-event-version:${pipeline.production.mode:false}}")
    private boolean requireEventVersion;

    @org.springframework.beans.factory.annotation.Value("${pipeline.alert.rescued-record-threshold:100}")
    private long rescuedRecordAlertThreshold;

    @org.springframework.beans.factory.annotation.Value("${pipeline.maintenance.audit.table:}")
    private String maintenanceAuditTable;

    @org.springframework.beans.factory.annotation.Value("${pipeline.maintenance.alert.on-failure:true}")
    private boolean maintenanceAlertOnFailure;

    @org.springframework.beans.factory.annotation.Value("${pipeline.maintenance.vacuum-retention-hours:168}")
    private int maintenanceVacuumRetentionHours;

    @org.springframework.beans.factory.annotation.Value("${pipeline.maintenance.audit-replay-required:true}")
    private boolean maintenanceAuditReplayRequired;

    @org.springframework.beans.factory.annotation.Value("${pipeline.maintenance.slow-downstream-max-lag-hours:24}")
    private int maintenanceSlowDownstreamMaxLagHours;

    @org.springframework.beans.factory.annotation.Value("${pipeline.stage.audit.table:}")
    private String stageAuditTable;

    @org.springframework.beans.factory.annotation.Value("${pipeline.stage.alert.on-anomaly:true}")
    private boolean stageAlertOnAnomaly;

    @org.springframework.beans.factory.annotation.Value("${pipeline.stage.alert.min-output-ratio:0.1}")
    private double stageMinOutputRatio;

    @org.springframework.beans.factory.annotation.Value("${pipeline.stage.alert.max-rejected-count:1000}")
    private long stageMaxRejectedCount;

    @Bean
    public CommandLineRunner runPipeline(ApplicationContext ctx, OrderTransformationService transformationService) {
        return args -> {
            log.info("Starting Databricks Order Pipeline...");
            ensureNoExternalTableNameOverrides(args);

            String stage = args.length > 0 ? args[0].toLowerCase() : "all";

            try {
                if ("bronze".equals(stage) || "all".equals(stage)) {
                    log.info("--- Running Bronze Stage ---");
                    transformationService.ingestBronze(
                            rawOrdersDir,
                            bronzeTable,
                            bronzeCheckpoint,
                            ingestionMode,
                            stageAuditTable,
                            stageAlertOnAnomaly,
                            stageMinOutputRatio);
                }
                if ("silver".equals(stage) || "all".equals(stage)) {
                    log.info("--- Running Silver Stage ---");
                    transformationService.processSilver(
                            bronzeTable,
                            silverTable,
                            quarantineTable,
                            productionMode,
                            rescuedRecordAlertThreshold,
                            requireUpdatedAt,
                            requireEventVersion,
                            stageAuditTable,
                            stageAlertOnAnomaly,
                            stageMinOutputRatio,
                            stageMaxRejectedCount);
                }
                if ("gold".equals(stage) || "all".equals(stage)) {
                    log.info("--- Running Gold Stage ---");
                    transformationService.aggregateGold(
                            silverTable,
                            goldTable,
                            stageAuditTable,
                            stageAlertOnAnomaly,
                            stageMinOutputRatio);
                }
                if ("maintenance".equals(stage) || "all".equals(stage)) {
                    log.info("--- Running Maintenance Stage ---");
                    transformationService.performMaintenance(
                            maintenanceAuditTable,
                            maintenanceAlertOnFailure,
                            maintenanceVacuumRetentionHours,
                            maintenanceAuditReplayRequired,
                            maintenanceSlowDownstreamMaxLagHours,
                            bronzeTable,
                            silverTable,
                            goldTable);
                }
            } catch (Exception e) {
                log.error("Error during pipeline execution", e);
                throw e; // Fail the job
            }

            log.info("Pipeline execution completed successfully.");
        };
    }

    private void ensureNoExternalTableNameOverrides(String[] args) {
        for (String argument : args) {
            String normalizedArgument = argument.toLowerCase(Locale.ROOT);
            for (String blockedPrefix : EXTERNAL_TABLE_OVERRIDE_PREFIXES) {
                if (normalizedArgument.startsWith(blockedPrefix)) {
                    throw new IllegalArgumentException(
                            "External table-name override is not allowed: " + blockedPrefix + "*. Use trusted config files or bundle target variables.");
                }
            }
        }
    }
}
