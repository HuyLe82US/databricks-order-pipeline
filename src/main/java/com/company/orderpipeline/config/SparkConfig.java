package com.company.orderpipeline.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spark.master:}")
    private String sparkMaster;

    @Value("${spark.app.name:OrderPipeline}")
    private String sparkAppName;

    @Bean
    public SparkSession sparkSession() {
        // In Databricks, SparkSession is already created and available.
        // We use builder().getOrCreate() to attach to the existing session.
        SparkSession.Builder builder = SparkSession.builder()
                .appName(sparkAppName);

        // For local execution/testing, we might want to specify a master
        if (sparkMaster != null && !sparkMaster.isEmpty()) {
            builder.master(sparkMaster);
        }

        return builder.getOrCreate();
    }
}
