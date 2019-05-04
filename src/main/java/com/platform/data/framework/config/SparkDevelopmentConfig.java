package com.platform.data.framework.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Data
@ConfigurationProperties(prefix = "test.spark")
public class SparkDevelopmentConfig {
    private boolean enabled;
    private String appName;
    private String sparkHome;
    private String masterUrl;
    private String runAppName;
    private List<String> runArgs;
}
