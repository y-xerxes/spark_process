package com.platform.data.framework.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Data
@ConfigurationProperties(prefix = "test.db")
public class DatabaseConfigGroup {
    private List<DatabaseConfig> databaseConfigs;
}
