package com.platform.data.framework.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "test.livy")
public class LivyConfig {
    private String livyAddress;
}
