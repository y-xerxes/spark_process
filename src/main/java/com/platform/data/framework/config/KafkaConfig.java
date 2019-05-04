package com.platform.data.framework.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "test.kafka")
public class KafkaConfig {
    private String bootstrapServers;
}
