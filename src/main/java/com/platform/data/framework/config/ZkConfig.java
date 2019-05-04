package com.platform.data.framework.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "test.zookeeper")
public class ZkConfig {
    private String zkServers;
}
