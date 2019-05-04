package com.platform.data.framework.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@Data
@ConfigurationProperties(prefix = "test.hdfs")
public class HdfsConfig {
    private String nameNodeName;
    private List<String> nameNodes;
}
