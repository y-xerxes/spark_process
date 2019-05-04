package com.platform.data.framework.autoconfiguration;

import com.platform.data.framework.config.*;
import com.platform.data.framework.SparkAppRunner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
@ConditionalOnProperty(
        prefix = "test",
        name = {"spark.enabled"}
)
@EnableConfigurationProperties({SparkDevelopmentConfig.class, HdfsConfig.class, DatabaseConfigGroup.class,
        LivyConfig.class, KafkaConfig.class, ZkConfig.class})
public class SparkAutoConfiguration {

    @Bean
    public SparkAppRunner sparkAppRunner(
            SparkDevelopmentConfig sparkDevelopmentConfig,
            HdfsConfig hdfsConfig, DatabaseConfigGroup databaseConfigGroup, LivyConfig livyConfig,
            KafkaConfig kafkaConfig, ZkConfig zkConfig) {
        return new SparkAppRunner(sparkDevelopmentConfig, hdfsConfig, kafkaConfig,
                databaseConfigGroup, livyConfig, zkConfig);
    }
}
