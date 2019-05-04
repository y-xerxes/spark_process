package com.platform.data.framework;

import com.google.common.base.Preconditions;
import com.platform.data.framework.config.HdfsConfig;
import com.platform.data.framework.config.KafkaConfig;
import com.platform.data.framework.config.ZkConfig;
import com.platform.data.framework.utils.KafkaManager;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"WeakerAccess", "unused"})
@Slf4j
public abstract class AbstractSparkStreamApp extends AbstractSparkApp {

    protected transient JavaStreamingContext javaStreamingContext;

    public abstract Duration duration();

    public AbstractSparkStreamApp() {

    }

    @Override
    public void updateSparkContext(SparkSession sparkSession,
                                   HdfsConfig hdfsConfig,
                                   KafkaConfig kafkaConfig,
                                   ZkConfig zkConfig) {
        super.updateSparkContext(sparkSession, hdfsConfig, kafkaConfig, zkConfig);
        this.javaStreamingContext = new JavaStreamingContext(this.javaSparkContext, duration());
        log.info("更新stream的context结束!!!");
    }

    protected JavaStreamingContext javaStreamingContext() {
        return this.javaStreamingContext;
    }

    abstract protected String groupId();

    abstract protected String clientId();

    protected void startAndWaitStreaming() {
        this.start();
        this.await();
        this.close();
    }

    public void start(){
        if (javaStreamingContext != null) {
            log.info("Shutting down Spark Streaming; this may take some time");
            javaStreamingContext.start();
        }
    }

    public void await(){
        Preconditions.checkState(javaStreamingContext != null);
        log.info("Spark Streaming is running");
        try {
            javaStreamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        if (javaStreamingContext != null) {
            log.info("Shutting down Spark Streaming; this may take some time");
            javaStreamingContext.stop(true, true);
            javaStreamingContext = null;
        }
    }


    protected Map<String, String> buildKafkaParams() {
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", this.kafkaConfig.getBootstrapServers());
        kafkaParams.put("group.id", groupId());
        kafkaParams.put("client.id", clientId());
        kafkaParams.put("enable.auto.commit", "false");
        kafkaParams.put("partition.assignment.strategy", "roundrobin");
        kafkaParams.put("auto.offset.reset", autoOffsetSet());
        return kafkaParams;
    }

    protected String autoOffsetSet() {
        return "smallest";
    }

    protected JavaInputDStream<String> signalTopicMonitor(String topic) {
        if (!KafkaManager.topicExists(this.zkConfig.getZkServers(), topic)) {
            throw new RuntimeException("Topic does not exist on server");
        }
        Map<TopicAndPartition, Long> seedOffsetsMap = KafkaManager.getOffsets(
                this.zkConfig.getZkServers(), this.zkConfig.getZkServers(), groupId(), topic, this.buildKafkaParams());

        JavaInputDStream<String> dStream = KafkaUtils.createDirectStream(javaStreamingContext, String.class, String.class,
                StringDecoder.class, StringDecoder.class, String.class, this.buildKafkaParams(), seedOffsetsMap,
                MessageAndMetadata::message);

        return dStream;
    }

}
