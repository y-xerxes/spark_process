package com.platform.data.framework.utils;

import kafka.common.TopicAndPartition;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class UpdateOffsets implements VoidFunction<JavaRDD<String>> {
    private final String group;
    private final String zkOffsetManager;

    public UpdateOffsets(String group, String zkOffsetManager) {
        this.group = group;
        this.zkOffsetManager = zkOffsetManager;
    }

    /**
     * @param javaRDD RDD whose underlying RDD must be an instance of {@link HasOffsetRanges},
     *  such as {@code KafkaRDD}
     * @return null
     */

    @Override
    public void call(JavaRDD<String> javaRDD) {
        OffsetRange[] ranges = ((HasOffsetRanges) javaRDD.rdd()).offsetRanges();
        Map<TopicAndPartition,Long> newOffsets = new HashMap<>(ranges.length);
        for (OffsetRange range : ranges) {
            newOffsets.put(new TopicAndPartition(range.topic(), range.partition()),
                    range.untilOffset());
        }
        log.info("Updating offsets: {}", newOffsets);
        KafkaManager.setOffsets(zkOffsetManager, group, newOffsets);
    }
}
