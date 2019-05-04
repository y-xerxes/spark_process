package com.platform.data.framework.helper;

import com.platform.data.framework.config.HdfsConfig;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * 从HDFS中载入文件
 *
 * @author xiang.sheng
 */
@Slf4j
public class HdfsHelper extends BaseHelper {
    private HdfsConfig hdfsConfig;

    public HdfsHelper(JavaSparkContext javaSparkContext,
                      SQLContext sqlContext,
                      HdfsConfig hdfsConfig) {
        super(javaSparkContext, sqlContext);

        this.hdfsConfig = hdfsConfig;
    }

    public JavaRDD<String> load(HdfsLoaderType hdfsLoaderType) {
        if (hdfsLoaderType.minPartitions == 0) {
            return this.javaSparkContext.textFile(hdfsLoaderType.buildFullPath(hdfsConfig));
        } else {
            return this.javaSparkContext.textFile(hdfsLoaderType.buildFullPath(hdfsConfig),
                    hdfsLoaderType.minPartitions);
        }
    }

    @Builder
    public static class HdfsLoaderType {
        String path;
        Integer minPartitions = 0;

        protected String buildFullPath(HdfsConfig hdfsConfig) {
            return "hdfs://" + hdfsConfig.getNameNodeName() + path;
        }
    }
}
