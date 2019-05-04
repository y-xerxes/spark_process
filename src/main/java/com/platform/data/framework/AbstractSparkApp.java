package com.platform.data.framework;

import com.platform.data.framework.config.HdfsConfig;
import com.platform.data.framework.config.KafkaConfig;
import com.platform.data.framework.config.ZkConfig;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.List;

public abstract class AbstractSparkApp implements Serializable {

    public abstract String appName();

    protected transient SparkSession sparkSession;
    protected transient JavaSparkContext javaSparkContext;
    protected transient SparkContext sparkContext;
    protected transient SQLContext sqlContext;
    protected transient HdfsConfig hdfsConfig;
    protected transient ZkConfig zkConfig;
    protected transient KafkaConfig kafkaConfig;


    public AbstractSparkApp() {

    }

    public void updateSparkContext(SparkSession sparkSession,
                                   HdfsConfig hdfsConfig,
                                   KafkaConfig kafkaConfig,
                                   ZkConfig zkConfig) {
        this.sparkSession = sparkSession;
        this.sparkContext = sparkSession.sparkContext();
        this.javaSparkContext = new JavaSparkContext(this.sparkContext);
        this.sqlContext = sparkSession.sqlContext();
        this.hdfsConfig = hdfsConfig;
        this.zkConfig = zkConfig;

    }

    protected SparkContext sparkContext() {
        return this.sparkContext;
    }

    protected JavaSparkContext javaSparkContext() {
        return this.javaSparkContext;
    }

    protected SQLContext sqlContext() {
        return this.sqlContext;
    }

    public abstract void run(List<String> args);
}
