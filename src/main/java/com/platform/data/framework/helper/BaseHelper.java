package com.platform.data.framework.helper;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

public class BaseHelper {
    protected JavaSparkContext javaSparkContext;
    protected SQLContext sqlContext;

    public BaseHelper(JavaSparkContext javaSparkContext,
                      SQLContext sqlContext) {
        this.javaSparkContext = javaSparkContext;
        this.sqlContext = sqlContext;
    }
}
