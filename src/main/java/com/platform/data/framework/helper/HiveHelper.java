package com.platform.data.framework.helper;

import com.platform.data.framework.utils.SQLHelper;
import com.platform.data.framework.utils.transformations.ColumnNameHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import scala.collection.JavaConverters;

import java.util.*;

/**
 * 存储DF或者Dataset到hive, 自动做数据库名或者表名的namespace
 *
 * @author xiang.sheng
 */
@Slf4j
public class HiveHelper extends BaseHelper {
    public static final Set<String> dbNames = new HashSet<>();
    public HiveHelper(JavaSparkContext javaSparkContext, SQLContext sqlContext) {
        super(javaSparkContext, sqlContext);
    }

    public <T> void save(String dbName, String tableName, List<String> partitionKeys, Dataset<T> data) {
        withDbName(dbName, true, fullDBName -> {
            String fullTableName = fullDBName + "." + tableName;
            List<Column> columns = new ArrayList<>(partitionKeys.size());
            final Dataset<Row> df = ColumnNameHelper.toSnakeCase(data.toDF());
            df.count();
            partitionKeys.forEach(keyName -> columns.add(df.col(keyName)));
            Dataset<Row> savedDf = df.repartition(JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq());
            savedDf.count();
            savedDf.write().format("parquet")
                    .partitionBy(partitionKeys.toArray(new String[] {}))
                    .mode(SaveMode.Append)
                    .saveAsTable(fullTableName);
        });
    }

    public <T> Dataset<T> load(String sql, Class<T> type) {
        return ColumnNameHelper.toCamelCase(load(sql)).as(Encoders.bean(type));
    }

    public Dataset<Row> load(String sql) {
        return this.sqlContext.sql(sql);
    }

    public void clearPartitions(String dbName, String tableName, Map<String, Object> partitionKeys) {
        withDbName(dbName, false, fullDBName -> {
            long tableCount = this.sqlContext.sql("show tables in " + fullDBName)
                    .javaRDD()
                    .map(row -> row.<String>getAs("tableName"))
                    .filter(dbTableName -> dbTableName.equalsIgnoreCase(tableName)).count();
            if (tableCount > 0) {
                String alterSql = String.format("alter table `%s`.`%s`  DROP IF EXISTS PARTITION (%s)",
                        fullDBName, tableName, SQLHelper.buildAssignSql(partitionKeys));
                log.info("clear by: {}", alterSql);
                this.sqlContext.sql(alterSql);
            } else {
                log.info("清空分区的时候, 表不存在, 表名: {}.{}", fullDBName, tableName);
            }
        });
    }



    @FunctionalInterface
    private interface WithDBInterface {
        /**
         * db存在的时候, 回调
         *
         * @param fullDBName dbName
         */
        void fullDbName(String fullDBName);
    }

    private void withDbName(String dbName, boolean autoCreate, WithDBInterface withDBInterface) {
        String fullDbName = dbName;
        if (!isDatabaseExists(fullDbName)) {

            if (autoCreate) {
                this.sqlContext.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", fullDbName));
                log.info("数据库 {} 不存在, 但是自动创建了", fullDbName);
                withDBInterface.fullDbName(fullDbName);
            } else {
                log.info("数据库 {} 不存在", fullDbName);
            }
        } else {
            log.info("数据库 {} 存在", fullDbName);
            withDBInterface.fullDbName(fullDbName);
        }
    }

    private boolean isDatabaseExists(String dbName) {
        synchronized (dbNames) {
            if (dbNames.contains(dbName)) {
                return true;
            }
        }
        boolean isDbExistsFlag = this.sqlContext.sql("show databases")
                .javaRDD()
                .map(row -> row.<String>getAs("databaseName"))
                .filter(databaseName -> databaseName.equalsIgnoreCase(dbName))
                .count() > 0;

        if (isDbExistsFlag) {
            synchronized (dbNames) {
                dbNames.add(dbName);
            }
        }

        return isDbExistsFlag;
    }
}
