package com.platform.data.framework.utils.transformations;

import com.google.common.base.CaseFormat;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * ColumnName case change
 *
 * @author xiang.sheng
 */
public class ColumnNameHelper {
    public static Dataset<Row> toSnakeCase(Dataset<Row> df) {
        return transactionColumnName(df, CaseFormat.LOWER_CAMEL, CaseFormat.LOWER_UNDERSCORE);
    }

    public static Dataset<Row> toCamelCase(Dataset<Row> df) {
        return transactionColumnName(df, CaseFormat.LOWER_UNDERSCORE, CaseFormat.LOWER_CAMEL);
    }

    public static Dataset<Row> transactionColumnName(Dataset<Row> df, CaseFormat lowerCamel, CaseFormat lowerUnderscore) {
        Dataset<Row> newDF = df;
        for (String columnName : newDF.columns()) {
            newDF = newDF.withColumnRenamed(columnName,
                    lowerCamel.to(lowerUnderscore, columnName));
        }
        return newDF;
    }
}
