package com.platform.data.framework.helper;

import com.platform.data.framework.config.DatabaseConfig;
import com.platform.data.framework.utils.ResultSetHelper;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Slf4j
public abstract class AbstractMysqlJdbcHelper {
    @FunctionalInterface
    public interface PreparedStatementCallback {
        /**
         * 提供给上层一个注入参数的机会
         *
         * @param preparedStatement statement
         * @throws SQLException sqlException
         */
        void apply(PreparedStatement preparedStatement) throws SQLException;
    }

    private final DatabaseConfigManager databaseConfigManager;

    public AbstractMysqlJdbcHelper(DatabaseConfigManager databaseConfigManager) {
        this.databaseConfigManager = databaseConfigManager;
    }

    public void executeSQL(String dbName, String sql) {
        DatabaseConfig databaseConfig = this.databaseConfigManager.selectDatabaseConfig(tag());
        this.executeSQL(databaseConfig, dbName, sql);
    }

    public void executeSQL(DatabaseConfig databaseConfig, String dbName, String sql) {
        String jdbcURL = databaseConfig.buildJdbcUrl(dbName);
        try {
            @Cleanup Connection connection =
                    DriverManager.getConnection(jdbcURL, databaseConfig.getUsername(), databaseConfig.getPassword());
            @Cleanup Statement statement = connection.createStatement();
            log.info("MYSQL JDBC, jdbc: {}, 执行的SQL: {}", jdbcURL, sql);
            statement.execute(sql);
        } catch (SQLException e) {
            log.error("打开数据库链接失败", e);
        }
    }

    @Nullable
    public List<Map<String, Object>> executeSQLWithStatement(String dbName, String sql,
                                                             PreparedStatementCallback preparedStatementCallback) {
        DatabaseConfig databaseConfig = this.databaseConfigManager.selectDatabaseConfig(tag());
        return executeSQLWithStatement(databaseConfig, dbName, sql, preparedStatementCallback);
    }

    @Nullable
    public List<Map<String, Object>> executeSQLWithStatement(
            DatabaseConfig databaseConfig, String dbName, String sql,
            PreparedStatementCallback preparedStatementCallback) {
        String jdbcURL = databaseConfig.buildJdbcUrl(dbName);
        try {
            @Cleanup Connection connection =
                    DriverManager.getConnection(jdbcURL, databaseConfig.getUsername(), databaseConfig.getPassword());
            @Cleanup PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatementCallback.apply(preparedStatement);
            log.info("MYSQL JDBC, jdbc: {}, 执行的SQL: {}", jdbcURL, sql);
            preparedStatement.execute();

            if (preparedStatement.getResultSet() == null) {
                return Collections.emptyList();
            } else {
                return ResultSetHelper.resultSetToListMap(preparedStatement.getResultSet());
            }
        } catch (SQLException e) {
            log.error("打开数据库链接失败", e);
            return null;
        }
    }

    abstract String tag();

}