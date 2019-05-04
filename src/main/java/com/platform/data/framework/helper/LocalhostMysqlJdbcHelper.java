package com.platform.data.framework.helper;

public class LocalhostMysqlJdbcHelper extends AbstractMysqlJdbcHelper {
    public LocalhostMysqlJdbcHelper(DatabaseConfigManager databaseConfigManager) {
        super(databaseConfigManager);
    }

    @Override
    String tag() {
        return "localhost";
    }
}
