package com.platform.data.framework.helper;

import com.platform.data.framework.config.DatabaseConfig;
import com.platform.data.framework.config.DatabaseConfigGroup;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DatabaseConfigManager {

    private Map<String, DatabaseConfig> groupedDatabaseConfigs;
    private DatabaseConfigGroup databaseConfigGroup;

    public DatabaseConfigManager(DatabaseConfigGroup databaseConfigGroup) {
        this.databaseConfigGroup = databaseConfigGroup;
        buildGroupdConfigs();
    }

    private void buildGroupdConfigs() {
        groupedDatabaseConfigs = new HashMap<>(this.databaseConfigGroup.getDatabaseConfigs().size());
        this.databaseConfigGroup.getDatabaseConfigs().forEach(databaseConfig -> {
            groupedDatabaseConfigs.put(databaseConfig.getName(), databaseConfig);
        });
    }

    public DatabaseConfig selectDatabaseConfig(String tag) {
        return this.groupedDatabaseConfigs
                .get(tag);
    }
}