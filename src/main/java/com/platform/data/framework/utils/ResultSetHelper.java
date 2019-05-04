package com.platform.data.framework.utils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResultSetHelper {
    public static List<Map<String, Object>> resultSetToListMap(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        List<String> columns = new ArrayList<>(metaData.getColumnCount());
        for(int i = 1; i <= metaData.getColumnCount(); i++){
            columns.add(metaData.getColumnName(i));
        }

        List<Map<String, Object>> data = new ArrayList<>();
        while(resultSet.next()){
            Map<String,Object> row = new HashMap<>(columns.size());
            for(String col : columns) {
                row.put(col, resultSet.getObject(col));
            }
            data.add(row);
        }

        return data;
    }
}
