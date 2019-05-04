package com.platform.data.framework.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * sql工具类
 *
 * @author xiang.sheng
 */
public class SQLHelper {
    /**
     * 将map中, { "a" : "b", "c" : "d" }
     * 转换成 a='b', c='d' 这种形式
     *
     * @param params 参数
     * @return str
     */
    public static String buildAssignSql(Map<String, Object> params) {
        List<String> assignParts = new ArrayList<>(params.keySet().size());
        String formatStr = "%s='%s'";
        params.keySet().forEach(keyName -> {
            assignParts.add(String.format(formatStr, keyName, params.get(keyName).toString()));
        });

        if (assignParts.size() == 0) {
            assignParts.add("1=1");
        }

        return String.join("and ", assignParts);
    }
}
