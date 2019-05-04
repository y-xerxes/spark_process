package com.platform.data.framework.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class DatabaseConfig implements Serializable {
    private String host;
    private Integer port;
    private String username;
    private String password;
    private String name;
    private String mode = "all";

    public String buildJdbcUrl(String dbName) {
        return String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf-8&useSSL=false&serverTimezone=Hongkong",
                this.getHost(), this.getPort().toString(), dbName);
    }
}
