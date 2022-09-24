package com.link_intersystems.dbunit.migration;

import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSourceProperties extends Map<String, String> {

    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";
    public static final String DATABASENAME = "databaseName";
    public static final String JDBC_URL = "jdbcUrl";
    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";

    default public void setUsername(String username) {
        put(USERNAME, username);
    }

    default public String getUsername() {
        return get(USERNAME);
    }

    default public void setPassword(String password) {
        put(PASSWORD, password);
    }

    default public String getPassword() {
        return get(PASSWORD);
    }


    default public void setDatabaseName(String databaseName) {
        put(DATABASENAME, databaseName);
    }

    default public String getDatabaseName() {
        return get(DATABASENAME);
    }


    default public void setJdbcUrl(String jdbcUrl) {
        put(JDBC_URL, jdbcUrl);
    }

    default public String getJdbcUrl() {
        return get(JDBC_URL);
    }

    default public void setHostname(String hostname) {
        put(HOSTNAME, hostname);
    }

    default public String getHostname() {
        return get(HOSTNAME);
    }

    default public void setPort(String port) {
        put(PORT, port);
    }

    default public String getPort() {
        return get(PORT);
    }

    public Map<String, String> getEnvironmentProperties();

}
