package com.link_intersystems.dbunit.testcontainers;

import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface JdbcContainerProperties {

    public String getUsername();

    public String getPassword();

    public String getDatabaseName();

    public String getJdbcUrl();

    public String getHost();

    public String getPort();

    public Map<String, String> getEnvironment();
}
