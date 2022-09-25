package com.link_intersystems.dbunit.migration.testcontainers;

import com.link_intersystems.dbunit.migration.DataSourceProperties;
import com.link_intersystems.dbunit.testcontainers.JdbcContainerProperties;

import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSourcePropertiesAdapter implements DataSourceProperties {

    private JdbcContainerProperties jdbcContainerProperties;

    public DataSourcePropertiesAdapter(JdbcContainerProperties jdbcContainerProperties) {
        this.jdbcContainerProperties = requireNonNull(jdbcContainerProperties);
    }

    @Override
    public String getUsername() {
        return jdbcContainerProperties.getUsername();
    }

    @Override
    public String getPassword() {
        return jdbcContainerProperties.getPassword();
    }

    @Override
    public String getDatabaseName() {
        return jdbcContainerProperties.getDatabaseName();
    }

    @Override
    public String getJdbcUrl() {
        return jdbcContainerProperties.getJdbcUrl();
    }

    public String getHost() {
        return jdbcContainerProperties.getHost();
    }

    @Override
    public String getPort() {
        return jdbcContainerProperties.getPort();
    }

    @Override
    public Map<String, String> getEnvironment() {
        return jdbcContainerProperties.getEnvironment();
    }
}
