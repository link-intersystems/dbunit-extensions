package com.link_intersystems.dbunit.testcontainers;

import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class JdbcDatabaseContainerProperties implements JdbcContainerProperties {

    private JdbcDatabaseContainer<?> jdbcDatabaseContainer;

    public JdbcDatabaseContainerProperties(JdbcDatabaseContainer<?> jdbcDatabaseContainer) {
        this.jdbcDatabaseContainer = requireNonNull(jdbcDatabaseContainer);
    }

    @Override
    public String getUsername() {
        return jdbcDatabaseContainer.getUsername();
    }

    @Override
    public String getPassword() {
        return jdbcDatabaseContainer.getPassword();
    }

    @Override
    public String getDatabaseName() {
        try {
            return jdbcDatabaseContainer.getDatabaseName();
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }

    @Override
    public String getJdbcUrl() {
        return jdbcDatabaseContainer.getJdbcUrl();
    }

    @Override
    public String getHost() {
        return jdbcDatabaseContainer.getHost();
    }

    @Override
    public String getPort() {
        List<Integer> exposedPorts = jdbcDatabaseContainer.getExposedPorts();
        if (!exposedPorts.isEmpty()) {
            Integer mappedPort = jdbcDatabaseContainer.getMappedPort(exposedPorts.get(0));
            return String.valueOf(mappedPort);
        }

        return null;
    }

    @Override
    public Map<String, String> getEnvironment() {
        return jdbcDatabaseContainer.getEnvMap();
    }
}
