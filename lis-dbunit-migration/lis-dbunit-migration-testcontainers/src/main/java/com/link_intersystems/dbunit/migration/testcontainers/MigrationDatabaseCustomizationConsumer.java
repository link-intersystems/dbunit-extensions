package com.link_intersystems.dbunit.migration.testcontainers;

import com.link_intersystems.dbunit.migration.DataSourceProperties;
import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.DefaultDataSourceProperties;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import com.link_intersystems.dbunit.testcontainers.JdbcContainerProperties;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseCustomizationConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class MigrationDatabaseCustomizationConsumer extends DatabaseCustomizationConsumer {
    private DatabaseMigrationSupport databaseMigrationSupport;

    private JdbcContainer jdbcContainer;

    public MigrationDatabaseCustomizationConsumer(DatabaseMigrationSupport databaseMigrationSupport) {
        this.databaseMigrationSupport = databaseMigrationSupport;
    }

    @Override
    protected void beforeStartDataSet(JdbcContainer jdbcContainer) throws Exception {
        this.jdbcContainer = jdbcContainer;
        JdbcContainerProperties properties = jdbcContainer.getProperties();
        databaseMigrationSupport.prepareDataSource(jdbcContainer.getDataSource(), toDataSourceProperties(properties));
    }

    @Override
    protected void beforeEndDataSet() throws Exception {
        JdbcContainerProperties properties = jdbcContainer.getProperties();
        databaseMigrationSupport.migrateDataSource(jdbcContainer.getDataSource(), toDataSourceProperties(properties));
    }

    protected DataSourceProperties toDataSourceProperties(JdbcContainerProperties properties) {
        DefaultDataSourceProperties dataSourceProperties = new DefaultDataSourceProperties();
        dataSourceProperties.setDatabaseName(properties.getDatabaseName());
        dataSourceProperties.setUsername(properties.getUsername());
        dataSourceProperties.setPassword(properties.getPassword());
        dataSourceProperties.setHostname(properties.getHostname());
        dataSourceProperties.setPort(properties.getPort());
        dataSourceProperties.setJdbcUrl(properties.getJdbcUrl());
        dataSourceProperties.setEnvironmentProperties(properties.getEnvironment());
        return dataSourceProperties;
    }
}
