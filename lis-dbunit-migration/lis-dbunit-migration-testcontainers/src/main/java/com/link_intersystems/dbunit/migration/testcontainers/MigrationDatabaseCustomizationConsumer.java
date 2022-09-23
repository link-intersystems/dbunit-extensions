package com.link_intersystems.dbunit.migration.testcontainers;

import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
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
        databaseMigrationSupport.prepareDataSource(jdbcContainer.getDataSource());
    }

    @Override
    protected void beforeEndDataSet() throws Exception {
        databaseMigrationSupport.migrateDataSource(jdbcContainer.getDataSource());
    }
}
