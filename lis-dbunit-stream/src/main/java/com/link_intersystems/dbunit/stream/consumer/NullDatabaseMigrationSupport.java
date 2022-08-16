package com.link_intersystems.dbunit.stream.consumer;

import javax.sql.DataSource;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class NullDatabaseMigrationSupport implements DatabaseMigrationSupport {

    public static final NullDatabaseMigrationSupport INSTANCE = new NullDatabaseMigrationSupport();

    @Override
    public void prepareDataSource(DataSource dataSource) {
    }

    @Override
    public void migrateDataSource(DataSource dataSource) {
    }
}
