package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class NullDatabaseMigrationSupport implements DatabaseMigrationSupport {

    public static final NullDatabaseMigrationSupport INSTANCE = new NullDatabaseMigrationSupport();

    @Override
    public void prepareDataSource(DataSource dataSource) throws DataSetException {
    }

    @Override
    public void migrateDataSource(DataSource dataSource) throws DataSetException {
    }
}
