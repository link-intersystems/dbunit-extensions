package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;

import javax.sql.DataSource;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DatabaseMigrationSupport {
    void prepareDataSource(DataSource dataSource) throws DataSetException;

    void migrateDataSource(DataSource dataSource) throws DataSetException;

}
