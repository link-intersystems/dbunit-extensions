package com.link_intersystems.dbunit.commands.flyway;

import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;

import javax.sql.DataSource;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DatabaseMigrationSupport {
    void startDataSet(DataSource dataSource) throws DataSetException;

    void endDataSet(DataSource dataSource) throws DataSetException;

    default IDataSet decorateResultDataSet(IDatabaseConnection databaseConnection, IDataSet resultDataSet) throws DataSetException {
        return resultDataSet;
    }
}
