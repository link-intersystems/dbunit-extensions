package com.link_intersystems.dbunit.table;

import org.dbunit.database.DatabaseSequenceFilterAccess;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseTableOrder implements TableOrder {

    private IDatabaseConnection databaseConnection;

    public DatabaseTableOrder(IDatabaseConnection databaseConnection) {
        this.databaseConnection = requireNonNull(databaseConnection);
    }

    @Override
    public String[] orderTables(String... tableNames) throws DataSetException {
        return DatabaseSequenceFilterAccess.sortTableNames(databaseConnection, tableNames);
    }
}
