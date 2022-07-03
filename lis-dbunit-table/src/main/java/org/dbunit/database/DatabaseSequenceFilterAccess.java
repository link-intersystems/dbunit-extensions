package org.dbunit.database;

import org.dbunit.dataset.DataSetException;

import java.sql.SQLException;

/**
 * A way to make the package local {@link DatabaseSequenceFilter#sortTableNames(IDatabaseConnection, String[])} method
 * available.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseSequenceFilterAccess {

    public static String[] sortTableNames(IDatabaseConnection connection, String[] tableNames) throws DataSetException {
        try {
            return DatabaseSequenceFilter.sortTableNames(connection, tableNames);
        } catch (SQLException e) {
            // Sadly the DatabaseSequenceFilter declares that it throws a SQLException, but it doesn't.
            throw new DataSetException(e);
        }
    }
}
