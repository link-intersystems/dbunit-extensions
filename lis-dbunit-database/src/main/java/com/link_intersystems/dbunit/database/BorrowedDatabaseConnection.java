package com.link_intersystems.dbunit.database;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;

import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class BorrowedDatabaseConnection extends DatabaseConnectionDelegate {

    private DatabaseConnectionBorrower databaseConnectionBorrower;
    private IDatabaseConnection targetConnection;

    public BorrowedDatabaseConnection(DatabaseConnectionBorrower databaseConnectionBorrower, IDatabaseConnection targetConnection) {
        this.databaseConnectionBorrower = databaseConnectionBorrower;
        this.targetConnection = targetConnection;
    }

    protected IDatabaseConnection getTargetConnection() {
        return targetConnection;
    }

    @Override
    public void close() throws SQLException {
        try {
            databaseConnectionBorrower.returnConnection(this);
        } catch (DatabaseUnitException e) {
            Throwable cause = e.getCause();
            if (cause instanceof SQLException) {
                SQLException sqlException = (SQLException) cause;
                throw sqlException;
            }
            throw new SQLException(e);
        }
    }
}
