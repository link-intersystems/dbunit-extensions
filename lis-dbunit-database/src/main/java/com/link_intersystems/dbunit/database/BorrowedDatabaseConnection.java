package com.link_intersystems.dbunit.database;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;

import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class BorrowedDatabaseConnection extends DatabaseConnectionDelegate {

    private DatabaseConnectionPool connectionPool;
    private IDatabaseConnection targetConnection;

    public BorrowedDatabaseConnection(DatabaseConnectionPool connectionPool, IDatabaseConnection targetConnection) {
        this.connectionPool = connectionPool;
        this.targetConnection = targetConnection;
    }

    protected IDatabaseConnection getTargetConnection() {
        return targetConnection;
    }

    @Override
    public void close() throws SQLException {
        try {
            connectionPool.returnConnection(this);
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
