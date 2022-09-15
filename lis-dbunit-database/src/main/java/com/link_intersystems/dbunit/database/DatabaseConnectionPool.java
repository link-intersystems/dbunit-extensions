package com.link_intersystems.dbunit.database;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;

import java.sql.SQLException;

import static java.text.MessageFormat.format;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class DatabaseConnectionPool {

    public IDatabaseConnection borrowConnection() throws DatabaseUnitException {
        IDatabaseConnection databaseConnection = borrowTargetConnection();
        if (databaseConnection == null) {
            throw new DatabaseUnitException("Unable to return a DatabaseConnection, because none is available");
        }
        return new BorrowedDatabaseConnection(this, databaseConnection);
    }

    protected abstract IDatabaseConnection borrowTargetConnection() throws DatabaseUnitException;

    public void returnConnection(IDatabaseConnection borrowedConnection) throws DatabaseUnitException {
        if (!(borrowedConnection instanceof BorrowedDatabaseConnection)) {
            String msg = format("borrowedConnection was not borrowed by this {0}", DatabaseConnectionPool.class.getSimpleName());
            throw new IllegalArgumentException(msg);
        }

        BorrowedDatabaseConnection borrowedDatabaseConnection = (BorrowedDatabaseConnection) borrowedConnection;
        IDatabaseConnection targetConnection = borrowedDatabaseConnection.getTargetConnection();
        returnTargetConnection(targetConnection);
    }

    protected void returnTargetConnection(IDatabaseConnection targetConnection) throws DatabaseUnitException {
        try {
            targetConnection.close();
        } catch (SQLException e) {
            throw new DatabaseUnitException(e);
        }
    }
}
