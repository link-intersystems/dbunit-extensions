package com.link_intersystems.dbunit.database;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DatabaseConnectionPoolTest {

    private DatabaseConnectionPool databaseConnectionPool;
    private IDatabaseConnection databaseConnection;

    @BeforeEach
    void setUp() {
        databaseConnection = mock(IDatabaseConnection.class);

        databaseConnectionPool = spy(new DatabaseConnectionPool() {

            @Override
            protected IDatabaseConnection borrowTargetConnection() throws DatabaseUnitException {
                return databaseConnection;
            }
        });
    }

    @Test
    void borrowConnection() throws DatabaseUnitException, SQLException {
        IDatabaseConnection borrowedConnection = databaseConnectionPool.borrowConnection();
        assertNotNull(borrowedConnection);
        assertNotSame(databaseConnection, borrowedConnection);

        borrowedConnection.getConnection();
        verify(databaseConnection, times(1)).getConnection();

        borrowedConnection.close();
        verify(databaseConnectionPool, times(1)).returnConnection(borrowedConnection);
    }

    @Test
    void returnUnknownConnection() throws DatabaseUnitException, SQLException {
        assertThrows(IllegalArgumentException.class, () -> databaseConnectionPool.returnConnection(mock(IDatabaseConnection.class)));
    }

    @Test
    void returnConnectionCausesException() throws DatabaseUnitException, SQLException {
        IDatabaseConnection borrowedConnection = databaseConnectionPool.borrowConnection();
        doThrow(new SQLException()).when(databaseConnection).close();
        assertThrows(SQLException.class, () -> borrowedConnection.close());
    }

    @Test
    void noConnectionAvailable() throws DatabaseUnitException, SQLException {
        databaseConnectionPool = new DatabaseConnectionPool() {

            @Override
            protected IDatabaseConnection borrowTargetConnection() throws DatabaseUnitException {
                return null;
            }
        };

        assertThrows(DatabaseUnitException.class, () -> databaseConnectionPool.borrowConnection());
    }
}