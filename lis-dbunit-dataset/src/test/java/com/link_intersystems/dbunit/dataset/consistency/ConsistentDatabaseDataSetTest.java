package com.link_intersystems.dbunit.dataset.consistency;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.ITable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class ConsistentDatabaseDataSetTest {


    private PreparedStatement preparedStatement;
    private Connection connection;
    private IDatabaseConnection databaseConnection;

    @BeforeEach
    void setUp() throws SQLException {
        preparedStatement = mock(PreparedStatement.class);
        connection = mock(Connection.class);
        databaseConnection = mock(IDatabaseConnection.class);
        when(databaseConnection.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);
    }

    @Test
    void exceptionOnConstructWithTables() throws SQLException {
        SQLException sqlException = new SQLException();
        when(databaseConnection.getConnection()).thenThrow(sqlException);
        ITable table = mock(ITable.class);

        DataSetException dataSetException = assertThrows(DataSetException.class, () -> new ConsistentDatabaseDataSet(databaseConnection, table));

        assertSame(sqlException, dataSetException.getCause());
    }

    @Test
    void exceptionOnConstruct() throws SQLException {
        SQLException sqlException = new SQLException();
        when(databaseConnection.getConnection()).thenThrow(sqlException);

        DataSetException dataSetException = assertThrows(DataSetException.class, () -> new ConsistentDatabaseDataSet(databaseConnection, new DefaultDataSet()));

        assertSame(sqlException, dataSetException.getCause());
    }
}