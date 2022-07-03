package com.link_intersystems.dbunit.dataset.consistency;

import com.link_intersystems.dbunit.dataset.consistency.ConsistentDataSetLoader;
import com.link_intersystems.test.UnitTest;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
public class ConsistentDataSetLoaderExceptionTest {

    private ConsistentDataSetLoader dataSetLoader;
    private PreparedStatement preparedStatement;
    private Connection connection;
    private IDatabaseConnection databaseConnection;

    @BeforeEach
    void setUp() throws DatabaseUnitException, SQLException {
        preparedStatement = mock(PreparedStatement.class);
        connection = mock(Connection.class);
        databaseConnection = mock(IDatabaseConnection.class);
        when(databaseConnection.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);

        dataSetLoader = new ConsistentDataSetLoader(databaseConnection);
    }

    @Test
    void sqlExceptionOnLoad() throws DatabaseUnitException, SQLException {
        SQLException sqlException = new SQLException();

        when(databaseConnection.getConnection()).thenReturn(connection).thenThrow(sqlException);

        dataSetLoader = new ConsistentDataSetLoader(databaseConnection);

        DataSetException dataSetException = assertThrows(DataSetException.class, () -> dataSetLoader.load("SELECT * from actor where actor_id in (1)"));

        assertSame(sqlException, dataSetException.getCause());
    }

    @Test
    void sqlExceptionOnLoadPreparedStatement() throws DatabaseUnitException, SQLException {
        SQLException sqlException = new SQLException();
        when(preparedStatement.execute()).thenThrow(sqlException);

        dataSetLoader = new ConsistentDataSetLoader(databaseConnection);

        DataSetException dataSetException = assertThrows(DataSetException.class, () -> dataSetLoader.load("SELECT * from actor where actor_id in (1)"));

        assertSame(sqlException, dataSetException.getCause());
    }

    @Test
    void preparedStatementDoesNotProduceResultSet() throws DatabaseUnitException, SQLException {
        when(preparedStatement.execute()).thenReturn(false);

        dataSetLoader = new ConsistentDataSetLoader(databaseConnection);

        IDataSet load = dataSetLoader.load("update  actor set first_name = 'test' where actor_id = 1");

        assertEquals(0, load.getTableNames().length);
    }
}

