package com.link_intersystems.dbunit.database;

import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.mockito.Mockito.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DatabaseConnectionDelegateTest {

    private IDatabaseConnection target;
    private DatabaseConnectionDelegate delegate;

    @BeforeEach
    void setUp() {
        target = mock(IDatabaseConnection.class);
        delegate = new DatabaseConnectionDelegate() {
            @Override
            protected IDatabaseConnection getTargetConnection() {
                return target;
            }
        };

    }

    @Test
    void getConnection() throws SQLException {
        delegate.getConnection();

        verify(target, times(1)).getConnection();
    }

    @Test
    void getSchema() {
        delegate.getSchema();

        verify(target, times(1)).getSchema();
    }

    @Test
    void close() throws SQLException {
        delegate.close();

        verify(target, times(1)).close();
    }

    @Test
    void createDataSet() throws SQLException {
        delegate.createDataSet();

        verify(target, times(1)).createDataSet();
    }

    @Test
    void createDataSetWithTables() throws DataSetException, SQLException {
        delegate.createDataSet(new String[]{"table1"});

        verify(target, times(1)).createDataSet(new String[]{"table1"});
    }

    @Test
    void createQueryTable() throws DataSetException, SQLException {
        delegate.createQueryTable("tab1", "select 1");

        verify(target, times(1)).createQueryTable("tab1", "select 1");
    }

    @Test
    void createTable() throws DataSetException, SQLException {
        delegate.createTable("tab1");

        verify(target, times(1)).createTable("tab1");
    }

    @Test
    void createTableWithPreparedStatement() throws DataSetException, SQLException {
        PreparedStatement ps = mock(PreparedStatement.class);
        delegate.createTable("tab1", ps);

        verify(target, times(1)).createTable("tab1", ps);
    }

    @Test
    void getRowCount() throws SQLException {
        delegate.getRowCount("tab1");

        verify(target, times(1)).getRowCount("tab1");
    }

    @Test
    void getRowCountWithWhere() throws SQLException {
        delegate.getRowCount("tab1", "where");

        verify(target, times(1)).getRowCount("tab1", "where");
    }

    @Test
    void getConfig() {
        delegate.getConfig();

        verify(target, times(1)).getConfig();
    }

    @Test
    void getStatementFactory() {
        delegate.getStatementFactory();

        verify(target, times(1)).getStatementFactory();
    }
}