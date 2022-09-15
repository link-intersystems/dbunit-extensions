package com.link_intersystems.dbunit.stream.producer.sql;

import com.link_intersystems.dbunit.database.DatabaseConnectionPool;
import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.producer.db.DatabaseDataSetProducerConfig;
import com.link_intersystems.dbunit.table.Row;
import com.link_intersystems.dbunit.table.TableUtil;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyDB;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyExtension;
import com.link_intersystems.sql.io.SqlScript;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@SakilaTinyExtension
class SqlScriptDataSetProducerTest {


    private DatabaseConnectionPool connectionPool;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException {
        DatabaseConnection databaseConnection = new DatabaseConnection(connection);
        connectionPool = spy(new DatabaseConnectionPool() {
            @Override
            protected IDatabaseConnection borrowTargetConnection() throws DatabaseUnitException {
                return databaseConnection;
            }
        });

    }

    @Test
    void produce() throws DataSetException {
        SqlScriptDataSetProducer sqlScriptDataSetProducer = new SqlScriptDataSetProducer(connectionPool, new SqlScript("INSERT INTO \"language\" VALUES\n" +
                "                           (999, 'French', TIMESTAMP '2006-02-15 05:02:19');"));
        DatabaseDataSetProducerConfig config = new DatabaseDataSetProducerConfig();
        config.setTableFilter(t -> SakilaTinyDB.getTableNames().contains(t));
        sqlScriptDataSetProducer.setDatabaseDataSetProducerConfig(config);

        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();
        sqlScriptDataSetProducer.setConsumer(copyDataSetConsumer);
        sqlScriptDataSetProducer.produce();

        IDataSet dataSet = copyDataSetConsumer.getDataSet();

        ITable languageTable = dataSet.getTable("language");
        TableUtil languageTableUtil = new TableUtil(languageTable);
        Row row = languageTableUtil.getRowById(999);
        assertNotNull(row);
    }

    @Test
    void produceException() throws DataSetException {
        SqlScriptDataSetProducer sqlScriptDataSetProducer = new SqlScriptDataSetProducer(connectionPool, new SqlScript("INSERT INTO \"language\" VALUES\n" +
                "                           (999, 'French', TIMESTAMP '2006-02-15 05:02:19');"));
        DatabaseDataSetProducerConfig config = new DatabaseDataSetProducerConfig();
        config.setTableFilter(t -> SakilaTinyDB.getTableNames().contains(t));
        sqlScriptDataSetProducer.setDatabaseDataSetProducerConfig(config);

        IDataSetConsumer dataSetConsumer = mock(IDataSetConsumer.class);
        DataSetException dataSetException = new DataSetException();
        doThrow(dataSetException).when(dataSetConsumer).startDataSet();
        sqlScriptDataSetProducer.setConsumer(dataSetConsumer);

        DataSetException thrownException = assertThrows(DataSetException.class, () -> sqlScriptDataSetProducer.produce());
        assertEquals(dataSetException, thrownException);
    }

    @Test
    void produceExceptionOnReturnConnection() throws DatabaseUnitException {
        SqlScriptDataSetProducer sqlScriptDataSetProducer = new SqlScriptDataSetProducer(connectionPool, new SqlScript("INSERT INTO \"language\" VALUES\n" +
                "                           (999, 'French', TIMESTAMP '2006-02-15 05:02:19');"));
        DatabaseDataSetProducerConfig config = new DatabaseDataSetProducerConfig();
        config.setTableFilter(t -> SakilaTinyDB.getTableNames().contains(t));
        sqlScriptDataSetProducer.setDatabaseDataSetProducerConfig(config);

        DatabaseUnitException databaseUnitException = new DatabaseUnitException();
        doThrow(databaseUnitException).when(connectionPool).returnConnection(any());

        DataSetException thrownException = assertThrows(DataSetException.class, () -> sqlScriptDataSetProducer.produce());
        assertEquals(databaseUnitException, thrownException.getCause());
    }
}