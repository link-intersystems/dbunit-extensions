package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.stream.consumer.db.DatabaseDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.support.DataSetConsumerSupport;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.operation.DatabaseOperation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetConsumerSupportTest implements DataSetConsumerSupport {

    private IDataSetConsumer dataSetConsumer;

    @AfterEach
    void clear() {
        this.dataSetConsumer = null;
    }

    @Override
    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        this.dataSetConsumer = dataSetConsumer;
    }


    @Test
    void setDatabaseConsumer() {
        IDataSetConsumer dataSetConsumer = mock(IDataSetConsumer.class);

        setDataSetConsumer(dataSetConsumer);

        assertSame(dataSetConsumer, this.dataSetConsumer);
    }

    @Test
    void testSetDatabaseConsumer() throws DatabaseUnitException {
        setDatabaseConsumer(mock(Connection.class));

        assertNotNull(dataSetConsumer);
    }

    @Test
    void setDatabaseConsumerWithDatabaseOperation() throws DatabaseUnitException {
        Connection connection = mock(Connection.class);

        setDatabaseConsumer(connection, DatabaseOperation.REFRESH);

        assertNotNull(dataSetConsumer);

        DatabaseDataSetConsumer databaseDataSetConsumer = (DatabaseDataSetConsumer) dataSetConsumer;
        assertEquals(DatabaseOperation.REFRESH, databaseDataSetConsumer.getDatabaseOperation());
    }

    @Test
    void testSetDatabaseConsumerWithOperation() {
        setDatabaseConsumer(mock(IDatabaseConnection.class), DatabaseOperation.INSERT);

        assertNotNull(dataSetConsumer);
    }

    @Test
    void testSetCsvConsumer() {
        setCsvConsumer("target/csv");

        assertNotNull(dataSetConsumer);
    }

    @Test
    void setXlsConsumer() throws IOException {
        setXlsConsumer("target/out.xls");

        assertNotNull(dataSetConsumer);
    }


    @Test
    void setXmlConsumer() throws IOException {
        setXmlConsumer("target/out.xml");

        assertNotNull(dataSetConsumer);
    }

    @Test
    void setFlatXmlConsumer() throws IOException {
        setFlatXmlConsumer("target/out.xml");

        assertNotNull(dataSetConsumer);
    }

    @Test
    void setDataSetConsumer() {
        IDataSetConsumer dataSetConsumer = mock(IDataSetConsumer.class);
        setDataSetConsumer(dataSetConsumer);

        assertSame(dataSetConsumer, this.dataSetConsumer);
    }
}