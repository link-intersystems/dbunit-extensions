package com.link_intersystems.dbunit.dataset.consumer;

import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimTestDBExtension;
import com.link_intersystems.jdbc.test.db.sakila.SakilaTinyDB;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.*;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaSlimTestDBExtension.class)
class XlsDataSetConsumerTest {


    private XlsDataSetConsumer dataSetConsumer;
    private ByteArrayOutputStream bout;
    private IDataSet sakilaDataSet;

    @BeforeEach
    void setUp(Connection connection) throws DatabaseUnitException, SQLException {
        bout = new ByteArrayOutputStream();
        dataSetConsumer = new XlsDataSetConsumer(bout);

        DatabaseDataSet databaseDataSet = new DatabaseDataSet(new DatabaseConnection(connection), false);
        sakilaDataSet = new FilteredDataSet(SakilaTinyDB.getTableNames().toArray(new String[0]), databaseDataSet);
    }

    @Test
    void createExcelFile() throws DataSetException, IOException {

        DataSetProducerAdapter producerAdapter = new DataSetProducerAdapter(sakilaDataSet);
        producerAdapter.setConsumer(dataSetConsumer);
        producerAdapter.produce();



        try(FileOutputStream fileOutputStream = new FileOutputStream("target/sakila.xls")){
            fileOutputStream.write(bout.toByteArray());
        }

        assertFileEquals(bout.toByteArray(), new FileInputStream("target/sakila.xls"));

        assertFileEquals("sakila.xls", bout.toByteArray());
    }

    private void assertFileEquals(String resourceName, byte[] bytes) throws IOException {
        InputStream expectedBytes = XlsDataSetConsumerTest.class.getResourceAsStream(resourceName);
        assertFileEquals(bytes, expectedBytes);
    }

    private void assertFileEquals(byte[] bytes, InputStream expectedBytes) throws IOException {
        InputStream actualBytes = new ByteArrayInputStream(bytes);

        int bytePos = 0;

        int read = -1;
        while ((read = actualBytes.read()) != -1) {
            int expectedByte = expectedBytes.read() & 0xFF;;
            assertEquals(expectedByte, read, "pos: " + bytePos++);
        }

        assertEquals(-1, expectedBytes.read());
    }
}