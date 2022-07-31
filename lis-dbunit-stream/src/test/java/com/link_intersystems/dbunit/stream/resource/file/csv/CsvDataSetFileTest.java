package com.link_intersystems.dbunit.stream.resource.file.csv;

import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.test.TinySakilaDataSetFiles;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class CsvDataSetFileTest {

    private CsvDataSetFile csvDataSetFile;

    @BeforeEach
    void setUp(@TempDir Path tmpDir) {
        TinySakilaDataSetFiles dataSetFiles = TinySakilaDataSetFiles.create(tmpDir);
        csvDataSetFile =  new CsvDataSetFile(dataSetFiles.getCsvDataSetDir().toFile());
    }

    @Test
    void createProducer() throws DataSetException {
        IDataSetProducer dataSetProducer = csvDataSetFile.createProducer();

        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();
        dataSetProducer.setConsumer(copyDataSetConsumer);

        dataSetProducer.produce();

        IDataSet dataSet = copyDataSetConsumer.getDataSet();
        ITable actor = dataSet.getTable("actor");
        Assertions.assertEquals(2, actor.getRowCount());
    }

    @Test
    void createConsumer() throws DataSetException {
        IDataSetConsumer consumer = csvDataSetFile.createConsumer();

        Assertions.assertNotNull(consumer);
    }
}