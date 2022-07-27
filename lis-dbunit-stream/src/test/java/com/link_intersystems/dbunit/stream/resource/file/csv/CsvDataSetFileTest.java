package com.link_intersystems.dbunit.stream.resource.file.csv;

import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.io.FilePath;
import com.link_intersystems.io.Unzip;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class CsvDataSetFileTest {

    private Path tmpDir;
    private CsvDataSetFile csvDataSetFile;

    @BeforeEach
    void setUp(@TempDir Path tmpDir) throws IOException {
        this.tmpDir = tmpDir;
        Unzip.unzip(CsvDataSetFileTest.class.getResourceAsStream("/tiny-sakila-dataset-files.zip"), tmpDir);

        csvDataSetFile = new CsvDataSetFile(new FilePath(tmpDir, Paths.get("tiny-sakila-csv")));
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