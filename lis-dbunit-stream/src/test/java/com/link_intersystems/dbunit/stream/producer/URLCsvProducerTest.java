package com.link_intersystems.dbunit.stream.producer;

import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.FilterTableConsumer;
import com.link_intersystems.dbunit.stream.producer.csv.URLCsvProducer;
import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class URLCsvProducerTest {

    @Test
    void produce(@TempDir Path tmpDir) throws DataSetException, IOException {
        URL csvResourceURL = URLCsvProducerTest.class.getResource("/tiny-sakila-csv.zip");

        byte[] buff = new byte[8192];
        Path tmpZipFile = tmpDir.resolve("tiny-sakila-csv.zip");
        try (InputStream inputStream = csvResourceURL.openStream()) {
            try (OutputStream outputStream = new FileOutputStream(tmpZipFile.toFile())) {
                int read;
                while ((read = inputStream.read(buff)) > 0) {
                    outputStream.write(buff, 0, read);
                }
            }
        }

        assertNotNull(csvResourceURL);

        URLCsvProducer urlCsvProducer = new URLCsvProducer(tmpZipFile.toUri().toURL());
        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        List<String> tableNames = Arrays.asList(tinySakilaDataSet.getTableNames());
        FilterTableConsumer filterTableConsumer = new FilterTableConsumer(tableNames::contains);
        filterTableConsumer.setSubsequentConsumer(copyDataSetConsumer);
        urlCsvProducer.setConsumer(filterTableConsumer);

        urlCsvProducer.produce();

        DBUnitAssertions.STRICT.assertDataSetEquals(tinySakilaDataSet, copyDataSetConsumer.getDataSet());


    }
}