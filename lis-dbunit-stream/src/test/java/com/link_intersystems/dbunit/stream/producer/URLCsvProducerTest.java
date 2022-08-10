package com.link_intersystems.dbunit.stream.producer;

import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.FilterTableConsumer;
import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class URLCsvProducerTest {

    @Test
    void produce() throws DataSetException, IOException {
        URL csvResourceURL = URLCsvProducerTest.class.getResource("/tiny-sakila-csv.zip");
        assertNotNull(csvResourceURL);
        assertEquals("file", csvResourceURL.getProtocol());

        URLCsvProducer urlCsvProducer = new URLCsvProducer(csvResourceURL);
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