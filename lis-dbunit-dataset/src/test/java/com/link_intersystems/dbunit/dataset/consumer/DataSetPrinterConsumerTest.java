package com.link_intersystems.dbunit.dataset.consumer;

import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetPrinterConsumerTest {


    @Test
    void printDataSet() throws DataSetException, IOException {
        IDataSet sakilaDataSet = TestDataSets.getTinySakilaDataSet();

        StringWriter sw = new StringWriter();
        DataSetPrinterConsumer dataSetPrinterConsumer = new DataSetPrinterConsumer(sw);
        dataSetPrinterConsumer.setLineSeparator("\n");

        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(sakilaDataSet);
        dataSetProducerAdapter.setConsumer(dataSetPrinterConsumer);

        dataSetProducerAdapter.produce();

        assertEquals(getExpected(), sw.toString());
    }

    private String getExpected() throws IOException {
        StringBuilder sb = new StringBuilder();

        InputStream inputStream = DataSetPrinterConsumerTest.class.getResourceAsStream("DataSetPrinterConsumerTest-expected.txt");
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        try (InputStreamReader inputStreamReader = new InputStreamReader(bufferedInputStream, StandardCharsets.UTF_8)) {
            int read;
            while ((read = inputStreamReader.read()) != -1) {
                sb.append((char) read);
            }
        }

        return sb.toString();
    }
}