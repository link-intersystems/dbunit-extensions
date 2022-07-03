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
        String expected = getExpected();
        String lineSeparator = determineLineSeparator(expected);

        IDataSet sakilaDataSet = TestDataSets.getTinySakilaDataSet();

        StringWriter sw = new StringWriter();
        DataSetPrinterConsumer dataSetPrinterConsumer = new DataSetPrinterConsumer(sw);
        dataSetPrinterConsumer.setLineSeparator(lineSeparator);

        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(sakilaDataSet);
        dataSetProducerAdapter.setConsumer(dataSetPrinterConsumer);

        dataSetProducerAdapter.produce();

        assertEquals(expected, sw.toString());
    }

    private String determineLineSeparator(String text) throws IOException {
        try (Reader reader = new BufferedReader(new StringReader(text))) {
            int c;

            while ((c = reader.read()) != -1) {
                if (c == '\r') {
                    c = reader.read();
                    if (c == '\n') {
                        return "\r\n";
                    } else {
                        return "\r";
                    }
                } else if (c == '\n') {
                    return "\n";
                }
            }
        }

        return System.lineSeparator();
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