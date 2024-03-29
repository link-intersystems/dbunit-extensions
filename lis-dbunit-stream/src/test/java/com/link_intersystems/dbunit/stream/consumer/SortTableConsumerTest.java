package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class SortTableConsumerTest {


    @Test
    void sortTables() throws DataSetException, IOException {
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();

        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(tinySakilaDataSet);

        CopyDataSetConsumer subsequentConsumer = new CopyDataSetConsumer();
        ExternalSortTableConsumer sortTableConsumer = new ExternalSortTableConsumer(tableNames -> new String[]{"language", "film", "actor", "film_actor"});
        sortTableConsumer.setSubsequentConsumer(subsequentConsumer);

        dataSetProducerAdapter.setConsumer(sortTableConsumer);

        dataSetProducerAdapter.produce();

        IDataSet dataSet = subsequentConsumer.getDataSet();
        assertNotNull(dataSet);

        assertArrayEquals(new String[]{"language", "film", "actor", "film_actor"}, dataSet.getTableNames());

    }

}