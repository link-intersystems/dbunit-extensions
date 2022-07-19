package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class FilterTableContentConsumerTest {

    @Test
    void filterTableContent() throws DataSetException, IOException {
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();

        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();
        FilterTableContentConsumer filterTableContentConsumer = new FilterTableContentConsumer(copyDataSetConsumer, metaData -> {
            if (metaData.getTableName().equals("actor")) {
                return iRowValueProvider -> {
                    try {
                        return iRowValueProvider.getColumnValue("first_name").equals("NICK");
                    } catch (DataSetException e) {
                        throw new RuntimeException(e);
                    }
                };
            }
            return null;
        });

        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(tinySakilaDataSet);
        dataSetProducerAdapter.setConsumer(filterTableContentConsumer);
        dataSetProducerAdapter.produce();

        IDataSet dataSet = copyDataSetConsumer.getDataSet();
        assertArrayEquals(new String[]{"actor", "language", "film", "film_actor"}, dataSet.getTableNames());

        ITable actorTable = dataSet.getTable("actor");
        assertEquals(1, actorTable.getRowCount());
        assertEquals("NICK", actorTable.getValue(0, "first_name"));


    }
}