package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class RepeatingDataSetConsumerTest {


    @Test
    void multipleConsumers() throws DataSetException, IOException {
        CopyDataSetConsumer copyDataSetConsumer1 = new CopyDataSetConsumer();
        CopyDataSetConsumer copyDataSetConsumer2 = new CopyDataSetConsumer();

        RepeatingDataSetConsumer repeatingDataSetConsumer = new RepeatingDataSetConsumer(copyDataSetConsumer1, copyDataSetConsumer2);

        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(tinySakilaDataSet);
        dataSetProducerAdapter.setConsumer(repeatingDataSetConsumer);
        dataSetProducerAdapter.produce();

        DBUnitAssertions.STRICT.assertDataSetEquals(tinySakilaDataSet, copyDataSetConsumer1.getDataSet());
        DBUnitAssertions.STRICT.assertDataSetEquals(tinySakilaDataSet, copyDataSetConsumer2.getDataSet());
    }
}