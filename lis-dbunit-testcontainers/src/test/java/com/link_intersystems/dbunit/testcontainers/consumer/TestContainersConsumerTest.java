package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.test.TestDataSets;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupportFactory;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class TestContainersConsumerTest {

    private DatabaseContainerSupport containerSupport;

    @BeforeEach
    void setUp() {
        containerSupport = DatabaseContainerSupportFactory.INSTANCE.createPostgres("postgres:latest");

    }

    @Test
    void consume() throws DataSetException, IOException {
        TestContainersConsumer testContainersConsumer = new TestContainersConsumer(containerSupport);
        testContainersConsumer.setDatabaseMigrationSupport(new SakilaMigrationSupport());

        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(tinySakilaDataSet);
        dataSetProducerAdapter.setConsumer(testContainersConsumer);
        dataSetProducerAdapter.produce();
    }

}