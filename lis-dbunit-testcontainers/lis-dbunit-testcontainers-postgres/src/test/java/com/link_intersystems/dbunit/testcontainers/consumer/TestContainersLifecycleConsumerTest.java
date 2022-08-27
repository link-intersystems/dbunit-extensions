package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.test.TestDataSets;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class TestContainersLifecycleConsumerTest {

    private DatabaseContainerSupport containerSupport;

    @BeforeEach
    void setUp() {
        containerSupport = DatabaseContainerSupport.getDatabaseContainerSupport("postgres:latest");
    }

    @Test
    void consume() throws DataSetException, IOException {
        TestContainersLifecycleConsumer testContainersConsumer = new TestContainersLifecycleConsumer(containerSupport);

        DatabaseCustomizationConsumer databaseCustomizationConsumer = new DatabaseCustomizationConsumer() {
            @Override
            protected void beforeStartDataSet(JdbcContainer jdbcContainer) throws SQLException {
                new SakilaDataSourceSetup().prepareDataSource(jdbcContainer.getDataSource());
            }
        };
        testContainersConsumer.setSubsequentConsumer(databaseCustomizationConsumer);

        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(tinySakilaDataSet);
        dataSetProducerAdapter.setConsumer(testContainersConsumer);
        dataSetProducerAdapter.produce();
    }

}