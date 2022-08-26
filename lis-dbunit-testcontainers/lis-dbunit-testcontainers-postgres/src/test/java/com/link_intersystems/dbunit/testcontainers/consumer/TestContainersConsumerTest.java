package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.test.TestDataSets;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class TestContainersConsumerTest {

    private DatabaseContainerSupport containerSupport;

    @BeforeEach
    void setUp() {
        containerSupport = DatabaseContainerSupport.getDatabaseContainerSupport("postgres:latest");
    }

    @Test
    void consume() throws DataSetException, IOException {
        TestContainersConsumer testContainersConsumer = new TestContainersConsumer(containerSupport);
        testContainersConsumer.setSubsequentConsumer(new DefaultContainerAwareDataSetConsumer() {
            @Override
            public void startDataSet(JdbcContainer jdbcContainer) throws DataSetException {
                DataSource dataSource = jdbcContainer.getDataSource();
                try {
                    new SakilaDataSourceSetup().prepareDataSource(dataSource);
                } catch (SQLException e) {
                    throw new DataSetException(e);
                }

            }
        });

        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(tinySakilaDataSet);
        dataSetProducerAdapter.setConsumer(testContainersConsumer);
        dataSetProducerAdapter.produce();
    }

}