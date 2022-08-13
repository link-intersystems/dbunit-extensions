package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.stream.consumer.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import org.dbunit.dataset.stream.IDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestContainersDataSetTransformer implements DataSetTransormer {

    private TestContainersConsumer testContainersConsumer;

    public TestContainersDataSetTransformer(DatabaseContainerSupport databaseContainerSupport, DatabaseMigrationSupport databaseMigrationSupport) {
        testContainersConsumer = new TestContainersConsumer(databaseContainerSupport);
        testContainersConsumer.setDatabaseMigrationSupport(databaseMigrationSupport);
    }

    @Override
    public IDataSetConsumer getInputConsumer() {
        return testContainersConsumer;
    }

    @Override
    public void setOutputConsumer(IDataSetConsumer dataSetConsumer) {
        testContainersConsumer.setResultConsumer(dataSetConsumer);
    }
}
