package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DataSetTransformer;
import com.link_intersystems.dbunit.stream.consumer.DatabaseMigrationSupport;
import org.dbunit.dataset.stream.IDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestContainersDataSetTransformer implements DataSetTransformer {

    private TestContainersConsumer testContainersConsumer;

    public TestContainersDataSetTransformer(JdbcDatabaseContainerLifecycle containerLifecycle) {
        testContainersConsumer = new TestContainersConsumer(containerLifecycle);
    }

    @Override
    public IDataSetConsumer getInputConsumer() {
        return testContainersConsumer;
    }

    public void setDatabaseContainerHandler(DatabaseMigrationSupport databaseContainerHandler) {
        testContainersConsumer.setDatabaseMigrationSupport(databaseContainerHandler);
    }

    @Override
    public void setOutputConsumer(IDataSetConsumer dataSetConsumer) {
        testContainersConsumer.setResultConsumer(dataSetConsumer);
    }
}
