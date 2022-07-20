package com.link_intersystems.dbunit.commands.flyway;

import com.link_intersystems.dbunit.commands.DataSetTransformer;
import org.dbunit.dataset.stream.IDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestContainersDataSetTransformer implements DataSetTransformer {

    private TestContainersConsumer testContainersConsumer = new TestContainersConsumer();

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
