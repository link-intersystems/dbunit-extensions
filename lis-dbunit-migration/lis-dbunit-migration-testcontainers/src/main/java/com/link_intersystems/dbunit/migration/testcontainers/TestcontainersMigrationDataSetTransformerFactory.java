package com.link_intersystems.dbunit.migration.testcontainers;

import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.MigrationDataSetTransformerFactory;
import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipe;
import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseCustomizationConsumer;
import com.link_intersystems.dbunit.testcontainers.consumer.TestContainersLifecycleConsumer;
import com.link_intersystems.dbunit.testcontainers.consumer.TestContainersMigrationConsumer;
import com.link_intersystems.dbunit.testcontainers.pool.RunningContainerPool;
import com.link_intersystems.dbunit.testcontainers.pool.SingleRunningContainerPool;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestcontainersMigrationDataSetTransformerFactory implements MigrationDataSetTransformerFactory {

    private RunningContainerPool runningContainerPool;

    public TestcontainersMigrationDataSetTransformerFactory(String dockerImageName) {
        this(DatabaseContainerSupport.getDatabaseContainerSupport(dockerImageName));
    }

    public TestcontainersMigrationDataSetTransformerFactory(DatabaseContainerSupport databaseContainerSupport) {
        this(new SingleRunningContainerPool(() -> new DBunitJdbcContainer(databaseContainerSupport.create(), databaseContainerSupport.getDatabaseConfig())));
    }

    public TestcontainersMigrationDataSetTransformerFactory(RunningContainerPool runningContainerPool) {
        this.runningContainerPool = requireNonNull(runningContainerPool);
    }

    @Override
    public ChainableDataSetConsumer createTransformer(DatabaseMigrationSupport databaseMigrationSupport) {
        TestContainersLifecycleConsumer testContainersConsumer = new TestContainersLifecycleConsumer(runningContainerPool);


        DatabaseCustomizationConsumer databaseCustomizationConsumer = new DatabaseCustomizationConsumer();
        databaseCustomizationConsumer.setCustomizeDatabaseOnStartDataSet(databaseMigrationSupport::prepareDataSource);
        databaseCustomizationConsumer.setCustomizeDatabaseOnEndDataSet(databaseMigrationSupport::migrateDataSource);


        TestContainersMigrationConsumer testContainersMigrationConsumer = new TestContainersMigrationConsumer();

        DataSetConsumerPipe dataSetConsumerPipe = new DataSetConsumerPipe();
        dataSetConsumerPipe.add(testContainersConsumer);
        dataSetConsumerPipe.add(databaseCustomizationConsumer);
        dataSetConsumerPipe.add(testContainersMigrationConsumer);

        return dataSetConsumerPipe;
    }
}
