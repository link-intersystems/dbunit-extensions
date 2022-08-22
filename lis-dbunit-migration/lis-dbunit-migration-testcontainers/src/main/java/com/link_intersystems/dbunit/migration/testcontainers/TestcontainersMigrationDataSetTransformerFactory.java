package com.link_intersystems.dbunit.migration.testcontainers;

import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.MigrationDataSetTransformerFactory;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipeTransformerAdapter;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.consumer.TestContainersConsumer;
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
    public DataSetTransormer createTransformer(DatabaseMigrationSupport databaseMigrationSupport) {
        TestContainersConsumer testContainersConsumer = new TestContainersConsumer(runningContainerPool);
        testContainersConsumer.setStartDataSourceConsumer(databaseMigrationSupport::prepareDataSource);
        testContainersConsumer.setEndDataSourceConsumer(databaseMigrationSupport::migrateDataSource);
        return new DataSetConsumerPipeTransformerAdapter(testContainersConsumer);
    }
}
