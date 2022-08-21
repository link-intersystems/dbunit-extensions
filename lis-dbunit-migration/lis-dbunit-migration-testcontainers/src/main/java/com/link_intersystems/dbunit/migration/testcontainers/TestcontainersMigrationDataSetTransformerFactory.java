package com.link_intersystems.dbunit.migration.testcontainers;

import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.MigrationDataSetTransformerFactory;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipeTransformerAdapter;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.consumer.TestContainersConsumer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestcontainersMigrationDataSetTransformerFactory implements MigrationDataSetTransformerFactory {

    private final DatabaseContainerSupport databaseContainerSupport;

    public TestcontainersMigrationDataSetTransformerFactory(String dockerImageName) {
        this(DatabaseContainerSupport.getDatabaseContainerSupport(dockerImageName));
    }

    public TestcontainersMigrationDataSetTransformerFactory(DatabaseContainerSupport databaseContainerSupport) {
        this.databaseContainerSupport = requireNonNull(databaseContainerSupport);
    }

    @Override
    public DataSetTransormer createTransformer(DatabaseMigrationSupport databaseMigrationSupport) {
        TestContainersConsumer testContainersConsumer = new TestContainersConsumer(databaseContainerSupport);
        testContainersConsumer.setStartDataSourceConsumer(databaseMigrationSupport::prepareDataSource);
        testContainersConsumer.setEndDataSourceConsumer(databaseMigrationSupport::migrateDataSource);
        return new DataSetConsumerPipeTransformerAdapter(testContainersConsumer);
    }
}
