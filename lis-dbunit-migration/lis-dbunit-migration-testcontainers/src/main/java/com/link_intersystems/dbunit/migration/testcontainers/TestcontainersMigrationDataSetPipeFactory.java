package com.link_intersystems.dbunit.migration.testcontainers;

import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.MigrationDataSetPipeFactory;
import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipe;
import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.consumer.ReproduceConsumerAdapter;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseOperationConsumer;
import com.link_intersystems.dbunit.testcontainers.consumer.TestContainersLifecycleConsumer;
import com.link_intersystems.dbunit.testcontainers.pool.RunningContainerPool;
import com.link_intersystems.dbunit.testcontainers.pool.SingleRunningContainerPool;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestcontainersMigrationDataSetPipeFactory implements MigrationDataSetPipeFactory {

    private RunningContainerPool runningContainerPool;

    private MigrationPipeCustomizationFactory migrationPipeCustomizationFactory;

    public TestcontainersMigrationDataSetPipeFactory(String dockerImageName) {
        this(DatabaseContainerSupport.getDatabaseContainerSupport(dockerImageName));
    }

    public TestcontainersMigrationDataSetPipeFactory(DatabaseContainerSupport databaseContainerSupport) {
        this(new SingleRunningContainerPool(new DBunitJdbcContainer(databaseContainerSupport.create(), databaseContainerSupport.getDatabaseConfig())));
    }

    public TestcontainersMigrationDataSetPipeFactory(RunningContainerPool runningContainerPool) {
        this.runningContainerPool = requireNonNull(runningContainerPool);
        setMigrationPipeCustomizationFactory(() -> new SkipExistingDatabaseEntitiesMigrationPipeCustomization());
    }

    public void setMigrationPipeCustomizationFactory(MigrationPipeCustomizationFactory migrationPipeCustomizationFactory) {
        this.migrationPipeCustomizationFactory = requireNonNull(migrationPipeCustomizationFactory);
    }

    @Override
    public DataSetConsumerPipe createMigrationPipe(DatabaseMigrationSupport databaseMigrationSupport) {
        TestContainersLifecycleConsumer testContainersConsumer = new TestContainersLifecycleConsumer(runningContainerPool);


        MigrationDatabaseCustomizationConsumer databaseCustomizationConsumer = new MigrationDatabaseCustomizationConsumer(databaseMigrationSupport);

        DatabaseOperationConsumer testContainersMigrationConsumer = new DatabaseOperationConsumer();


        DataSetConsumerPipe dataSetConsumerPipe = new DataSetConsumerPipe();
        dataSetConsumerPipe.add(testContainersConsumer);
        dataSetConsumerPipe.add(databaseCustomizationConsumer);
        dataSetConsumerPipe.add(testContainersMigrationConsumer);

        addDatabaseDataSetConsumerAdapter(dataSetConsumerPipe);

        return dataSetConsumerPipe;
    }

    private void addDatabaseDataSetConsumerAdapter(DataSetConsumerPipe dataSetConsumerPipe) {
        ReproduceConsumerAdapter databaseDataSetConsumerAdapter = new ReproduceConsumerAdapter();

        MigrationPipeCustomization migrationPipeCustomization = migrationPipeCustomizationFactory.create();

        if (migrationPipeCustomization != null) {
            ChainableDataSetConsumer afterMigrationConsumerConsumer = migrationPipeCustomization.getAfterMigrationConsumerConsumer();
            if (afterMigrationConsumerConsumer != null) {
                dataSetConsumerPipe.add(afterMigrationConsumerConsumer);
            }

            databaseDataSetConsumerAdapter.setRowFilterFactory(migrationPipeCustomization.getMigratedDataSetRowFilterFactory());
        }

        dataSetConsumerPipe.add(databaseDataSetConsumerAdapter);
    }


}
