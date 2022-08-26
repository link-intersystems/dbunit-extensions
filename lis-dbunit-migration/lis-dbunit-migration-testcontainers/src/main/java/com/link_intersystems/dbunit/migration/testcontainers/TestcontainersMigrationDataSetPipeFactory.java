package com.link_intersystems.dbunit.migration.testcontainers;

import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.MigrationDataSetPipeFactory;
import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipe;
import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseDataSetConsumerAdapter;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseOperationConsumer;
import com.link_intersystems.dbunit.testcontainers.consumer.ExistingEntriesConsumerRowFilterFactory;
import com.link_intersystems.dbunit.testcontainers.consumer.TestContainersLifecycleConsumer;
import com.link_intersystems.dbunit.testcontainers.pool.RunningContainerPool;
import com.link_intersystems.dbunit.testcontainers.pool.SingleRunningContainerPool;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestcontainersMigrationDataSetPipeFactory implements MigrationDataSetPipeFactory {

    private RunningContainerPool runningContainerPool;

    private Supplier<MigrationPipeCustomization> migrationPipeCustomizationSupplier = () -> new MigrationPipeCustomization() {

        private ExistingEntriesConsumerRowFilterFactory existingEntriesConsumerRowFilterFactory = new ExistingEntriesConsumerRowFilterFactory();

        @Override
        public IRowFilterFactory getMigratedDataSetRowFilterFactory() {
            return existingEntriesConsumerRowFilterFactory;
        }

        @Override
        public ChainableDataSetConsumer getAfterMigrationConsumerConsumer() {
            return existingEntriesConsumerRowFilterFactory;
        }
    };

    public TestcontainersMigrationDataSetPipeFactory(String dockerImageName) {
        this(DatabaseContainerSupport.getDatabaseContainerSupport(dockerImageName));
    }

    public TestcontainersMigrationDataSetPipeFactory(DatabaseContainerSupport databaseContainerSupport) {
        this(new SingleRunningContainerPool(() -> new DBunitJdbcContainer(databaseContainerSupport.create(), databaseContainerSupport.getDatabaseConfig())));
    }

    public TestcontainersMigrationDataSetPipeFactory(RunningContainerPool runningContainerPool) {
        this.runningContainerPool = requireNonNull(runningContainerPool);
    }

    public void setMigrationPipeCustomizationSupplier(Supplier<MigrationPipeCustomization> migrationPipeCustomizationSupplier) {
        this.migrationPipeCustomizationSupplier = requireNonNull(migrationPipeCustomizationSupplier);
    }

    @Override
    public DataSetConsumerPipe createMigrationPipe(DatabaseMigrationSupport databaseMigrationSupport) {
        TestContainersLifecycleConsumer testContainersConsumer = new TestContainersLifecycleConsumer(runningContainerPool);


        MigrationDatabaseCustomizationConsumer databaseCustomizationConsumer = new MigrationDatabaseCustomizationConsumer(databaseMigrationSupport);

        DatabaseOperationConsumer testContainersMigrationConsumer = new DatabaseOperationConsumer();
        DatabaseDataSetConsumerAdapter databaseDataSetConsumerAdapter = new DatabaseDataSetConsumerAdapter();

        MigrationPipeCustomization migrationPipeCustomization = migrationPipeCustomizationSupplier.get();
        if (migrationPipeCustomization != null) {
            databaseDataSetConsumerAdapter.setRowFilterFactory(migrationPipeCustomization.getMigratedDataSetRowFilterFactory());
        }

        DataSetConsumerPipe dataSetConsumerPipe = new DataSetConsumerPipe();
        dataSetConsumerPipe.add(testContainersConsumer);
        dataSetConsumerPipe.add(databaseCustomizationConsumer);
        dataSetConsumerPipe.add(testContainersMigrationConsumer);
        if (migrationPipeCustomization != null) {
            ChainableDataSetConsumer afterMigrationConsumerConsumer = migrationPipeCustomization.getAfterMigrationConsumerConsumer();
            if (afterMigrationConsumerConsumer != null) {
                dataSetConsumerPipe.add(afterMigrationConsumerConsumer);
            }
        }
        dataSetConsumerPipe.add(databaseDataSetConsumerAdapter);

        return dataSetConsumerPipe;
    }

}
