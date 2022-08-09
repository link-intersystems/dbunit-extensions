package com.link_intersystems.dbunit.migration.testcontainers;

import com.link_intersystems.dbunit.migration.MigrationDataSetTransformerFactory;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.stream.consumer.DatabaseMigrationSupport;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.consumer.TestContainersDataSetTransformer;

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
        return new TestContainersDataSetTransformer(databaseContainerSupport, databaseMigrationSupport);
    }
}
