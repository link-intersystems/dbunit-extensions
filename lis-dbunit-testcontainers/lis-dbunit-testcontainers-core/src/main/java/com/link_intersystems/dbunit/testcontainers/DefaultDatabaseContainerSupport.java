package com.link_intersystems.dbunit.testcontainers;

import org.dbunit.database.DatabaseConfig;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDatabaseContainerSupport extends DatabaseContainerSupport {

    private DatabaseConfig databaseConfig = new DatabaseConfig();

    private Supplier<JdbcDatabaseContainer<?>> containerSupplier;

    public DefaultDatabaseContainerSupport(Supplier<JdbcDatabaseContainer<?>> containerSupplier) {
        this.containerSupplier = requireNonNull(containerSupplier);
    }

    @Override
    public JdbcDatabaseContainer<?> create() {
        return containerSupplier.get();
    }

    @Override
    public DatabaseConfig getDatabaseConfig() {
        return databaseConfig;
    }

}
