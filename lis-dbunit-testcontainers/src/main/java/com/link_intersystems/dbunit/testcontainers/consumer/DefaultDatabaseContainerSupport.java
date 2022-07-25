package com.link_intersystems.dbunit.testcontainers.consumer;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.MessageFormat;
import java.util.function.Supplier;

import static java.text.MessageFormat.format;
import static java.util.Objects.requireNonNull;
import static org.dbunit.database.DatabaseConfig.PROPERTY_DATATYPE_FACTORY;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDatabaseContainerSupport implements DatabaseContainerSupport {

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
