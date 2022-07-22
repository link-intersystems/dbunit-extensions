package com.link_intersystems.dbunit.testcontainers.consumer;

import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDatabaseContainerFactory implements DatabaseContainerFactory {

    private Supplier<JdbcDatabaseContainer<?>> containerSupplier;

    public DefaultDatabaseContainerFactory(Supplier<JdbcDatabaseContainer<?>> containerSupplier) {
        this.containerSupplier = requireNonNull(containerSupplier);
    }

    @Override
    public JdbcDatabaseContainer<?> create() {
        return containerSupplier.get();
    }

}
