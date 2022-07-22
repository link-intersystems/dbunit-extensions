package com.link_intersystems.dbunit.testcontainers.consumer;

import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DatabaseContainerFactory {
    public JdbcDatabaseContainer<?> create();
}
