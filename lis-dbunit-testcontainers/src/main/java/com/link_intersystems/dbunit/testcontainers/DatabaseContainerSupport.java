package com.link_intersystems.dbunit.testcontainers;

import org.dbunit.database.DatabaseConfig;
import org.testcontainers.containers.JdbcDatabaseContainer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DatabaseContainerSupport {
    public JdbcDatabaseContainer<?> create();

    default public DatabaseConfig getDatabaseConfig(){
        return new DatabaseConfig();
    }
}
