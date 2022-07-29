package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DatabaseDefinition {
    DatabaseContainerSupport databaseContainerSupport;
    String containerName;


    public DatabaseDefinition(String containerName, DatabaseContainerSupport databaseContainerSupport) {
        this.databaseContainerSupport = databaseContainerSupport;
        this.containerName = containerName;
    }


    @Override
    public String toString() {
        return containerName;
    }
}
