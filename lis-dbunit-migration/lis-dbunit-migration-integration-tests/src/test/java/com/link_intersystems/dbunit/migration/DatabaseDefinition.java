package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseDefinition {
    private final String containerName;


    public DatabaseDefinition(String containerName) {
        this.containerName = containerName;
    }

    public DatabaseContainerSupport getDatabaseContainerSupport() {
        return DatabaseContainerSupport.getDatabaseContainerSupport(containerName + ":latest");
    }

    public String getContainerName() {
        return containerName;
    }

    @Override
    public String toString() {
        return containerName;
    }

}
