package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseDefinition {
    private final String containerName;
    private String scriptsBase;


    public DatabaseDefinition(String containerName) {
        this(containerName, containerName);
    }

    public DatabaseDefinition(String containerName, String scriptsBase) {

        this.containerName = containerName;
        this.scriptsBase = scriptsBase;
    }

    public DatabaseContainerSupport getDatabaseContainerSupport() {
        return DatabaseContainerSupport.getDatabaseContainerSupport(containerName + ":latest");
    }

    public String getContainerName() {
        return containerName;
    }

    public String getScriptsBase() {
        return scriptsBase;
    }

    @Override
    public String toString() {
        if (containerName.equals(scriptsBase)) {
            return containerName;
        }
        return containerName + " (" + scriptsBase + ")";
    }

}
