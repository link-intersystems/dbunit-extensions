package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DatabaseDefinition {
    private final String containerName;
    private String scriptsBase;
    private String sourceVersion;


    public DatabaseDefinition(String containerName) {
        this(containerName, containerName, "1");
    }

    public DatabaseDefinition(String containerName, String scriptsBase, String sourceVersion) {

        this.containerName = containerName;
        this.scriptsBase = scriptsBase;
        this.sourceVersion = sourceVersion;
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

    public String getSourceVersion() {
        return sourceVersion;
    }
}
