package com.link_intersystems.dbunit.flyway;

import org.flywaydb.core.api.MigrationVersion;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlywayDataSetMigrationConfig {

    private MigrationVersion sourceVersion;
    private MigrationVersion targetVersion;

    public void setSourceVersion(String sourceVersion) {
        this.setSourceVersion(MigrationVersion.fromVersion(sourceVersion));
    }

    public void setSourceVersion(MigrationVersion sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    public MigrationVersion getSourceVersion() {
        return sourceVersion;
    }

    public void setTargetVersion(String targetVersion) {
        this.setTargetVersion(MigrationVersion.fromVersion(targetVersion));
    }

    public void setTargetVersion(MigrationVersion targetVersion) {
        this.targetVersion = targetVersion;
    }

    public MigrationVersion getTargetVersion() {
        return targetVersion;
    }
}
