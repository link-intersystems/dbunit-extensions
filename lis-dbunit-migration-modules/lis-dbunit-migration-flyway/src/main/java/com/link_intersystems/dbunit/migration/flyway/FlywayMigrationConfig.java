package com.link_intersystems.dbunit.migration.flyway;

import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlywayMigrationConfig {

    private MigrationVersion sourceVersion;
    private MigrationVersion targetVersion;
    private boolean removeFlywayTables = true;
    private Configuration flywayConfiguration = new FluentConfiguration();

    public void setFlywayConfiguration(Configuration flywayConfiguration) {
        this.flywayConfiguration = requireNonNull(flywayConfiguration);
    }

    public void setRemoveFlywayTables(boolean removeFlywayTables) {
        this.removeFlywayTables = removeFlywayTables;
    }

    public boolean isRemoveFlywayTables() {
        return removeFlywayTables;
    }

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

    public Configuration getFlywayConfiguration() {
        return flywayConfiguration;
    }
}
