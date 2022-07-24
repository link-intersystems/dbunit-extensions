package com.link_intersystems.dbunit.flyway;

import org.flywaydb.core.api.ClassProvider;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.JavaMigration;

import java.util.Collections;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface FlywayMigrationConfiguration {
    default ClassProvider<JavaMigration> getJavaMigrationClassProvider() {
        return null;
    }

    default List<JavaMigration> getJavaMigrations() {
        return Collections.emptyList();
    }

    default List<String> getLocations() {
        return Collections.emptyList();
    }

    MigrationVersion getSourceVersion();

    MigrationVersion getTargetVersion();
}
