package com.link_intersystems.dbunit.flyway;

import org.flywaydb.core.api.ClassProvider;
import org.flywaydb.core.api.migration.JavaMigration;

import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface FlywayConfigurationSupport extends FlywayMigrationConfiguration {
    void setLocations(String... locations);

    void setLocations(List<String> locations);

    void setJavaMigrations(JavaMigration... javaMigrations);

    void setJavaMigrations(List<JavaMigration> javaMigrations);

    void setJavaMigrationClassProvider(ClassProvider<JavaMigration> javaMigrationClassProvider);

    default void apply(FlywayMigrationConfiguration migrationConfiguration) {
        if (this == migrationConfiguration) {
            return;
        }

        setLocations(migrationConfiguration.getLocations());
        setJavaMigrations(migrationConfiguration.getJavaMigrations());
        setJavaMigrationClassProvider(migrationConfiguration.getJavaMigrationClassProvider());
    }
}
