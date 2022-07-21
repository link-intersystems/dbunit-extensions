package com.link_intersystems.dbunit.commands.flyway;

import org.flywaydb.core.api.ClassProvider;
import org.flywaydb.core.api.migration.JavaMigration;

import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface FlywayConfigurationSupport {
    void setLocations(String... locations);

    void setLocations(List<String> locations);

    void setJavaMigrations(JavaMigration... javaMigrations);

    void setJavaMigrations(List<JavaMigration> javaMigrations);

    ClassProvider<JavaMigration> getJavaMigrationClassProvider();

    void setJavaMigrationClassProvider(ClassProvider<JavaMigration> javaMigrationClassProvider);

    List<JavaMigration> getJavaMigrations();

    List<String> getLocations();

    default void apply(FlywayConfigurationSupport flywayConfigurationSupport) {
        if (this == flywayConfigurationSupport) {
            return;
        }

        setLocations(flywayConfigurationSupport.getLocations());
        setJavaMigrations(flywayConfigurationSupport.getJavaMigrations());
        setJavaMigrationClassProvider(flywayConfigurationSupport.getJavaMigrationClassProvider());
    }
}
