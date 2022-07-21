package com.link_intersystems.dbunit.flyway;

import org.flywaydb.core.api.ClassProvider;
import org.flywaydb.core.api.migration.JavaMigration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.unmodifiableList;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class AbstractFlywayConfigurationSupport implements FlywayConfigurationSupport {

    private List<String> locations = new ArrayList<>();
    private List<JavaMigration> javaMigrations = new ArrayList<>();
    private ClassProvider<JavaMigration> javaMigrationClassProvider;

    @Override
    public void setLocations(String... locations) {
        setLocations(Arrays.asList(locations));
    }

    @Override
    public void setLocations(List<String> locations) {
        this.locations.clear();
        this.locations.addAll(locations);
    }

    @Override
    public void setJavaMigrations(JavaMigration... javaMigrations) {
        setJavaMigrations(Arrays.asList(javaMigrations));
    }

    @Override
    public void setJavaMigrations(List<JavaMigration> javaMigrations) {
        this.javaMigrations.clear();
        this.javaMigrations.addAll(javaMigrations);
    }

    @Override
    public ClassProvider<JavaMigration> getJavaMigrationClassProvider() {
        return javaMigrationClassProvider;
    }

    @Override
    public void setJavaMigrationClassProvider(ClassProvider<JavaMigration> javaMigrationClassProvider) {
        this.javaMigrationClassProvider = javaMigrationClassProvider;
    }

    @Override
    public List<JavaMigration> getJavaMigrations() {
        return unmodifiableList(javaMigrations);
    }

    @Override
    public List<String> getLocations() {
        return unmodifiableList(locations);
    }


}
