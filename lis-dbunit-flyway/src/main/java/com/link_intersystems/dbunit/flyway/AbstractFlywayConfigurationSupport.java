package com.link_intersystems.dbunit.flyway;

import org.flywaydb.core.api.ClassProvider;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.JavaMigration;

import java.util.*;

import static java.util.Collections.unmodifiableList;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class AbstractFlywayConfigurationSupport implements FlywayConfigurationSupport {

    private List<String> locations = new ArrayList<>();
    private List<JavaMigration> javaMigrations = new ArrayList<>();
    private ClassProvider<JavaMigration> javaMigrationClassProvider;
    private MigrationVersion sourceVersion;
    private MigrationVersion targetVersion;
    private Map<String, String> placeholders = new HashMap<>();

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


    @Override
    public void setSourceVersion(String sourceVersion) {
        setSourceVersion(MigrationVersion.fromVersion(sourceVersion));
    }

    @Override
    public void setSourceVersion(MigrationVersion sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    @Override
    public MigrationVersion getSourceVersion() {
        return sourceVersion;
    }

    @Override
    public void setTargetVersion(String targetVersion) {
        setTargetVersion(MigrationVersion.fromVersion(targetVersion));
    }

    @Override
    public void setTargetVersion(MigrationVersion targetVersion) {
        this.targetVersion = targetVersion;
    }

    @Override
    public MigrationVersion getTargetVersion() {
        return targetVersion;
    }

    @Override
    public void setPlaceholders(Map<String, String> placeholders) {
        this.placeholders.clear();
        this.placeholders.putAll(placeholders);
    }

    @Override
    public Map<String, String> getPlaceholders() {
        return Collections.unmodifiableMap(placeholders);
    }
}
