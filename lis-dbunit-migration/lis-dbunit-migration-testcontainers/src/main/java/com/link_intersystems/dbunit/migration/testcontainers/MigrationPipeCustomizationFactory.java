package com.link_intersystems.dbunit.migration.testcontainers;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@FunctionalInterface
public interface MigrationPipeCustomizationFactory {
    MigrationPipeCustomization create();
}
