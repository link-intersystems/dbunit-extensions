package com.link_intersystems.dbunit.flyway;

import org.dbunit.dataset.DataSetException;
import org.flywaydb.core.api.MigrationVersion;

import javax.sql.DataSource;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface FlywayMigration extends FlywayConfigurationSupport {
    void execute(DataSource dataSource, MigrationVersion targetVersion) throws DataSetException;
}
