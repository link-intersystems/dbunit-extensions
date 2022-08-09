package com.link_intersystems.dbunit.migration.flyway;

import org.dbunit.dataset.DataSetException;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.output.MigrateResult;

import javax.sql.DataSource;
import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlywayMigration {

    private Configuration configuration;

    public FlywayMigration(Configuration configuration) {
        this.configuration = Objects.requireNonNull(configuration);
    }

    public void execute(DataSource dataSource, MigrationVersion targetVersion) throws DataSetException {
        ClassLoader classLoader = configuration.getClassLoader();
        FluentConfiguration fluentConfiguration = new FluentConfiguration(classLoader);
        fluentConfiguration.configuration(configuration);

        fluentConfiguration.dataSource(dataSource);
        fluentConfiguration.target(targetVersion);

        Flyway flyway = fluentConfiguration.load();
        execute(flyway);
    }

    protected void execute(Flyway flyway) throws DataSetException {
        MigrateResult migrateResult = flyway.migrate();
        if (!migrateResult.success) {
            throw new DataSetException("Unable to setup baseline ");
        }
    }
}
