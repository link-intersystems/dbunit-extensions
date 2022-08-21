package com.link_intersystems.dbunit.migration.flyway;

import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
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

    public void execute(DataSource dataSource, MigrationVersion targetVersion) throws FlywayException {
        ClassLoader classLoader = configuration.getClassLoader();
        FluentConfiguration fluentConfiguration = new FluentConfiguration(classLoader);
        fluentConfiguration.configuration(configuration);

        fluentConfiguration.dataSource(dataSource);
        fluentConfiguration.target(targetVersion);

        Flyway flyway = fluentConfiguration.load();
        execute(flyway);
    }

    protected void execute(Flyway flyway) throws FlywayException {
        MigrateResult migrateResult = flyway.migrate();
        if (!migrateResult.success) {
            throw new FlywayException("Unable to setup baseline ");
        }
    }
}
