package com.link_intersystems.dbunit.flyway;

import org.dbunit.dataset.DataSetException;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.migration.JavaMigration;
import org.flywaydb.core.api.output.MigrateResult;

import javax.sql.DataSource;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultFlywayMigration extends AbstractFlywayConfigurationSupport implements FlywayMigration {

    @Override
    public void execute(DataSource dataSource, MigrationVersion targetVersion) throws DataSetException {
        FluentConfiguration configuration = Flyway.configure();
        configureFlyway(configuration);
        configuration.dataSource(dataSource);
        configuration.target(targetVersion);

        Flyway flyway = configuration.load();
        execute(flyway);
    }

    protected void configureFlyway(FluentConfiguration configuration) {
        configuration.locations(getLocations().toArray(new String[0]));
        configuration.javaMigrations(getJavaMigrations().toArray(new JavaMigration[0]));
        configuration.javaMigrationClassProvider(getJavaMigrationClassProvider());
        configuration.placeholders(getPlaceholders());
    }

    protected void execute(Flyway flyway) throws DataSetException {
        MigrateResult migrateResult = flyway.migrate();
        if (!migrateResult.success) {
            throw new DataSetException("Unable to setup baseline ");
        }
    }
}
