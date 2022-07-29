package com.link_intersystems.dbunit.flyway;

import org.dbunit.dataset.DataSetException;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.migration.JavaMigration;
import org.flywaydb.core.api.output.MigrateResult;

import javax.sql.DataSource;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultFlywayMigration implements FlywayMigration {

    private Supplier<FluentConfiguration> flywayConfigurationSupplier = Flyway::configure;

    public void setFlywayConfigurationSupplier(Supplier<FluentConfiguration> flywayConfigurationSupplier) {
        this.flywayConfigurationSupplier = Objects.requireNonNull(flywayConfigurationSupplier);
    }

    @Override
    public void execute(DataSource dataSource, MigrationVersion targetVersion) throws DataSetException {
        FluentConfiguration configuration = flywayConfigurationSupplier.get();
        configuration.dataSource(dataSource);
        configuration.target(targetVersion);

        Flyway flyway = configuration.load();
        execute(flyway);
    }

    protected void execute(Flyway flyway) throws DataSetException {
        MigrateResult migrateResult = flyway.migrate();
        if (!migrateResult.success) {
            throw new DataSetException("Unable to setup baseline ");
        }
    }
}
