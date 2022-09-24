package com.link_intersystems.dbunit.migration.flyway;

import com.link_intersystems.dbunit.migration.DataSourceProperties;
import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlywayDatabaseMigrationSupport implements DatabaseMigrationSupport {

    private static final Collection<String> FLYWAY_TABLES = Arrays.asList("flyway_schema_history");

    private FlywayMigrationConfig migrationConfig;

    public FlywayDatabaseMigrationSupport(FlywayMigrationConfig migrationConfig) {
        this.migrationConfig = requireNonNull(migrationConfig);
    }

    public FlywayMigrationConfig getMigrationConfig() {
        return migrationConfig;
    }

    @Override
    public void prepareDataSource(DataSource dataSource, DataSourceProperties properties) throws SQLException {
        FlywayMigration flywayMigration = createFlywayMigration(properties);
        FlywayMigrationConfig migrationConfig = getMigrationConfig();

        flywayMigration.execute(dataSource, migrationConfig.getSourceVersion());
    }


    @Override
    public void migrateDataSource(DataSource dataSource, DataSourceProperties properties) throws SQLException {
        FlywayMigration flywayMigration = createFlywayMigration(properties);
        FlywayMigrationConfig migrationConfig = getMigrationConfig();

        flywayMigration.execute(dataSource, migrationConfig.getTargetVersion());

        afterMigrate(dataSource);
    }

    protected void afterMigrate(DataSource dataSource) throws SQLException {
        if (getMigrationConfig().isRemoveFlywayTables()) {
            try (Connection connection = dataSource.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    for (String flywayTable : getFlywayTables()) {
                        dropTable(statement, flywayTable);
                    }
                }
            }
        }
    }

    protected FlywayMigration createFlywayMigration(DataSourceProperties properties) {
        Configuration configuration = migrationConfig.getFlywayConfiguration();
        FluentConfiguration effectiveConfiguration = createEffectiveConfiguration(configuration, properties);

        return new FlywayMigration(effectiveConfiguration);
    }

    protected FluentConfiguration createEffectiveConfiguration(Configuration configuration, DataSourceProperties properties) {
        FluentConfiguration effectiveConfiguration = new FluentConfiguration();
        effectiveConfiguration.configuration(configuration);

        Map<String, String> placeholders = new HashMap<>();

        placeholders.putAll(properties);

        Map<String, String> environmentProperties = properties.getEnvironmentProperties();
        for (Map.Entry<String, String> environmentPropertyEntry : environmentProperties.entrySet()) {
            placeholders.put("env." + environmentPropertyEntry.getKey(), environmentPropertyEntry.getValue());
        }

        placeholders.putAll(configuration.getPlaceholders());
        effectiveConfiguration.placeholders(placeholders);

        return effectiveConfiguration;
    }

    protected Collection<String> getFlywayTables() {
        return FLYWAY_TABLES;
    }

    protected void dropTable(Statement statement, String flywayTable) throws SQLException {
        statement.execute("drop table " + flywayTable);
    }
}
