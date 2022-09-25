package com.link_intersystems.dbunit.migration.flyway;

import com.link_intersystems.dbunit.migration.DataSourceProperties;
import com.link_intersystems.dbunit.migration.DatabaseMigrationSupport;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlywayDatabaseMigrationSupport implements DatabaseMigrationSupport {

    private PlaceholdersSourceTransformer placeholdersSourceTransformer = (ps, dsp) -> ps;

    private FlywayMigrationConfig migrationConfig;

    public FlywayDatabaseMigrationSupport(FlywayMigrationConfig migrationConfig) {
        this.migrationConfig = requireNonNull(migrationConfig);
    }

    public void setPlaceholdersSourceTransformer(PlaceholdersSourceTransformer placeholdersSourceTransformer) {
        this.placeholdersSourceTransformer = requireNonNull(placeholdersSourceTransformer);
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
        FluentConfiguration effectiveConfiguration = createFlywayConfiguration(properties);
        return new FlywayMigration(effectiveConfiguration);
    }

    private FluentConfiguration createFlywayConfiguration(DataSourceProperties properties) {
        FluentConfiguration effectiveConfiguration = new FluentConfiguration();

        Configuration basicConfiguration = migrationConfig.getFlywayConfiguration();
        effectiveConfiguration.configuration(basicConfiguration);

        PlaceholdersSource placeholdersSource = createPlaceholdersSource(properties);
        PlaceholdersSource transformedPlaceholderSource = placeholdersSourceTransformer.transform(placeholdersSource, properties);

        effectiveConfiguration.placeholders(transformedPlaceholderSource.getPlaceholders());

        return effectiveConfiguration;
    }

    private PlaceholdersSource createPlaceholdersSource(DataSourceProperties properties) {
        List<PlaceholdersSource> placeholdersSources = new ArrayList<>();

        PlaceholdersSource dataSourcePlaceholdersSource = createDataSourcePlaceholdersSource(properties);
        placeholdersSources.add(dataSourcePlaceholdersSource);

        PlaceholdersSource migrationConfigPlaceholdersSource = createMigrationConfigPlaceholdersSource(migrationConfig);
        placeholdersSources.add(migrationConfigPlaceholdersSource);

        return new PlaceholdersSourceChain(placeholdersSources);
    }

    protected PlaceholdersSource createMigrationConfigPlaceholdersSource(FlywayMigrationConfig migrationConfig) {
        Configuration configuration = migrationConfig.getFlywayConfiguration();
        return configuration::getPlaceholders;
    }

    protected PlaceholdersSource createDataSourcePlaceholdersSource(DataSourceProperties dataSourceProperties) {
        return PlaceholdersSource.fromMap(placeholders -> {
            placeholders.put("username", dataSourceProperties.getUsername());
            placeholders.put("password", dataSourceProperties.getPassword());
            placeholders.put("databaseName", dataSourceProperties.getDatabaseName());
            placeholders.put("host", dataSourceProperties.getHost());
            placeholders.put("port", dataSourceProperties.getPort());
            placeholders.put("jdbcUrl", dataSourceProperties.getJdbcUrl());

            Map<String, String> environmentProperties = dataSourceProperties.getEnvironment();
            for (Map.Entry<String, String> environmentPropertyEntry : environmentProperties.entrySet()) {
                placeholders.put("env." + environmentPropertyEntry.getKey(), environmentPropertyEntry.getValue());
            }
        });
    }

    protected Collection<String> getFlywayTables() {
        return singletonList("flyway_schema_history");
    }

    protected void dropTable(Statement statement, String flywayTable) throws SQLException {
        statement.execute("drop table " + flywayTable);
    }
}
