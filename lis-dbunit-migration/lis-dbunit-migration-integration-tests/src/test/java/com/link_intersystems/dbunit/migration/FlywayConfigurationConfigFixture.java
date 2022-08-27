package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.migration.flyway.FlywayMigrationConfig;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlywayConfigurationConfigFixture {

    public static FlywayMigrationConfig createConfig(DatabaseDefinition databaseDefinition) {
        FlywayMigrationConfig migrationConfig = new FlywayMigrationConfig();

        String scriptsBase = databaseDefinition.getScriptsBase();

        FluentConfiguration configuration = Flyway.configure();
        configuration.locations("com/link_intersystems/dbunit/migration/" + scriptsBase);
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("new_first_name_column_name", "firstname");
        placeholders.put("new_last_name_column_name", "lastname");
        configuration.placeholders(placeholders);

        migrationConfig.setFlywayConfiguration(configuration);

        migrationConfig.setSourceVersion(databaseDefinition.getSourceVersion());
        return migrationConfig;
    }

    public static FlywayMigrationConfig createPostgresConfig() {
        return createConfig(new DatabaseDefinition("postgres"));
    }

    public static FlywayMigrationConfig createMySqlConfig() {
        return createConfig(new DatabaseDefinition("mysql"));
    }
}
