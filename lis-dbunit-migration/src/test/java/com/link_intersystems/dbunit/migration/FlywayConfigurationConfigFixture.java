package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.flyway.FlywayMigrationConfig;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;

import java.util.HashMap;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlywayConfigurationConfigFixture {

    public static FlywayMigrationConfig createPostgresConfig() {
        FlywayMigrationConfig migrationConfig = new FlywayMigrationConfig();

        FluentConfiguration configuration = Flyway.configure();
        configuration.locations("com/link_intersystems/dbunit/migration/postgres");
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("new_first_name_column_name", "firstname");
        placeholders.put("new_last_name_column_name", "lastname");
        configuration.placeholders(placeholders);

        migrationConfig.setFlywayConfiguration(configuration);

        migrationConfig.setSourceVersion("1");
        return migrationConfig;
    }
}
