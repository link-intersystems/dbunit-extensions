package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.migration.flyway.FlywayDatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.flyway.FlywayMigrationConfig;
import com.link_intersystems.dbunit.migration.testcontainers.TestcontainersMigrationDataSetPipeFactory;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * This test is primarily used for documentation issues, e.g. link it in a README.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetMigrationDocTest {

    @Test
    void migrate() throws DataSetException, IOException {
        DataSetMigration dataSetMigration = new DataSetMigration();

        dataSetMigration.setDataSetProducer(sourceDataSet);
        dataSetMigration.setFlatXmlConsumer("target/migrated-source-data-set.xml");

        DatabaseContainerSupport containerSupport = DatabaseContainerSupport.getDatabaseContainerSupport("postgres:latest");
        dataSetMigration.setMigrationDataSetTransformerFactory(new TestcontainersMigrationDataSetPipeFactory(containerSupport));

        FlywayMigrationConfig migrationConfig = createConfig();
        dataSetMigration.setDatabaseMigrationSupport(new FlywayDatabaseMigrationSupport(migrationConfig));

        dataSetMigration.exec();
    }

    /*
     * Place everything else at the bottom so that the line number of the migrate test does not change for
     * documentation linking reasons.
     */
    private IDataSet sourceDataSet;

    @BeforeEach
    void setUp() throws DataSetException {
        InputStream resourceAsStream = DataSetMigrationDocTest.class.getResourceAsStream("/tiny-sakila-flat-column-sensing.xml");
        sourceDataSet = new FlatXmlDataSetBuilder().setColumnSensing(true).build(resourceAsStream);
    }

    public static FlywayMigrationConfig createConfig() {
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