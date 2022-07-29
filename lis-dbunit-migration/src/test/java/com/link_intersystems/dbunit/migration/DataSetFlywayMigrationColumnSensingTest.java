package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.flyway.FlywayDataSetMigrationConfig;
import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DefaultDataSetConsumerSupport;
import com.link_intersystems.dbunit.table.Row;
import com.link_intersystems.dbunit.table.TableUtil;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupportFactory;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetFlywayMigrationColumnSensingTest {

    @Test
    void migrate() throws DataSetException, IOException {
        DatabaseContainerSupport postgres = DatabaseContainerSupportFactory.INSTANCE.createPostgres("postgres:latest");
        DatabaseDefinition databaseDefinition = new DatabaseDefinition("postgres", postgres);

        DataSetFlywayMigration flywayMigration = new DataSetFlywayMigration();

        InputStream resourceAsStream = DataSetFlywayMigrationColumnSensingTest.class.getResourceAsStream("/tiny-sakila-flat-column-sensing.xml");
        FlatXmlDataSet sourceDataSet = new FlatXmlDataSetBuilder().setColumnSensing(true).build(resourceAsStream);
        flywayMigration.setDataSetProducer(sourceDataSet);

        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();

        DefaultDataSetConsumerSupport consumerSupport = new DefaultDataSetConsumerSupport();

        consumerSupport.setFlatXmlConsumer("target/flat.xml");
        IDataSetConsumer flatXmlConsumer = consumerSupport.getDataSetConsumer();

        flywayMigration.setDataSetConsumers(copyDataSetConsumer, flatXmlConsumer);

        flywayMigration.setDatabaseContainerSupport(databaseDefinition.databaseContainerSupport);

        FlywayDataSetMigrationConfig migrationConfig = new FlywayDataSetMigrationConfig();
        migrationConfig.setSourceVersion("1");
        flywayMigration.setMigrationConfig(migrationConfig);

        FluentConfiguration flywayConfiguration = Flyway.configure();
        flywayConfiguration.locations("com/link_intersystems/dbunit/migration/" + databaseDefinition.containerName);
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("new_first_name_column_name", "firstname");
        placeholders.put("new_last_name_column_name", "lastname");
        flywayConfiguration.placeholders(placeholders);

        flywayMigration.setFlywayConfiguration(flywayConfiguration);

        flywayMigration.exec();

        IDataSet migratedDataSet = copyDataSetConsumer.getDataSet();

        ITable actorTable = migratedDataSet.getTable("actor");
        assertNotNull(actorTable);

        assertEquals(2, actorTable.getRowCount());
        TableUtil actorUtil = new TableUtil(actorTable);
        Row row = actorUtil.getRowById(2);
        assertEquals("WAHLBERG", row.getValueByColumnName("lastname"));

    }
}