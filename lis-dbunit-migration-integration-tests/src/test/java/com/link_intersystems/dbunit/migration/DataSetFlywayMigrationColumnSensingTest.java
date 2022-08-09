package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.migration.flyway.FlywayDatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.flyway.FlywayMigrationConfig;
import com.link_intersystems.dbunit.migration.testcontainers.TestcontainersMigrationDataSetTransformerFactory;
import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DefaultDataSetConsumerSupport;
import com.link_intersystems.dbunit.table.Row;
import com.link_intersystems.dbunit.table.TableUtil;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetFlywayMigrationColumnSensingTest {

    @Test
    void migrate() throws DataSetException, IOException {
        DatabaseContainerSupport postgres = DatabaseContainerSupport.getDatabaseContainerSupport("postgres:latest");
        DatabaseDefinition databaseDefinition = new DatabaseDefinition("postgres", postgres);

        DataSetMigration flywayMigration = new DataSetMigration();

        InputStream resourceAsStream = DataSetFlywayMigrationColumnSensingTest.class.getResourceAsStream("/tiny-sakila-flat-column-sensing.xml");
        FlatXmlDataSet sourceDataSet = new FlatXmlDataSetBuilder().setColumnSensing(true).build(resourceAsStream);
        flywayMigration.setDataSetProducer(sourceDataSet);

        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();

        DefaultDataSetConsumerSupport consumerSupport = new DefaultDataSetConsumerSupport();

        consumerSupport.setFlatXmlConsumer("target/flat.xml");
        IDataSetConsumer flatXmlConsumer = consumerSupport.getDataSetConsumer();

        flywayMigration.setDataSetConsumers(copyDataSetConsumer, flatXmlConsumer);

        flywayMigration.setMigrationDataSetTransformerFactory(new TestcontainersMigrationDataSetTransformerFactory(databaseDefinition.databaseContainerSupport));

        FlywayMigrationConfig migrationConfig = FlywayConfigurationConfigFixture.createPostgresConfig();
        flywayMigration.setDatabaseMigrationSupport(new FlywayDatabaseMigrationSupport(migrationConfig));

        flywayMigration.exec();

        IDataSet migratedDataSet = copyDataSetConsumer.getDataSet();

        ITable actorTable = migratedDataSet.getTable("actor");
        assertNotNull(actorTable);

        assertEquals(2, actorTable.getRowCount());
        TableUtil actorUtil = new TableUtil(actorTable);
        Row row = actorUtil.getRowById(2);
        assertEquals("WAHLBERG", row.getValue("lastname"));

    }
}