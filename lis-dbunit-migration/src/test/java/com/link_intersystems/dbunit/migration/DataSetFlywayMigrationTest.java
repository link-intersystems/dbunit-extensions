package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.flyway.FlywayMigrationConfig;
import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DefaultDataSetConsumerSupport;
import com.link_intersystems.dbunit.test.TestDataSets;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetFlywayMigrationTest {

    static Stream<DatabaseDefinition> databases() {
        return Stream.of(
                        new DatabaseDefinition("postgres", DatabaseContainerSupport.getDatabaseContainerSupport("postgres:latest")),
                        new DatabaseDefinition("mysql", DatabaseContainerSupport.getDatabaseContainerSupport("mysql:latest"))
        );
    }

    @ParameterizedTest
    @MethodSource("databases")
    void migrate(DatabaseDefinition databaseDefinition) throws DataSetException, IOException {
        DataSetFlywayMigration flywayMigration = new DataSetFlywayMigration();

        IDataSet sourceDataSet = TestDataSets.getTinySakilaDataSet();
        flywayMigration.setDataSetProducer(sourceDataSet);

        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();

        DefaultDataSetConsumerSupport consumerSupport = new DefaultDataSetConsumerSupport();
        consumerSupport.setCsvConsumer("target/csv");
        IDataSetConsumer csvConsumer = consumerSupport.getDataSetConsumer();

        consumerSupport.setFlatXmlConsumer("target/flat.xml");
        IDataSetConsumer flatXmlConsumer = consumerSupport.getDataSetConsumer();

        flywayMigration.setDataSetConsumers(copyDataSetConsumer, csvConsumer, flatXmlConsumer);

        flywayMigration.setDatabaseContainerSupport(databaseDefinition.databaseContainerSupport);

        FlywayMigrationConfig migrationConfig = FlywayConfigurationConfigFixture.createConfig(databaseDefinition.containerName);
        flywayMigration.setMigrationConfig(migrationConfig);

        flywayMigration.exec();

        IDataSet migratedDataSet = copyDataSetConsumer.getDataSet();

        ITable filmDescriptionTable = migratedDataSet.getTable("film_description");
        assertNotNull(filmDescriptionTable);

        ITable film = sourceDataSet.getTable("film");
        DefaultTable filmWithMetaData = new DefaultTable(filmDescriptionTable.getTableMetaData());
        filmWithMetaData.addTableRows(film);

        for (int rowIndex = 0; rowIndex < filmDescriptionTable.getRowCount(); rowIndex++) {
            assertEquals(filmWithMetaData.getValue(rowIndex, "film_id"), filmDescriptionTable.getValue(rowIndex, "film_id").toString());
            assertEquals(filmWithMetaData.getValue(rowIndex, "description"), filmDescriptionTable.getValue(rowIndex, "description"));
        }
    }
}