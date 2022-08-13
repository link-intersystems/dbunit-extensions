package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.migration.flyway.FlywayDatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.flyway.FlywayMigrationConfig;
import com.link_intersystems.dbunit.migration.testcontainers.TestcontainersMigrationDataSetTransformerFactory;
import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DefaultDataSetConsumerSupport;
import com.link_intersystems.dbunit.test.TestDataSets;
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
                new DatabaseDefinition("postgres"),
                new DatabaseDefinition("mysql")
        );
    }

    @ParameterizedTest
    @MethodSource("databases")
    void migrate(DatabaseDefinition databaseDefinition) throws DataSetException, IOException {
        DataSetMigration dataSetMigration = new DataSetMigration();

        IDataSet sourceDataSet = TestDataSets.getTinySakilaDataSet();
        dataSetMigration.setDataSetProducer(sourceDataSet);

        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();

        DefaultDataSetConsumerSupport consumerSupport = new DefaultDataSetConsumerSupport();
        consumerSupport.setCsvConsumer("target/csv");
        IDataSetConsumer csvConsumer = consumerSupport.getDataSetConsumer();

        consumerSupport.setFlatXmlConsumer("target/flat.xml");
        IDataSetConsumer flatXmlConsumer = consumerSupport.getDataSetConsumer();

        dataSetMigration.setDataSetConsumers(copyDataSetConsumer, csvConsumer, flatXmlConsumer);

        dataSetMigration.setMigrationDataSetTransformerFactory(new TestcontainersMigrationDataSetTransformerFactory(databaseDefinition.getDatabaseContainerSupport()));

        FlywayMigrationConfig migrationConfig = FlywayConfigurationConfigFixture.createConfig(databaseDefinition.getContainerName());
        dataSetMigration.setDatabaseMigrationSupport(new FlywayDatabaseMigrationSupport(migrationConfig));

        dataSetMigration.exec();

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