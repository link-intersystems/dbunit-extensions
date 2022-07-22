package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DataSetPrinterConsumer;
import com.link_intersystems.dbunit.stream.consumer.DefaultDataSetConsumerSupport;
import com.link_intersystems.dbunit.test.TestDataSets;
import com.link_intersystems.dbunit.testcontainers.consumer.JdbcDatabaseContainerFactory;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
//@Disabled("I have to figure out how to start containers in github actions first")
class DataSetFlywayMigrationTest {

    private static class DatabaseDefinition {
        JdbcDatabaseContainerFactory jdbcDatabaseContainerFactory;
        String containerName;


        public DatabaseDefinition(String containerName, Function<String, JdbcDatabaseContainer<?>> containerConstructor) {
            this.jdbcDatabaseContainerFactory = () -> containerConstructor.apply(containerName);
            this.containerName = containerName;
        }


        @Override
        public String toString() {
            return containerName;
        }
    }

    static Stream<DatabaseDefinition> databases() {
        return Arrays.asList(
                        new DatabaseDefinition("postgres", PostgreSQLContainer::new),
                        new DatabaseDefinition("mysql", MySQLContainer::new))
                .stream();
    }

    @ParameterizedTest
    @MethodSource("databases")
    void applyDataSetMigration(DatabaseDefinition databaseDefinition) throws DataSetException, IOException {
        DataSetFlywayMigration flywayMigrationCommand = new DataSetFlywayMigration();

        IDataSet sourceDataSet = TestDataSets.getTinySakilaDataSet();
        flywayMigrationCommand.setDataSetProducer(sourceDataSet);

        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();

        DefaultDataSetConsumerSupport consumerSupport = new DefaultDataSetConsumerSupport();
        consumerSupport.setCsvConsumer("target/csv");
        IDataSetConsumer csvConsumer = consumerSupport.getDataSetConsumer();

        consumerSupport.setFlatXmlConsumer("target/flat.xml");
        IDataSetConsumer flatXmlConsumer = consumerSupport.getDataSetConsumer();

        flywayMigrationCommand.setDataSetConsumers(copyDataSetConsumer, csvConsumer, flatXmlConsumer, new DataSetPrinterConsumer());

        flywayMigrationCommand.setJdbcDatabaseContainerFactory(databaseDefinition.jdbcDatabaseContainerFactory);

        flywayMigrationCommand.setSourceVersion("1");
        flywayMigrationCommand.setLocations("com/link_intersystems/dbunit/migration/" + databaseDefinition.containerName);

        flywayMigrationCommand.exec();

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