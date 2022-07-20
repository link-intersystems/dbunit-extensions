package com.link_intersystems.dbunit.commands;

import com.link_intersystems.dbunit.commands.flyway.FlywayDatabaseMigrationSupport;
import com.link_intersystems.dbunit.commands.flyway.TestContainersDataSetTransformer;
import com.link_intersystems.dbunit.stream.consumer.*;
import com.link_intersystems.dbunit.stream.producer.DataSetBuilder;
import com.link_intersystems.dbunit.table.DatabaseTableOrder;
import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@Disabled("I have to figure out how to start containers in github actions first")
class DataSetTransformCommandTest {

    @Test
    void applyDataSetMigration() throws DataSetException, IOException {
        DataSetTransformCommand dataSetTransformCommand = new DataSetTransformCommand();

        IDataSet sourceDataSet = TestDataSets.getTinySakilaDataSet();
        dataSetTransformCommand.setDataSetProducer(sourceDataSet);
        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();

        DefaultDataSetConsumerSupport consumerSupport = new DefaultDataSetConsumerSupport();
        consumerSupport.setCsvConsumer("target/csv");
        IDataSetConsumer csvConsumer = consumerSupport.getDataSetConsumer();

        consumerSupport.setFlatXmlConsumer("target/flat.xml");
        IDataSetConsumer flatXmlConsumer = consumerSupport.getDataSetConsumer();

        dataSetTransformCommand.setDataSetConsumers(copyDataSetConsumer, csvConsumer,flatXmlConsumer, new DataSetPrinterConsumer());

        TestContainersDataSetTransformer transformer = new TestContainersDataSetTransformer();
        FlywayDatabaseMigrationSupport databaseContainerHandler = new FlywayDatabaseMigrationSupport();
        databaseContainerHandler.setLocations("com/link_intersystems/dbunit/commands/migrations");
        databaseContainerHandler.setStartVersion("1");
        transformer.setDatabaseContainerHandler(databaseContainerHandler);
        dataSetTransformCommand.setDataSetTransformer(transformer);

        dataSetTransformCommand.exec();

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