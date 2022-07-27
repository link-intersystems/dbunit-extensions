package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipeTransformerAdapter;
import com.link_intersystems.dbunit.stream.consumer.ExternalSortTableConsumer;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetection;
import com.link_intersystems.dbunit.table.DefaultTableOrder;
import com.link_intersystems.dbunit.table.TableOrder;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseContainerSupportFactory;
import com.link_intersystems.io.Unzip;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetCollectionFlywayMigrationTest {

    private Path sourcePath;
    private Path targetPath;

    @BeforeEach
    void setUp(@TempDir Path tmpDir) throws IOException {
        this.sourcePath = Paths.get(tmpDir.toString(), "source");
        this.targetPath = Paths.get(tmpDir.toString(), "target");
        Unzip.unzip(DataSetCollectionFlywayMigration.class.getResourceAsStream("/tiny-sakila-dataset-files.zip"), sourcePath);
    }

    @Test
    void migrateDataSetCollection() throws DataSetException {
        DataSetCollectionFlywayMigration dataSetCollectionMigration = new DataSetCollectionFlywayMigration();
        DataSetFileLocationsScanner fileLocationsScanner = new DataSetFileLocationsScanner(sourcePath);
        fileLocationsScanner.addDefaultFilePatterns();
        dataSetCollectionMigration.setDataSetFileLocations(fileLocationsScanner);

        dataSetCollectionMigration.setDatabaseContainerSupport(DatabaseContainerSupportFactory.INSTANCE.createPostgres("postgres:latest"));
        dataSetCollectionMigration.setLocations("com/link_intersystems/dbunit/migration/postgres");
        dataSetCollectionMigration.setTargetPathSupplier(new BasepathTargetPathSupplier(targetPath));
        dataSetCollectionMigration.setSourceVersion("1");
        Map<String, String> placeholders = new HashMap<>();
        placeholders.put("new_first_name_column_name", "firstname");
        placeholders.put("new_last_name_column_name", "lastname");
        dataSetCollectionMigration.setPlaceholders(placeholders);
        TableOrder tableOrder = new DefaultTableOrder("language", "film", "actor", "film_actor");
        ExternalSortTableConsumer externalSortTableConsumer = new ExternalSortTableConsumer(tableOrder);
        dataSetCollectionMigration.setBeforeMigration(new DataSetConsumerPipeTransformerAdapter(externalSortTableConsumer));


        DataSetCollectionMigrationResult result = dataSetCollectionMigration.exec();

        assertDataSetsMigratedSuccessfully(dataSetCollectionMigration, result);
    }

    private void assertDataSetsMigratedSuccessfully(DataSetCollectionFlywayMigration dataSetCollectionMigration, DataSetCollectionMigrationResult result) throws DataSetException {
        Map<Path, Path> migratedPaths = result.getMigratedPaths();

        assertEquals(3, migratedPaths.size(), "migrated paths");

        for (Path migratedPath : migratedPaths.values()) {
            DataSetFileDetection dataSetFileDetection = dataSetCollectionMigration.getDataSetFileDetection();
            DataSetFile dataSetFile = dataSetFileDetection.detect(migratedPath);

            IDataSetProducer producer = dataSetFile.createProducer();
            CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();
            producer.setConsumer(copyDataSetConsumer);
            producer.produce();

            IDataSet dataSet = copyDataSetConsumer.getDataSet();
            assertNotNull(dataSet);

            ITable actorTable = dataSet.getTable("actor");
            assertNotNull(actorTable);
            assertEquals(2, actorTable.getRowCount());

            ITable languageTable = dataSet.getTable("language");
            assertNotNull(languageTable);
            assertEquals(1, languageTable.getRowCount());

            ITable filmTable = dataSet.getTable("film");
            assertNotNull(filmTable);
            assertEquals(44, filmTable.getRowCount());

            ITable filmDescription = dataSet.getTable("film_description");
            assertNotNull(filmDescription);
            assertEquals(44, filmDescription.getRowCount());

            ITable filmActorTable = dataSet.getTable("film_actor");
            assertNotNull(filmActorTable);
            assertEquals(44, filmActorTable.getRowCount());
        }
    }
}