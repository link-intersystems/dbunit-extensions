package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.dbunit.flyway.FlywayMigrationConfig;
import com.link_intersystems.dbunit.migration.FlywayConfigurationConfigFixture;
import com.link_intersystems.dbunit.migration.resources.BasepathTargetPathSupplier;
import com.link_intersystems.dbunit.migration.resources.DataSetFileLocationsScanner;
import com.link_intersystems.dbunit.migration.resources.DefaultDataSetResourcesSupplier;
import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipeTransformerAdapter;
import com.link_intersystems.dbunit.stream.consumer.ExternalSortTableConsumer;
import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetection;
import com.link_intersystems.dbunit.table.DefaultTableOrder;
import com.link_intersystems.dbunit.table.TableOrder;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupportFactory;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetCollectionFlywayMigrationTest {

    private Path sourcePath;
    private Path targetPath;

    private DataSetCollectionFlywayMigration dataSetCollectionMigration;
    private DataSetFileLocationsScanner fileLocationsScanner;

    @BeforeEach
    void setUp(@TempDir Path tmpDir) throws IOException {
        this.sourcePath = Paths.get(tmpDir.toString(), "source");
        this.targetPath = Paths.get(tmpDir.toString(), "target/someSubdir");
        Unzip.unzip(DataSetCollectionFlywayMigration.class.getResourceAsStream("/tiny-sakila-dataset-files.zip"), sourcePath);
        BasepathTargetPathSupplier basepathTargetPathSupplier = new BasepathTargetPathSupplier(sourcePath, targetPath);
        dataSetCollectionMigration = new DataSetCollectionFlywayMigration();
        dataSetCollectionMigration.setTargetDataSetFileSupplier(basepathTargetPathSupplier);
        fileLocationsScanner = new DataSetFileLocationsScanner(sourcePath);
        dataSetCollectionMigration.setDataSetResourcesSupplier(new DefaultDataSetResourcesSupplier(fileLocationsScanner, new DataSetFileDetection()));
    }

    @Test
    void migrateDataSetCollection() throws DataSetException {
        fileLocationsScanner.addDefaultFilePatterns();

        dataSetCollectionMigration.setDatabaseContainerSupport(DatabaseContainerSupportFactory.INSTANCE.createPostgres("postgres:latest"));

        FlywayMigrationConfig migrationConfig = FlywayConfigurationConfigFixture.createPostgresConfig();

        dataSetCollectionMigration.setMigrationConfig(migrationConfig);

        TableOrder tableOrder = new DefaultTableOrder("language", "film", "actor", "film_actor");
        ExternalSortTableConsumer externalSortTableConsumer = new ExternalSortTableConsumer(tableOrder);
        dataSetCollectionMigration.setBeforeMigration(new DataSetConsumerPipeTransformerAdapter(externalSortTableConsumer));

        DataSetCollectionMigrationResult result = dataSetCollectionMigration.exec();

        assertDataSetsMigratedSuccessfully(dataSetCollectionMigration, result);
    }


    private void assertDataSetsMigratedSuccessfully(DataSetCollectionFlywayMigration dataSetCollectionMigration, DataSetCollectionMigrationResult result) throws DataSetException {
        Map<DataSetResource, DataSetResource> migratedDataSetResources = result.getMigratedDataSetResources();

        assertEquals(3, migratedDataSetResources.size(), "migrated paths");

        for (DataSetResource migratedDataSetResource : migratedDataSetResources.values()) {
            IDataSetProducer producer = migratedDataSetResource.createProducer();
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