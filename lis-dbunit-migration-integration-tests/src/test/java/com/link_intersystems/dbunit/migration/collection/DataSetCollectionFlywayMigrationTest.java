package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.dbunit.migration.flyway.FlywayDatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.flyway.FlywayMigrationConfig;
import com.link_intersystems.dbunit.migration.FlywayConfigurationConfigFixture;
import com.link_intersystems.dbunit.migration.testcontainers.TestcontainersMigrationDataSetTransformerFactory;
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
import com.link_intersystems.dbunit.test.TinySakilaDataSetFiles;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetCollectionFlywayMigrationTest {

    private Path sourcePath;
    private Path targetPath;

    private DataSetsMigrations dataSetsMigrations;
    private DataSetFileLocationsScanner fileLocationsScanner;

    @BeforeEach
    void setUp(@TempDir Path tmpDir) {
        this.sourcePath = Paths.get(tmpDir.toString(), "source");
        this.targetPath = Paths.get(tmpDir.toString(), "target/someSubdir");
        TinySakilaDataSetFiles.create(sourcePath);
        dataSetsMigrations = new DataSetsMigrations();

        BasepathTargetPathSupplier basepathTargetPathSupplier = new BasepathTargetPathSupplier(sourcePath, targetPath);
        dataSetsMigrations.setTargetDataSetResourceSupplier(basepathTargetPathSupplier);

        fileLocationsScanner = new DataSetFileLocationsScanner(sourcePath);
        dataSetsMigrations.setDataSetResourcesSupplier(new DefaultDataSetResourcesSupplier(fileLocationsScanner, new DataSetFileDetection()));
    }

    @Test
    void migrateDataSetCollection() throws DataSetException {
        dataSetsMigrations.setMigrationDataSetTransformerFactory(new TestcontainersMigrationDataSetTransformerFactory("postgres:latest"));

        FlywayMigrationConfig migrationConfig = FlywayConfigurationConfigFixture.createPostgresConfig();

        dataSetsMigrations.setDatabaseMigrationSupport(new FlywayDatabaseMigrationSupport(migrationConfig));

        TableOrder tableOrder = new DefaultTableOrder("language", "film", "actor", "film_actor");
        ExternalSortTableConsumer externalSortTableConsumer = new ExternalSortTableConsumer(tableOrder);
        dataSetsMigrations.setBeforeMigration(new DataSetConsumerPipeTransformerAdapter(externalSortTableConsumer));

        MigrationsResult result = dataSetsMigrations.exec();

        assertDataSetsMigratedSuccessfully(result);
    }


    private void assertDataSetsMigratedSuccessfully(MigrationsResult result) throws DataSetException {
        assertEquals(3, result.size(), "migrated paths");

        for (DataSetResource migratedDataSetResource : result.values()) {
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