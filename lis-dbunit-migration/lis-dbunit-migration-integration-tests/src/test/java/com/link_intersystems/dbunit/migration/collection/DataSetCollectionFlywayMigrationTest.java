package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.dbunit.migration.FlywayConfigurationConfigFixture;
import com.link_intersystems.dbunit.migration.flyway.FlywayDatabaseMigrationSupport;
import com.link_intersystems.dbunit.migration.flyway.FlywayMigrationConfig;
import com.link_intersystems.dbunit.migration.resources.DataSetResourcesMigration;
import com.link_intersystems.dbunit.migration.resources.MigrationsResult;
import com.link_intersystems.dbunit.migration.resources.RebaseTargetpathDataSetResourceSupplier;
import com.link_intersystems.dbunit.migration.testcontainers.TestcontainersMigrationDataSetTransformerFactory;
import com.link_intersystems.dbunit.stream.consumer.CopyDataSetConsumer;
import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipeTransformerAdapter;
import com.link_intersystems.dbunit.stream.consumer.ExternalSortTableConsumer;
import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetection;
import com.link_intersystems.dbunit.stream.resource.detection.DetectingDataSetFileResourcesSupplier;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileLocationsScanner;
import com.link_intersystems.dbunit.table.DefaultTableOrder;
import com.link_intersystems.dbunit.table.TableOrder;
import com.link_intersystems.dbunit.test.TinySakilaDataSetFiles;
import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.commons.CommonsRunningContainerPool;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetCollectionFlywayMigrationTest {

    private Path sourcePath;
    private Path targetPath;

    private DataSetResourcesMigration dataSetResourcesMigration;
    private DetectingDataSetFileResourcesSupplier fileResourcesSupplier;

    @BeforeEach
    void setUp(@TempDir Path tmpDir) {
        this.sourcePath = Paths.get(tmpDir.toString(), "source");
        this.targetPath = Paths.get(tmpDir.toString(), "target/someSubdir");
        TinySakilaDataSetFiles.create(sourcePath);
        dataSetResourcesMigration = new DataSetResourcesMigration();

        RebaseTargetpathDataSetResourceSupplier basepathTargetPathSupplier = new RebaseTargetpathDataSetResourceSupplier(sourcePath, targetPath);
        dataSetResourcesMigration.setTargetDataSetResourceSupplier(basepathTargetPathSupplier);

        DataSetFileLocationsScanner fileLocationsScanner = new DataSetFileLocationsScanner(sourcePath);
        fileResourcesSupplier = new DetectingDataSetFileResourcesSupplier(fileLocationsScanner, new DataSetFileDetection());
    }

    @Test
    void migrateDataSetCollection() throws DataSetException {
        List<DataSetResource> dataSetResources = fileResourcesSupplier.getDataSetResources();

        CommonsRunningContainerPool runningContainerPool = CommonsRunningContainerPool.createPool(() -> {
            DatabaseContainerSupport databaseContainerSupport = DatabaseContainerSupport.getDatabaseContainerSupport("postgres:latest");
            return new DBunitJdbcContainer(databaseContainerSupport.create(), databaseContainerSupport.getDatabaseConfig());
        }, dataSetResources.size());

        TestcontainersMigrationDataSetTransformerFactory migrationDataSetTransformerFactory = new TestcontainersMigrationDataSetTransformerFactory(runningContainerPool);
        dataSetResourcesMigration.setMigrationDataSetTransformerFactory(migrationDataSetTransformerFactory);

        FlywayMigrationConfig migrationConfig = FlywayConfigurationConfigFixture.createPostgresConfig();

        dataSetResourcesMigration.setDatabaseMigrationSupport(new FlywayDatabaseMigrationSupport(migrationConfig));


        dataSetResourcesMigration.setBeforeMigrationSupplier(() -> {
            TableOrder tableOrder = new DefaultTableOrder("language", "film", "actor", "film_actor");
            return new DataSetConsumerPipeTransformerAdapter(new ExternalSortTableConsumer(tableOrder));
        });

        MigrationsResult result = dataSetResourcesMigration.exec(dataSetResources);

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