package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipeTransformerAdapter;
import com.link_intersystems.dbunit.stream.consumer.ExternalSortTableConsumer;
import com.link_intersystems.dbunit.table.DefaultTableOrder;
import com.link_intersystems.dbunit.table.TableOrder;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseContainerSupportFactory;
import com.link_intersystems.io.Unzip;
import org.dbunit.dataset.DataSetException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

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
    void applyDataSetMigration() throws DataSetException {
        DataSetCollectionFlywayMigration dataSetCollectionMigration = new DataSetCollectionFlywayMigration(sourcePath);

        dataSetCollectionMigration.addDefaultFilePatterns();
        dataSetCollectionMigration.setDatabaseContainerSupport(DatabaseContainerSupportFactory.forPostgres("postgres:latest"));
        dataSetCollectionMigration.setLocations("com/link_intersystems/dbunit/migration/postgres");
        dataSetCollectionMigration.setTargetPathSupplier(new BasepathTargetPathSupplier(targetPath));
        dataSetCollectionMigration.setSourceVersion("1");
        TableOrder tableOrder = new DefaultTableOrder("language", "film", "actor", "film_actor");
        ExternalSortTableConsumer externalSortTableConsumer = new ExternalSortTableConsumer(tableOrder);
        dataSetCollectionMigration.setBeforeMigration(new DataSetConsumerPipeTransformerAdapter(externalSortTableConsumer));


        dataSetCollectionMigration.exec();
    }
}