package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.flyway.AbstractFlywayConfigurationSupport;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetection;
import com.link_intersystems.dbunit.testcontainers.consumer.DatabaseContainerFactory;
import com.link_intersystems.io.FileScanner;
import com.link_intersystems.io.PathMatch;
import com.link_intersystems.io.PathMatches;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.io.File;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionFlywayMigration extends AbstractFlywayConfigurationSupport {

    private DataSetCollectionFlywayMigrationLogger logger = new DataSetCollectionFlywayMigrationLogger();

    private FileScanner fileScanner;

    private DataSetFileDetection dataSetFileDetection = new DataSetFileDetection();
    private DatabaseContainerFactory databaseContainerFactory;
    private DataSetTransormer beforeMigrationTransformer;
    private DataSetTransormer afterMigrationTransformer;
    private TargetPathSupplier targetPathSupplier = new BasepathTargetPathSupplier();

    public DataSetCollectionFlywayMigration(File basedir) {
        this(basedir.toPath());
    }

    public DataSetCollectionFlywayMigration(Path basepath) {
        fileScanner = new FileScanner(basepath);
    }

    public void setBeforeMigration(DataSetTransormer beforeMigrationTransformer) {
        this.beforeMigrationTransformer = beforeMigrationTransformer;
    }

    public void setAfterMigrationTransformer(DataSetTransormer afterMigrationTransformer) {
        this.afterMigrationTransformer = afterMigrationTransformer;
    }

    public void addDefaultFilePatterns() {
        addFilePatterns("**/*.xml", "*.xml", "**/*.xls", "*.xls");
        addDirectoryPatterns("**");
    }

    public void addFilePatterns(String... globPattern) {
        fileScanner.addFilePattern(globPattern);
    }

    public void addDirectoryPatterns(String... globPattern) {
        fileScanner.addDirectoryPatterns(globPattern);
    }

    public void setDataSetFileDetection(DataSetFileDetection dataSetFileDetection) {
        this.dataSetFileDetection = dataSetFileDetection;
    }

    public void setTargetPathSupplier(TargetPathSupplier targetPathSupplier) {
        this.targetPathSupplier = requireNonNull(targetPathSupplier);
    }

    public void setDatabaseContainerFactory(DatabaseContainerFactory databaseContainerFactory) {
        this.databaseContainerFactory = databaseContainerFactory;
    }

    public void exec() throws DataSetException {
        PathMatches dataSetMatches = fileScanner.scan();

        logger.logDataSetMatches(dataSetMatches);

        Map<Path, Path> migratedPaths = new LinkedHashMap<>();

        for (PathMatch dataSetMatch : dataSetMatches) {
            Path sourcePathAbsolute = dataSetMatch.getAbsolutePath();
            DataSetFile sourceDataSetFile = dataSetFileDetection.detect(sourcePathAbsolute);

            if (sourceDataSetFile == null) {
                continue;
            }

            DataSetFlywayMigration flywayMigration = new DataSetFlywayMigration();
            flywayMigration.setDataSetProducer(sourceDataSetFile.createProducer());
            flywayMigration.setDatabaseContainerFactory(databaseContainerFactory);
            flywayMigration.apply(this);
            flywayMigration.setBeforeMigrationTransformer(beforeMigrationTransformer);
            flywayMigration.setAfterMigrationTransformer(afterMigrationTransformer);


            Path targetDataSetPath = targetPathSupplier.getTarget(dataSetMatch.getPath());
            DataSetFile targetDataSetFile = sourceDataSetFile.withNewPath(targetDataSetPath);

            IDataSetConsumer targetDataSetFileConsumer = targetDataSetFile.createConsumer();
            flywayMigration.setDataSetConsumer(targetDataSetFileConsumer);

            logger.logStartMigration(sourceDataSetFile);
            try {
                flywayMigration.exec();
                migratedPaths.put(sourcePathAbsolute, targetDataSetPath);
                logger.logMigrated(targetDataSetFile);
            } catch (DataSetException e) {
                logger.logMigrationError(sourceDataSetFile, e);
            }
        }

        logger.logMigrationFinished(migratedPaths);
    }

}
