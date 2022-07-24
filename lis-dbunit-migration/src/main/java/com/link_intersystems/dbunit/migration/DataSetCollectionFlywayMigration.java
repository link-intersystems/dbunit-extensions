package com.link_intersystems.dbunit.migration;

import com.github.dockerjava.api.model.Link;
import com.link_intersystems.dbunit.flyway.AbstractFlywayConfigurationSupport;
import com.link_intersystems.dbunit.migration.detection.DataSetFile;
import com.link_intersystems.dbunit.migration.detection.DataSetFileDetection;
import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
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
import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionFlywayMigration extends AbstractFlywayConfigurationSupport {

    private DataSetCollectionFlywayMigrationLogger logger = new DataSetCollectionFlywayMigrationLogger();

    private FileScanner fileScanner;

    private DataSetFileDetection dataSetFileDetection = new DataSetFileDetection();
    private Path targetPath;
    private DatabaseContainerFactory databaseContainerFactory;
    private DataSetTransormer beforeMigrationTransformer;
    private DataSetTransormer afterMigrationTransformer;

    public DataSetCollectionFlywayMigration(File basedir) {
        fileScanner = new FileScanner(basedir);
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
        this.dataSetFileDetection = Objects.requireNonNull(dataSetFileDetection);
    }

    public void setTargetPath(Path targetPath) {
        this.targetPath = targetPath;
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


            Path targetDataSetPath = targetPath.resolve(dataSetMatch.getPath());
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
