package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.io.FilePath;
import org.dbunit.dataset.DataSetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class LoggingDataSetCollectionMigrationListener implements DataSetCollectionMigrationListener {

    private Logger logger;

    public LoggingDataSetCollectionMigrationListener() {
        this(LoggerFactory.getLogger(LoggingDataSetCollectionMigrationListener.class));

    }

    public LoggingDataSetCollectionMigrationListener(Logger logger) {
        this.logger = requireNonNull(logger);
    }

    @Override
    public void successfullyMigrated(DataSetFile dataSetFile) {
        String msg = "\u2714\ufe0e Migrated '{}'";
        logger.info(msg, dataSetFile);
    }

    @Override
    public void failedMigration(DataSetFile dataSetFile, DataSetException e) {
        String msg = "\u274c\ufe0e Unable to migrate '{}'";
        if (logger.isDebugEnabled()) {
            logger.error(msg, dataSetFile, e);
        } else {
            logger.error(msg, dataSetFile);
        }
    }

    @Override
    public void skippedMigrationTypeNotDetectable(FilePath path) {
        String msg = "\u26A1\ufe0e Not a dataset file '{}'";
        logger.error(msg, path);
    }

    @Override
    public void startMigration(DataSetFile dataSetFile) {
        logger.info("\u267b\ufe0e Start migration '{}'", dataSetFile);
    }

    @Override
    public void pathScanned(List<FilePath> dataSetMatches) {
        logger.info("Found {} files matching the file pattern", dataSetMatches.size());
        if (logger.isDebugEnabled()) {
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                pw.println("Files matching:");

                Iterator<FilePath> iterator = dataSetMatches.iterator();
                while (iterator.hasNext()) {
                    FilePath dataSetMatch = iterator.next();
                    pw.print("\t\u2022 ");
                    pw.print(dataSetMatch.toAbsolutePath());

                    if (iterator.hasNext()) {
                        pw.println();
                    }
                }
            }

            logger.debug(sw.toString());
        }
    }

    @Override
    public void dataSetCollectionMigrationFinished(Map<DataSetFile, DataSetFile> migratedDataSetFiles) {
        logger.info("Migrated {} files ", migratedDataSetFiles.size());
        if (logger.isDebugEnabled()) {
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                pw.println("Migrated files:");

                Set<Map.Entry<DataSetFile, DataSetFile>> entries = migratedDataSetFiles.entrySet();
                Iterator<Map.Entry<DataSetFile, DataSetFile>> iterator = entries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<DataSetFile, DataSetFile> entry = iterator.next();
                    pw.print("\t\u2022 ");
                    pw.println(entry.getKey());
                    pw.print("\t\t\u2192 ");
                    pw.print(entry.getValue());

                    if (iterator.hasNext()) {
                        pw.println();
                    }
                }
            }

            logger.debug(sw.toString());
        }
    }
}
