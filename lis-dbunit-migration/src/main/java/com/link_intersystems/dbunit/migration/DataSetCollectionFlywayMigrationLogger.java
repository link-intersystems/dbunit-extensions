package com.link_intersystems.dbunit.migration;

import com.link_intersystems.io.FilePath;
import org.dbunit.dataset.DataSetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetCollectionFlywayMigrationLogger implements DataSetCollectionMigrationListener {

    private Logger logger = LoggerFactory.getLogger(DataSetCollectionFlywayMigration.class);

    @Override
    public void successfullyMigrated(Path path) {
        String msg = "\u2714\ufe0e Migrated '{}'";
        logger.info(msg, path);
    }

    @Override
    public void failedMigration(Path path, DataSetException e) {
        String msg = "\u274c\ufe0e Unable to migrate '{}'";
        if (logger.isDebugEnabled()) {
            logger.error(msg, path, e);
        } else {
            logger.error(msg, path);
        }
    }

    @Override
    public void skippedMigrationTypeNotDetectable(Path path) {
        String msg = "\u26A1\ufe0e Not a dataset file '{}'";
        logger.error(msg, path);
    }

    @Override
    public void startMigration(Path path) {
        logger.info("\u267b\ufe0e Start migration '{}'", path);
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
                    pw.print(dataSetMatch.getAbsolutePath());

                    if (iterator.hasNext()) {
                        pw.println();
                    }
                }
            }

            logger.debug(sw.toString());
        }
    }

    @Override
    public void dataSetCollectionMigrationFinished(Map<Path, Path> fromToPathMap) {
        logger.info("Migrated {} files ", fromToPathMap.size());
        if (logger.isDebugEnabled()) {
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                pw.println("Migrated files:");

                Set<Map.Entry<Path, Path>> entries = fromToPathMap.entrySet();
                Iterator<Map.Entry<Path, Path>> iterator = entries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<Path, Path> entry = iterator.next();
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
