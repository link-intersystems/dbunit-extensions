package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.io.PathMatch;
import com.link_intersystems.io.PathMatches;
import org.dbunit.dataset.DataSetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetCollectionFlywayMigrationLogger {
    private Logger logger = LoggerFactory.getLogger(DataSetCollectionFlywayMigration.class);

    public void logMigrated(DataSetFile dataSetFile) {
        String msg = "\u2714\ufe0e Migrated '{}'";
        logger.info(msg, dataSetFile);
    }

    void logMigrationError(DataSetFile sourceDataSetFile, DataSetException e) {
        String msg = "\u274c\ufe0e Unable to migrate '{}'";
            logger.error(msg, sourceDataSetFile, e);
    }

    void logStartMigration(DataSetFile dataSetFile) {
        logger.info("\u267b\ufe0e Start migration '{}'", dataSetFile);
    }

    void logDataSetMatches(PathMatches dataSetMatches) {
        logger.info("Found {} files matching the file pattern", dataSetMatches.size());
        if (logger.isDebugEnabled()) {
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                pw.println("Files matching:");

                Iterator<PathMatch> iterator = dataSetMatches.iterator();
                while (iterator.hasNext()) {
                    PathMatch dataSetMatch = iterator.next();
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

    public void logMigrationFinished(Map<Path, Path> migratedPaths) {
        logger.info("Migrated {} files ", migratedPaths.size());
        if (logger.isDebugEnabled()) {
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                pw.println("Migrated files:");

                Set<Map.Entry<Path, Path>> entries = migratedPaths.entrySet();
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
