package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
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
    public void successfullyMigrated(DataSetResource dataSetResource) {
        String msg = "\u2714\ufe0e Migrated '{}'";
        logger.info(msg, dataSetResource);
    }

    @Override
    public void failedMigration(DataSetResource dataSetResource, DataSetException e) {
        String msg = "\u274c\ufe0e Unable to migrate '{}'";
        if (logger.isDebugEnabled()) {
            logger.error(msg, dataSetResource, e);
        } else {
            logger.error(msg, dataSetResource);
        }
    }

    @Override
    public void startMigration(DataSetResource dataSetResource) {
        logger.info("\u267b\ufe0e Start migration '{}'", dataSetResource);
    }

    @Override
    public void resourcesSupplied(List<DataSetResource> dataSetResources) {
        logger.info("Found {} data set resources to migrate", dataSetResources.size());
        if (logger.isDebugEnabled()) {
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                pw.println("Files matching:");

                Iterator<DataSetResource> iterator = dataSetResources.iterator();
                while (iterator.hasNext()) {
                    DataSetResource dataSetResource = iterator.next();
                    pw.print("\t\u2022 ");
                    pw.print(dataSetResource);

                    if (iterator.hasNext()) {
                        pw.println();
                    }
                }
            }

            logger.debug(sw.toString());
        }
    }

    @Override
    public void dataSetCollectionMigrationFinished(Map<DataSetResource, DataSetResource> migratedDataSetResources) {
        logger.info("Migrated {} files ", migratedDataSetResources.size());
        if (logger.isDebugEnabled()) {
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                pw.println("Migrated files:");

                Set<Map.Entry<DataSetResource, DataSetResource>> entries = migratedDataSetResources.entrySet();
                Iterator<Map.Entry<DataSetResource, DataSetResource>> iterator = entries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<DataSetResource, DataSetResource> entry = iterator.next();
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
