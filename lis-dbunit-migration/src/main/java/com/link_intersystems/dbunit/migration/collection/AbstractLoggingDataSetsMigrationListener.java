package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import org.dbunit.dataset.DataSetException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.text.MessageFormat.format;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractLoggingDataSetsMigrationListener implements DataSetsMigrationListener {
    @Override
    public void migrationSuccessful(DataSetResource dataSetResource) {
        String msg = format("\u2714\ufe0e Migrated ''{0}''", dataSetResource);
        logMigrationSuccessful(msg);
    }

    protected abstract void logMigrationSuccessful(String msg);

    @Override
    public void migrationFailed(DataSetResource dataSetResource, DataSetException e) {
        String msg = format("\u274c\ufe0e Unable to migrate ''{0}''", dataSetResource);
        logMigrationFailed(e, msg);
    }

    protected abstract void logMigrationFailed(DataSetException e, String msg);

    @Override
    public void startMigration(DataSetResource dataSetResource) {
        String msg = format("\u267b\ufe0e Start migration ''{0}''", dataSetResource);
        logStartMigration(msg);
    }

    protected abstract void logStartMigration(String msg);

    @Override
    public void resourcesSupplied(List<DataSetResource> dataSetResources) {
        String msg = format("Detected {0} data set resources to migrate", dataSetResources.size());
        logResourcesSupplied(msg, () -> {
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                pw.println("Matching data sets:");

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
            return sw.toString();
        });
    }

    protected abstract void logResourcesSupplied(String msg, Supplier<String> details);

    @Override
    public void migrationsFinished(MigrationsResult migrationsResult) {
        String msg = format("Migrated {0} data set resources ", migrationsResult.size());
        logMigrationsFinished(msg, () -> {
            StringWriter sw = new StringWriter();
            try (PrintWriter pw = new PrintWriter(sw)) {
                pw.println("Migrated data set resources:");

                Set<Map.Entry<DataSetResource, DataSetResource>> entries = migrationsResult.entrySet();
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
            return sw.toString();
        });

    }

    protected abstract void logMigrationsFinished(String msg, Supplier<String> details);
}
