package com.link_intersystems.dbunit.migration.csv;

import com.link_intersystems.dbunit.migration.DataSetDetector;
import com.link_intersystems.dbunit.migration.DataSetFile;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CsvDataSetDetector implements DataSetDetector {
    @Override
    public DataSetFile detect(File file) {
        if (file.isFile()) {
            return null;
        }

        if (isTableOrderingTxtExistent(file) && hasCsvFiles(file)) {
            return new CsvDataSetFile(file);
        }

        return null;
    }

    private boolean hasCsvFiles(File file) {
        return file.listFiles(f -> f.getName().endsWith(".csv")).length > 0;
    }

    private boolean isTableOrderingTxtExistent(File file) {
        return new File(file, "table-ordering.txt").exists();
    }
}
