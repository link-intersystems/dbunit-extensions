package com.link_intersystems.dbunit.stream.resource.file.csv;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetector;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CsvDataSetDetector implements DataSetFileDetector {
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
