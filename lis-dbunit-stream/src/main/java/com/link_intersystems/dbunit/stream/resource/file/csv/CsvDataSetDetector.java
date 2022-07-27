package com.link_intersystems.dbunit.stream.resource.file.csv;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetector;
import com.link_intersystems.io.FilePath;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CsvDataSetDetector implements DataSetFileDetector {

    public static final String TABLE_ORDERING_TXT = "table-ordering.txt";
    public static final String CSV_EXTENSION = ".csv";

    @Override
    public DataSetFile detect(FilePath filePath) {
        File file = filePath.toAbsoluteFile();
        FilePath effectivePath = filePath;

        if (file.isFile()) {
            if (isCsvDataSetFile(file)) {
                effectivePath = new FilePath(filePath.getBasepath(), filePath.getPath().getParent());
            } else {
                return null;
            }
        }

        File effectiveDir = effectivePath.toAbsoluteFile();
        if (isTableOrderingTxtExistent(effectiveDir) && hasCsvFiles(effectiveDir)) {
            return new CsvDataSetFile(effectivePath);
        }

        return null;
    }

    private boolean isCsvDataSetFile(File file) {
        if (file.getName().endsWith(CSV_EXTENSION)) {
            File csvDir = file.getParentFile();
            return isTableOrderingTxtExistent(csvDir);
        }

        return file.getName().equals(TABLE_ORDERING_TXT);
    }

    private boolean hasCsvFiles(File file) {
        return file.listFiles(f -> f.getName().endsWith(CSV_EXTENSION)).length > 0;
    }

    private boolean isTableOrderingTxtExistent(File file) {
        return new File(file, TABLE_ORDERING_TXT).exists();
    }
}
