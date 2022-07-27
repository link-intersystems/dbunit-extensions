package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.io.FilePath;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class MigrationDescription {

    private FilePath filePath;
    private DataSetFile dataSetFile;

    public MigrationDescription(FilePath filePath, DataSetFile dataSetFile) {
        this.filePath = filePath;
        this.dataSetFile = dataSetFile;
    }

    public DataSetFile getDataSetFile() {
        return dataSetFile;
    }

    public FilePath getFilePath() {
        return filePath;
    }
}
