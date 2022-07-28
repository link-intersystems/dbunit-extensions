package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import com.link_intersystems.io.FilePath;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class MigrationDescription {

    private DataSetResource dataSetResource;

    public MigrationDescription(DataSetResource dataSetResource) {
        this.dataSetResource = dataSetResource;
    }

    public DataSetResource getDataSetResource() {
        return dataSetResource;
    }

}
