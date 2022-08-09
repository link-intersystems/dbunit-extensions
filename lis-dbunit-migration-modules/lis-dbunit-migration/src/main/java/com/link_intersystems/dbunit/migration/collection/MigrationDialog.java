package com.link_intersystems.dbunit.migration.collection;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;

import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface MigrationDialog {

    default boolean aboutToStartMigration(List<DataSetResource> sourceDataSetResources) {
        return true;
    }
}
