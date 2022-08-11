package com.link_intersystems.dbunit.migration.resources;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import org.dbunit.dataset.DataSetException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TargetDataSetResourceSupplier {
    DataSetResource getTargetDataSetResource(DataSetResource dataSetResource) throws DataSetException;
}
