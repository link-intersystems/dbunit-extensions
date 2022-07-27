package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import org.dbunit.dataset.DataSetException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface TargetDataSetFileSupplier {
    DataSetFile getTarget(DataSetFile dataSetFile) throws DataSetException;
}
