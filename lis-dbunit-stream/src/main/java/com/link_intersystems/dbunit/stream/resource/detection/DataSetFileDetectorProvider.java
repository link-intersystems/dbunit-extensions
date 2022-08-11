package com.link_intersystems.dbunit.stream.resource.detection;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFileConfig;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetFileDetectorProvider {

    public DataSetFileDetector getDataSetFileDetector(DataSetFileConfig detectionConfig);
}
