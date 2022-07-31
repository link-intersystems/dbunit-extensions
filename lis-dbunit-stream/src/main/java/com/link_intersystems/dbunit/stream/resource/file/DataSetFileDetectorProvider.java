package com.link_intersystems.dbunit.stream.resource.file;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetFileDetectorProvider {

    public DataSetFileDetector getDataSetFileDetector(DataSetFileConfig detectionConfig);
}
