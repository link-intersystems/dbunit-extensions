package com.link_intersystems.dbunit.stream.resource.detection.file.csv;

import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetector;
import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetectorProvider;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileConfig;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CsvDataSetDetectorProvider implements DataSetFileDetectorProvider {
    @Override
    public DataSetFileDetector getDataSetFileDetector(DataSetFileConfig dataSetFileConfig) {
        return new CsvDataSetDetector();
    }
}
