package com.link_intersystems.dbunit.stream.resource.file.csv;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFileConfig;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetector;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetectorProvider;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CsvDataSetDetectorProvider implements DataSetFileDetectorProvider {
    @Override
    public DataSetFileDetector getDataSetFileDetector(DataSetFileConfig dataSetFileConfig) {
        return new CsvDataSetDetector();
    }
}
