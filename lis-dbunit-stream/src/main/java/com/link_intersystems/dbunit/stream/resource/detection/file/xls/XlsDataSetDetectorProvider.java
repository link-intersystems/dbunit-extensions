package com.link_intersystems.dbunit.stream.resource.detection.file.xls;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFileConfig;
import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetector;
import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetectorProvider;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XlsDataSetDetectorProvider implements DataSetFileDetectorProvider {
    @Override
    public DataSetFileDetector getDataSetFileDetector(DataSetFileConfig dataSetFileConfig) {
        return new XlsDataSetDetector();
    }
}
