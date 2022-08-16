package com.link_intersystems.dbunit.stream.resource.detection.file.xml;

import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetector;
import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetectorProvider;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileConfig;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XmlDataSetDetectorProvider implements DataSetFileDetectorProvider {
    @Override
    public DataSetFileDetector getDataSetFileDetector(DataSetFileConfig dataSetFileConfig) {
        return new XmlDataSetDetector(dataSetFileConfig);
    }
}
