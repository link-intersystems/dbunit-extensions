package com.link_intersystems.dbunit.stream.resource.detection.file.xls;

import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetector;
import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetectorProvider;
import com.link_intersystems.dbunit.stream.resource.detection.Order;
import com.link_intersystems.util.config.properties.ConfigProperties;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@Order
public class XlsDataSetDetectorProvider implements DataSetFileDetectorProvider {
    @Override
    public DataSetFileDetector getDataSetFileDetector(ConfigProperties configProperties) {
        return new XlsDataSetDetector();
    }
}
