package com.link_intersystems.dbunit.stream.resource.detection.file.xml;

import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetector;
import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetectorProvider;
import com.link_intersystems.dbunit.stream.resource.file.xml.FlatXmlDataSetFileConfig;
import com.link_intersystems.util.config.properties.ConfigProperties;
import com.link_intersystems.util.config.properties.ConfigPropertiesProxy;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FlatXmlDataSetDetectorProvider implements DataSetFileDetectorProvider {
    @Override
    public DataSetFileDetector getDataSetFileDetector(ConfigProperties configProperties) {
        FlatXmlDataSetFileConfig flatXmlDataSetFileConfig = ConfigPropertiesProxy.create(configProperties, FlatXmlDataSetFileConfig.class);
        return new FlatXmlDataSetDetector(flatXmlDataSetFileConfig);
    }
}
