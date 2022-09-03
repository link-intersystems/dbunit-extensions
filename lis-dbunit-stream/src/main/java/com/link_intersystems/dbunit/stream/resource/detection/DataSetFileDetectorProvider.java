package com.link_intersystems.dbunit.stream.resource.detection;

import com.link_intersystems.util.config.properties.ConfigProperties;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetFileDetectorProvider {

    public DataSetFileDetector getDataSetFileDetector(ConfigProperties configProperties);
}
