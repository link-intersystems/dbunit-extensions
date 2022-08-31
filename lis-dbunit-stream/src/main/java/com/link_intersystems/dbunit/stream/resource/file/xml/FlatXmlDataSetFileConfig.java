package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFileConfig;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface FlatXmlDataSetFileConfig {

    public static final DataSetFileConfig.ConfigProperty<Boolean> COLUMN_SENSING_PROPERTY = new DataSetFileConfig.ConfigProperty<>(Boolean.class, false);

}
