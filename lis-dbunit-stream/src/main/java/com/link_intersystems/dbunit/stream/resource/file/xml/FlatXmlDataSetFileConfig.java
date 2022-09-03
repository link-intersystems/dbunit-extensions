package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFileConfig;
import com.link_intersystems.util.config.properties.ConfigProperty;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface FlatXmlDataSetFileConfig extends DataSetFileConfig {

    public static final ConfigProperty<Boolean> COLUMN_SENSING = ConfigProperty.named("columnSensing").withDefaultValue(false);

    public boolean isColumnSensing();

}
