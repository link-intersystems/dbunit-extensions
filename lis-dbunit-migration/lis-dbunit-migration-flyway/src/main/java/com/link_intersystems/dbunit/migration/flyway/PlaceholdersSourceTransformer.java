package com.link_intersystems.dbunit.migration.flyway;

import com.link_intersystems.dbunit.migration.DataSourceProperties;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface PlaceholdersSourceTransformer {

    PlaceholdersSource transform(PlaceholdersSource placeholdersSource, DataSourceProperties dataSourceProperties);
}
