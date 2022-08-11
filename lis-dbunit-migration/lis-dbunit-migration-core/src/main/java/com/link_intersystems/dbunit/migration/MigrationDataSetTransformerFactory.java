package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.consumer.DataSetTransormer;
import com.link_intersystems.dbunit.stream.consumer.DatabaseMigrationSupport;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface MigrationDataSetTransformerFactory {
    DataSetTransormer createTransformer(DatabaseMigrationSupport databaseMigrationSupport);
}
