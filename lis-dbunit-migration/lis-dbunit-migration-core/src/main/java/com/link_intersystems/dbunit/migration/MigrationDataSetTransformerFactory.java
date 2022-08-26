package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface MigrationDataSetTransformerFactory {
    ChainableDataSetConsumer createTransformer(DatabaseMigrationSupport databaseMigrationSupport);
}
