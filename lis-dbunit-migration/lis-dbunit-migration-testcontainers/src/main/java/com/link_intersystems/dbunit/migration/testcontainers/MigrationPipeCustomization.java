package com.link_intersystems.dbunit.migration.testcontainers;

import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.table.IRowFilterFactory;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface MigrationPipeCustomization {
    IRowFilterFactory getMigratedDataSetRowFilterFactory();

    ChainableDataSetConsumer getAfterMigrationConsumerConsumer();
}
