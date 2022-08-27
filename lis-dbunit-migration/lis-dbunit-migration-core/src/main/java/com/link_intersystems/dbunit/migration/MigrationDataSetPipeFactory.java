package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.stream.consumer.DataSetConsumerPipe;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface MigrationDataSetPipeFactory {
    DataSetConsumerPipe createMigrationPipe(DatabaseMigrationSupport databaseMigrationSupport);
}
