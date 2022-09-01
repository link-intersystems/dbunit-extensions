package com.link_intersystems.dbunit.migration.testcontainers;

import com.link_intersystems.dbunit.stream.consumer.ChainableDataSetConsumer;
import com.link_intersystems.dbunit.table.IRowFilterFactory;
import com.link_intersystems.dbunit.testcontainers.consumer.ExistingEntriesConsumerRowFilterFactory;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SkipExistingDatabaseEntitiesMigrationPipeCustomization implements MigrationPipeCustomization {

    private ExistingEntriesConsumerRowFilterFactory existingEntriesConsumerRowFilterFactory = new ExistingEntriesConsumerRowFilterFactory();

    @Override
    public IRowFilterFactory getMigratedDataSetRowFilterFactory() {
        return existingEntriesConsumerRowFilterFactory;
    }

    @Override
    public ChainableDataSetConsumer getAfterMigrationConsumerConsumer() {
        return existingEntriesConsumerRowFilterFactory;
    }
}
