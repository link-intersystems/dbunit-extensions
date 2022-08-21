package com.link_intersystems.dbunit.testcontainers.consumer;

import javax.sql.DataSource;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class NullDataSourceConsumer implements DataSourceConsumer {
    public static final DataSourceConsumer INSTANCE = new NullDataSourceConsumer();

    @Override
    public void consume(DataSource dataSource) {
    }
}
