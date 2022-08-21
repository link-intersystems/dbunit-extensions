package com.link_intersystems.dbunit.testcontainers.consumer;

import org.dbunit.dataset.DataSetException;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@FunctionalInterface
public interface DataSourceConsumer {

    public void consume(DataSource dataSource) throws SQLException;
}
