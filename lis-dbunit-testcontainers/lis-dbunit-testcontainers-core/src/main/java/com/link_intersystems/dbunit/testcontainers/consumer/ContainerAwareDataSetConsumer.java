package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface ContainerAwareDataSetConsumer extends IDataSetConsumer {
    void containerStarted(JdbcContainer jdbcContainer) throws DataSetException;

}
