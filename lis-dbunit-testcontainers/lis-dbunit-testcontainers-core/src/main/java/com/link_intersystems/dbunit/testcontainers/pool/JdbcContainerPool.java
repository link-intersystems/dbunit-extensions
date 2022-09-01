package com.link_intersystems.dbunit.testcontainers.pool;

import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.dataset.DataSetException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface JdbcContainerPool extends AutoCloseable {

    public JdbcContainer borrowContainer() throws DataSetException;

    public void returnContainer(JdbcContainer jdbcContainer);
}
