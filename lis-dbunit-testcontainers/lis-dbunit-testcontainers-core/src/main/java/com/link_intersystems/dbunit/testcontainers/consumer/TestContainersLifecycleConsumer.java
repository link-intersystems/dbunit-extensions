package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DefaultChainableDataSetConsumer;
import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import com.link_intersystems.dbunit.testcontainers.pool.JdbcContainerPool;
import com.link_intersystems.dbunit.testcontainers.pool.SingleJdbcContainerPool;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestContainersLifecycleConsumer extends DefaultChainableDataSetConsumer {
    private JdbcContainerPool jdbcContainerPool;
    private JdbcContainer jdbcContainer;

    public TestContainersLifecycleConsumer(DatabaseContainerSupport databaseContainerSupport) {
        this(new DBunitJdbcContainer(databaseContainerSupport.create(), databaseContainerSupport.getDatabaseConfig()));
    }

    public TestContainersLifecycleConsumer(DBunitJdbcContainer dBunitJdbcContainer) {
        this(new SingleJdbcContainerPool(dBunitJdbcContainer));
    }

    public TestContainersLifecycleConsumer(JdbcContainerPool jdbcContainerPool) {
        this.jdbcContainerPool = requireNonNull(jdbcContainerPool);
    }

    @Override
    public void startDataSet() throws DataSetException {
        jdbcContainer = jdbcContainerPool.borrowContainer();
        JdbcContainerHolder.set(jdbcContainer);

        super.startDataSet();
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            super.endDataSet();
        } finally {
            JdbcContainerHolder.remove();
            jdbcContainerPool.returnContainer(jdbcContainer);
        }
    }
}
