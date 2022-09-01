package com.link_intersystems.dbunit.testcontainers.pool;

import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.dataset.DataSetException;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SingleJdbcContainerPool implements JdbcContainerPool {

    private DBunitJdbcContainer dBunitJdbcContainer;

    public SingleJdbcContainerPool(DBunitJdbcContainer dBunitJdbcContainer) {
        this.dBunitJdbcContainer = requireNonNull(dBunitJdbcContainer);
    }

    @Override
    public synchronized JdbcContainer borrowContainer() throws DataSetException {
        while (dBunitJdbcContainer != null && dBunitJdbcContainer.isRunning()) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        if (dBunitJdbcContainer == null) {
            throw new DataSetException("No JdbcContainer available. SingleJdbcContainerPool is closed");
        }

        dBunitJdbcContainer.start();
        return dBunitJdbcContainer;
    }

    @Override
    public synchronized void returnContainer(JdbcContainer jdbcContainer) {
        if (jdbcContainer != dBunitJdbcContainer) {
            throw new IllegalArgumentException("jdbcContainer is not an instance of this pool");
        }

        dBunitJdbcContainer.stop();
        notifyAll();
    }

    @Override
    public synchronized void close() {
        dBunitJdbcContainer.stop();
        dBunitJdbcContainer = null;
    }
}
