package com.link_intersystems.dbunit.testcontainers.pool;

import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import com.link_intersystems.dbunit.testcontainers.RunningContainer;
import org.dbunit.dataset.DataSetException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SingleJdbcContainerPool implements JdbcContainerPool {

    private DBunitJdbcContainer dBunitJdbcContainer;

    private RunningContainer runningContainer;

    public SingleJdbcContainerPool(DBunitJdbcContainer dBunitJdbcContainer) {
        this.dBunitJdbcContainer = dBunitJdbcContainer;
    }

    @Override
    public synchronized JdbcContainer borrowContainer() throws DataSetException {
        while (runningContainer != null) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        runningContainer = dBunitJdbcContainer.start();
        return runningContainer;
    }

    @Override
    public synchronized void returnContainer(JdbcContainer jdbcContainer) {
        if (jdbcContainer != runningContainer) {
            throw new IllegalArgumentException("jdbcContainer is not an instance of this pool");
        }

        runningContainer.stop();
        this.runningContainer = null;
        notifyAll();
    }

    @Override
    public synchronized void close() {
        if (runningContainer != null) {
            returnContainer(runningContainer);
        }
    }
}
