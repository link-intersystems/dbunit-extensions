package com.link_intersystems.dbunit.testcontainers.pool;

import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.RunningContainer;
import org.dbunit.dataset.DataSetException;

import java.util.function.Supplier;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class SingleRunningContainerPool implements RunningContainerPool {

    private DBunitJdbcContainer dBunitJdbcContainer;

    private RunningContainer runningContainer;

    public SingleRunningContainerPool(DBunitJdbcContainer dBunitJdbcContainer) {
        this.dBunitJdbcContainer = dBunitJdbcContainer;
    }

    @Override
    public synchronized RunningContainer borrowContainer() throws DataSetException {
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
    public synchronized void returnContainer(RunningContainer runningContainer) {
        if (runningContainer != this.runningContainer) {
            throw new IllegalArgumentException("runningContainer is not an instance of this pool");
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
