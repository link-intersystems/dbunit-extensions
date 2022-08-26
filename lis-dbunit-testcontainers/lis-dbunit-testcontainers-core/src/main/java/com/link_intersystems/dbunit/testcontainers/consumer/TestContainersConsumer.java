package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DefaultChainableDataSetConsumer;
import com.link_intersystems.dbunit.testcontainers.DBunitJdbcContainer;
import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import com.link_intersystems.dbunit.testcontainers.RunningContainer;
import com.link_intersystems.dbunit.testcontainers.pool.RunningContainerPool;
import com.link_intersystems.dbunit.testcontainers.pool.SingleRunningContainerPool;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestContainersConsumer extends DefaultChainableDataSetConsumer {
    private RunningContainerPool runningContainerPool;
    private RunningContainer runningContainer;

    public TestContainersConsumer(DatabaseContainerSupport databaseContainerSupport) {
        this(() -> new DBunitJdbcContainer(databaseContainerSupport.create(), databaseContainerSupport.getDatabaseConfig()));
    }

    public TestContainersConsumer(Supplier<DBunitJdbcContainer> dBunitJdbcContainerSupplier) {
        this.runningContainerPool = new SingleRunningContainerPool(
                dBunitJdbcContainerSupplier
        );
    }

    public TestContainersConsumer(RunningContainerPool runningContainerPool) {
        this.runningContainerPool = requireNonNull(runningContainerPool);
    }

    @Override
    public void startDataSet() throws DataSetException {
        runningContainer = runningContainerPool.borrowContainer();

        IDataSetConsumer delegate = getDelegate();
        if (delegate instanceof ContainerAwareDataSetConsumer) {
            ContainerAwareDataSetConsumer containerAwareDataSetConsumer = (ContainerAwareDataSetConsumer) delegate;
            containerAwareDataSetConsumer.containerStarted(runningContainer);
        }

        super.startDataSet();
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            super.endDataSet();
        } finally {
            runningContainerPool.returnContainer(runningContainer);
        }
    }
}
