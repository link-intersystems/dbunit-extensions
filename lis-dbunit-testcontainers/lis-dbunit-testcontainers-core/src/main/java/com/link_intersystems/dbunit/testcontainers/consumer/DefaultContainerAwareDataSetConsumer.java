package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DefaultChainableDataSetConsumer;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;

import static java.text.MessageFormat.format;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultContainerAwareDataSetConsumer extends DefaultChainableDataSetConsumer implements ContainerAwareDataSetConsumer {

    private JdbcContainer jdbcContainer;

    @Override
    public final void containerStarted(JdbcContainer jdbcContainer) {
        this.jdbcContainer = jdbcContainer;
    }

    @Override
    public final void startDataSet() throws DataSetException {
        if (jdbcContainer == null) {
            String jdbcContainerName = JdbcContainer.class.getSimpleName();
            String containerAwareClassName = ContainerAwareDataSetConsumer.class.getName();
            String testContainerConsumerClassName = TestContainersLifecycleConsumer.class.getName();
            String msg = format("{0} is null. A {1} must be used in a context that supports {1}, like the {2}",
                    jdbcContainerName,
                    containerAwareClassName,
                    testContainerConsumerClassName
            );
            throw new DataSetException(msg);
        }

        startDataSet(jdbcContainer);
    }


    public JdbcContainer getJdbcContainer() {
        return jdbcContainer;
    }

    protected void startDataSet(JdbcContainer jdbcContainer) throws DataSetException {
        IDataSetConsumer delegate = getDelegate();

        if (delegate instanceof ContainerAwareDataSetConsumer) {
            ContainerAwareDataSetConsumer containerAwareDataSetConsumer = (ContainerAwareDataSetConsumer) delegate;
            containerAwareDataSetConsumer.containerStarted(jdbcContainer);
        }

        super.startDataSet();
    }


}
