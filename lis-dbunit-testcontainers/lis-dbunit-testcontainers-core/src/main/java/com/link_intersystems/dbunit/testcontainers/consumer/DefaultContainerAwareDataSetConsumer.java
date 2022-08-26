package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.DefaultConsumer;

import static java.text.MessageFormat.format;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultContainerAwareDataSetConsumer extends DefaultConsumer implements ContainerAwareDataSetConsumer {

    private JdbcContainer jdbcContainer;

    @Override
    public final void containerStarted(JdbcContainer jdbcContainer) throws DataSetException {
        this.jdbcContainer = jdbcContainer;
    }

    @Override
    public final void startDataSet() throws DataSetException {
        if (jdbcContainer == null) {
            String jdbcContainerName = JdbcContainer.class.getSimpleName();
            String containerAwareClassName = ContainerAwareDataSetConsumer.class.getName();
            String testContainerConsumerClassName = TestContainersConsumer.class.getName();
            String msg = format("{} is null. A {} must be used in a context that supports {}, like the {}",
                    jdbcContainerName,
                    containerAwareClassName,
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
    }


}
