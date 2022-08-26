package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DefaultChainableDataSetConsumer;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

import static java.text.MessageFormat.format;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultContainerAwareDataSetConsumer extends DefaultChainableDataSetConsumer implements ContainerAwareDataSetConsumer {

    private Logger logger = LoggerFactory.getLogger(DefaultContainerAwareDataSetConsumer.class);

    private JdbcContainer jdbcContainer;

    private ThreadLocal<Boolean> endlessRecursionDetector = new ThreadLocal<>();

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

        try {
            if (Boolean.TRUE.equals(endlessRecursionDetector.get())) {
                if (logger.isWarnEnabled()) {
                    String msg = format("{0} introduces an endless recursion. You probably call super.startDataSet() within an overridden startDataSet({1}) and you should fix this." +
                                    " The {2} will automatically break the endless recursion and call startDataSet({1}) for you.",
                            getClass().getName(),
                            JdbcContainer.class.getSimpleName(),
                            DefaultContainerAwareDataSetConsumer.class.getName()
                    );
                    logger.warn(msg);
                }

                startDataSetInternal(getJdbcContainer());;
                return;
            }

            endlessRecursionDetector.set(true);
            startDataSet(jdbcContainer);
        } finally {
            endlessRecursionDetector.remove();
        }
    }


    public JdbcContainer getJdbcContainer() {
        return jdbcContainer;
    }

    protected void startDataSet(JdbcContainer jdbcContainer) throws DataSetException {
        startDataSetInternal(jdbcContainer);
    }

    private void startDataSetInternal(JdbcContainer jdbcContainer) throws DataSetException {
        IDataSetConsumer delegate = getDelegate();

        if (delegate instanceof ContainerAwareDataSetConsumer) {
            ContainerAwareDataSetConsumer containerAwareDataSetConsumer = (ContainerAwareDataSetConsumer) delegate;
            containerAwareDataSetConsumer.containerStarted(jdbcContainer);
        }

        super.startDataSet();
    }


}
