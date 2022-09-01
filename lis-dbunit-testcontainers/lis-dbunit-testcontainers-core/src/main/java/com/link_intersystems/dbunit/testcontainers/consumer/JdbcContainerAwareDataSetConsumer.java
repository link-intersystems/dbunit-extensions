package com.link_intersystems.dbunit.testcontainers.consumer;

import com.link_intersystems.dbunit.stream.consumer.DefaultChainableDataSetConsumer;
import com.link_intersystems.dbunit.testcontainers.JdbcContainer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;

import static java.text.MessageFormat.format;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JdbcContainerAwareDataSetConsumer extends DefaultChainableDataSetConsumer implements IDataSetConsumer {

    private ThreadLocal<Boolean> endlessRecursionDetector = new ThreadLocal<>();

    @Override
    public final void startDataSet() throws DataSetException {
        JdbcContainer jdbcContainer = getJdbcContainer();
        if (jdbcContainer == null) {
            String jdbcContainerClassName = JdbcContainer.class.getSimpleName();
            String testContainerConsumerClassName = TestContainersLifecycleConsumer.class.getName();
            String msg = format("{0} is null. Ensure that a {0} is available through the {1}. Usually set by {2}",
                    jdbcContainerClassName,
                    JdbcContainerHolder.class.getSimpleName(),
                    TestContainersLifecycleConsumer.class.getSimpleName()
            );
            throw new DataSetException(msg);
        }

        try {
            if (Boolean.TRUE.equals(endlessRecursionDetector.get())) {
                String msg = format("Endless recursion detected. You probably call super.startDataSet() within an overridden startDataSet({0}) which introduces an endless recursion. " +
                                "You might want to call super.startDataSet({0}) instead.",
                        JdbcContainer.class.getSimpleName()
                );

                throw new DataSetException(msg);
            }

            endlessRecursionDetector.set(true);
            startDataSet(jdbcContainer);
        } finally {
            endlessRecursionDetector.remove();
        }
    }

    protected JdbcContainer getJdbcContainer() {
        return JdbcContainerHolder.get();
    }


    protected void startDataSet(JdbcContainer jdbcContainer) throws DataSetException {
        super.startDataSet();
    }

}
