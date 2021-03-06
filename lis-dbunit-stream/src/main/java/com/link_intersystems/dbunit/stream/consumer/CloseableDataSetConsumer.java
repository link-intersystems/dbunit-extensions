package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CloseableDataSetConsumer extends AbstractDataSetConsumerDelegate {


    private Logger logger = LoggerFactory.getLogger(CloseableDataSetConsumer.class);

    private final IDataSetConsumer dataSetConsumer;
    private final AutoCloseable autoCloseable;

    public <C extends IDataSetConsumer & AutoCloseable> CloseableDataSetConsumer(C dataSetConsumer) {
        this(dataSetConsumer, dataSetConsumer);
    }

    public CloseableDataSetConsumer(IDataSetConsumer dataSetConsumer, AutoCloseable autoCloseable) {
        this.dataSetConsumer = requireNonNull(dataSetConsumer);
        this.autoCloseable = requireNonNull(autoCloseable);
    }

    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected IDataSetConsumer getDelegate() {
        return dataSetConsumer;
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            super.endDataSet();
        } finally {
            try {
                autoCloseable.close();
            } catch (Exception e) {
                getLogger().error("Unable to close resources", e);
            }
        }
    }

}
