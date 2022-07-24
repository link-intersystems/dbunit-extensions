package com.link_intersystems.dbunit.stream.producer;

import com.link_intersystems.dbunit.stream.consumer.AbstractDataSetConsumerDelegate;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CloseableDataSetProducer implements AutoCloseable, IDataSetProducer {

    private static class ConsumerExpectionInterceptor extends AbstractDataSetConsumerDelegate {

        private IDataSetConsumer interceptionTarget;
        private DataSetException e;
        private boolean endDataSet;

        ConsumerExpectionInterceptor(IDataSetConsumer interceptionTarget) {
            this.interceptionTarget = interceptionTarget;
        }

        @Override
        public void startDataSet() throws DataSetException {
            try {
                super.startDataSet();
            } catch (DataSetException e) {
                this.e = e;
                throw e;
            }
        }

        @Override
        public void startTable(ITableMetaData iTableMetaData) throws DataSetException {
            try {
                super.startTable(iTableMetaData);
            } catch (DataSetException e) {
                this.e = e;
                throw e;
            }
        }

        @Override
        public void row(Object[] objects) throws DataSetException {
            try {
                super.row(objects);
            } catch (DataSetException e) {
                this.e = e;
                throw e;
            }
        }

        @Override
        public void endTable() throws DataSetException {
            try {
                super.endTable();
            } catch (DataSetException e) {
                this.e = e;
                throw e;
            }
        }

        @Override
        public void endDataSet() throws DataSetException {
            super.endDataSet();
            endDataSet = true;
        }

        @Override
        protected IDataSetConsumer getDelegate() {
            return interceptionTarget;
        }

        public void produceFinished() throws DataSetException {
            if (!endDataSet && e != null) {
                endDataSet();
            }
        }
    }

    private Logger logger = LoggerFactory.getLogger(CloseableDataSetProducer.class);

    private final IDataSetProducer dataSetProducer;
    private AutoCloseable autoCloseable;
    private ConsumerExpectionInterceptor interceptor;

    public CloseableDataSetProducer(IDataSetProducer dataSetProducer) {
        this(dataSetProducer, null);
    }

    public CloseableDataSetProducer(IDataSetProducer dataSetProducer, AutoCloseable autoCloseable) {
        this.dataSetProducer = requireNonNull(dataSetProducer);
        this.autoCloseable = autoCloseable;
    }

    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void setConsumer(IDataSetConsumer consumer) throws DataSetException {
        interceptor = new ConsumerExpectionInterceptor(consumer);
        dataSetProducer.setConsumer(interceptor);
    }

    @Override
    public void produce() throws DataSetException {
        try {
            dataSetProducer.produce();
        } finally {
            try {
                close();
            } catch (Exception e) {
                getLogger().error("Unable to close resource", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            interceptor.produceFinished();
        } catch (DataSetException e) {
            getLogger().error("Unable to close resource", e);
        }

        if (autoCloseable != null) {
            autoCloseable.close();
        }
    }
}
