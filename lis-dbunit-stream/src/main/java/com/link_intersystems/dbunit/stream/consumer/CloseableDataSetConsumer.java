package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CloseableDataSetConsumer extends AbstractDataSetConsumerDelegate implements AutoCloseable {


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

        public void dataSetFinished() throws DataSetException {
            if (!endDataSet && e != null) {
                endDataSet();
            }
        }
    }

    private Logger logger = LoggerFactory.getLogger(CloseableDataSetConsumer.class);

    private final AutoCloseable autoCloseable;
    private final ConsumerExpectionInterceptor interceptor;

    public CloseableDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        this(dataSetConsumer, null);
    }

    public CloseableDataSetConsumer(IDataSetConsumer dataSetConsumer, AutoCloseable autoCloseable) {
        interceptor = new ConsumerExpectionInterceptor(requireNonNull(dataSetConsumer));
        this.autoCloseable = autoCloseable;
    }

    protected Logger getLogger() {
        return logger;
    }

    @Override
    protected IDataSetConsumer getDelegate() {
        return interceptor;
    }

    @Override
    public void endDataSet() throws DataSetException {
        try {
            super.endDataSet();
        } finally {
            try {
                close();
            } catch (Exception e) {
                getLogger().error("Unable to close resources", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            interceptor.dataSetFinished();
        } catch (DataSetException e) {
            getLogger().error("Unable to close resource", e);
        }

        if (autoCloseable != null) {
            autoCloseable.close();
        }
    }
}
