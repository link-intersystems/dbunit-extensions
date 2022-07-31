package com.link_intersystems.dbunit.stream.producer;

import com.link_intersystems.dbunit.stream.consumer.AbstractDataSetConsumerDelegate;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class AutocloseDataSetProducer implements AutoCloseable, IDataSetProducer {

    private static class ConsumerExpectionInterceptor extends AbstractDataSetConsumerDelegate {

        private IDataSetConsumer interceptionTarget = new DefaultConsumer();
        private boolean endDataSet;

        public void setInterceptionTarget(IDataSetConsumer interceptionTarget) {
            this.interceptionTarget = requireNonNull(interceptionTarget);
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

        public void ensureEndDataSetInvoked() throws DataSetException {
            if (!endDataSet) {
                endDataSet();
            }
        }
    }

    private Logger logger = LoggerFactory.getLogger(AutocloseDataSetProducer.class);

    private final IDataSetProducer dataSetProducer;
    private AutoCloseable autoCloseable;
    private ConsumerExpectionInterceptor interceptor = new ConsumerExpectionInterceptor();

    public AutocloseDataSetProducer(IDataSetProducer dataSetProducer) {
        this(dataSetProducer, null);
    }

    public AutocloseDataSetProducer(IDataSetProducer dataSetProducer, AutoCloseable autoCloseable) {
        this.dataSetProducer = requireNonNull(dataSetProducer);
        this.autoCloseable = autoCloseable;
    }

    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void setConsumer(IDataSetConsumer consumer) throws DataSetException {
        interceptor.setInterceptionTarget(requireNonNull(consumer));
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
            interceptor.ensureEndDataSetInvoked();
        } catch (DataSetException e) {
            getLogger().error("Unable to ensure that endDataSet has been invoked. Resources may not be closed.", e);
        }

        if (autoCloseable != null) {
            autoCloseable.close();
        }
    }
}
