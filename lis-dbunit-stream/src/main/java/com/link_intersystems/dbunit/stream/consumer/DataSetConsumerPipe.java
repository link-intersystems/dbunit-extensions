package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetConsumerPipe extends AbstractDataSetConsumerDelegate implements ChainableDataSetConsumer {

    private ChainableDataSetConsumer firstElement;
    private ChainableDataSetConsumer lastElement;
    private IDataSetConsumer outputConsumer;

    public DataSetConsumerPipe() {
        this(new DefaultConsumer());
    }

    public DataSetConsumerPipe(IDataSetConsumer outputConsumer) {
        this.outputConsumer = requireNonNull(outputConsumer);
    }

    public void setOutputConsumer(IDataSetConsumer outputConsumer) {
        this.outputConsumer = requireNonNull(outputConsumer);

        if (lastElement != null) {
            lastElement.setSubsequentConsumer(outputConsumer);
        }
    }

    @Override
    public void setSubsequentConsumer(IDataSetConsumer outputConsumer) {
        setOutputConsumer(outputConsumer);
    }

    @Override
    protected IDataSetConsumer getDelegate() {
        return firstElement == null ? outputConsumer : firstElement;
    }

    public void add(ChainableDataSetConsumer dataSetConsumer) {
        if (dataSetConsumer == null) {
            return;
        }

        if (firstElement == null) {
            firstElement = dataSetConsumer;
            lastElement = firstElement;
        } else {
            firstElement.setSubsequentConsumer(dataSetConsumer);
            lastElement = dataSetConsumer;
        }

        lastElement.setSubsequentConsumer(outputConsumer);
    }

    public void execute(IDataSetProducer dataSetProducer) throws DataSetException {
        dataSetProducer.setConsumer(this);
        dataSetProducer.produce();
    }
}
