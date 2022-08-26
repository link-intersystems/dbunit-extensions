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
    private IDataSetConsumer outputConsumer = new DefaultConsumer();

    public DataSetConsumerPipe() {
    }

    public DataSetConsumerPipe(ChainableDataSetConsumer firstElement) {
        add(firstElement);
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

        if (dataSetConsumer == this) {
            throw new IllegalArgumentException("Can not add me to myself as a pipe element");
        }

        if (lastElement == null) {
            firstElement = dataSetConsumer;
            lastElement = firstElement;
        } else {
            lastElement.setSubsequentConsumer(dataSetConsumer);
            lastElement = dataSetConsumer;
        }

        lastElement.setSubsequentConsumer(outputConsumer);
    }

    public void execute(IDataSetProducer dataSetProducer) throws DataSetException {
        dataSetProducer.setConsumer(this);
        dataSetProducer.produce();
    }
}
