package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetTransformerChain implements DataSetTransormer {

    private DataSetTransormer firstElement;
    private DataSetTransormer lastElement;
    private IDataSetConsumer outputConsumer = new DefaultConsumer();

    public DataSetTransformerChain() {
    }

    public DataSetTransformerChain(DataSetConsumerPipe dataSetConsumerPipe) {
        this(new DataSetConsumerPipeTransformerAdapter(dataSetConsumerPipe));
    }

    public DataSetTransformerChain(DataSetTransormer dataSetTransormer) {
        add(dataSetTransormer);
    }

    public void add(DataSetConsumerPipe dataSetConsumerPipe) {
        add(new DataSetConsumerPipeTransformerAdapter(dataSetConsumerPipe));
    }

    public void add(DataSetTransormer dataSetTransormer) {
        if (dataSetTransormer == null) {
            return;
        }

        if (firstElement == null) {
            firstElement = dataSetTransormer;
            lastElement = firstElement;
        } else {
            firstElement.setOutputConsumer(dataSetTransormer.getInputConsumer());
            lastElement = dataSetTransormer;
        }

        lastElement.setOutputConsumer(outputConsumer);
    }


    @Override
    public IDataSetConsumer getInputConsumer() {
        if (firstElement == null) {
            return outputConsumer;
        }
        return firstElement.getInputConsumer();
    }

    @Override
    public void setOutputConsumer(IDataSetConsumer outputConsumer) {
        this.outputConsumer = requireNonNull(outputConsumer);

        if (lastElement == null) {
            return;
        }

        lastElement.setOutputConsumer(outputConsumer);
    }
}
