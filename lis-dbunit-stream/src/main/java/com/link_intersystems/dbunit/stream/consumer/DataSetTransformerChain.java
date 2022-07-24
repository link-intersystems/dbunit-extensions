package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetTransformerChain implements DataSetTransormer {

    private DataSetTransormer firstElement;
    private DataSetTransormer lastElement;

    public DataSetTransformerChain() {
    }

    public DataSetTransformerChain(DataSetTransormer dataSetTransormer) {
        add(dataSetTransormer);
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
    }


    @Override
    public IDataSetConsumer getInputConsumer() {
        if (firstElement == null) {
            return new DefaultConsumer();
        }
        return firstElement.getInputConsumer();
    }

    @Override
    public void setOutputConsumer(IDataSetConsumer dataSetConsumer) {
        if (lastElement == null) {
            return;
        }

        lastElement.setOutputConsumer(dataSetConsumer);
    }
}
