package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetConsumerPipeTransformerAdapter implements DataSetTransormer {

    private DataSetConsumerPipe dataSetConsumerDelegate;

    public DataSetConsumerPipeTransformerAdapter(DataSetConsumerPipe dataSetConsumerDelegate) {
        this.dataSetConsumerDelegate = requireNonNull(dataSetConsumerDelegate);
    }

    @Override
    public IDataSetConsumer getInputConsumer() {
        return dataSetConsumerDelegate == null ? new DefaultConsumer() : dataSetConsumerDelegate;
    }

    @Override
    public void setOutputConsumer(IDataSetConsumer dataSetConsumer) {
        dataSetConsumerDelegate.setSubsequentConsumer(dataSetConsumer);
    }
}
