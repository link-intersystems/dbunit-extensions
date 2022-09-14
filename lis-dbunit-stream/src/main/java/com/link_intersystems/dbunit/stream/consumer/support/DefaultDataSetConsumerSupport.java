package com.link_intersystems.dbunit.stream.consumer.support;

import org.dbunit.dataset.stream.IDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDataSetConsumerSupport implements DataSetConsumerSupport {

    private IDataSetConsumer dataSetConsumer;

    public IDataSetConsumer getDataSetConsumer() {
        return dataSetConsumer;
    }

    @Override
    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        this.dataSetConsumer = dataSetConsumer;
    }
}
