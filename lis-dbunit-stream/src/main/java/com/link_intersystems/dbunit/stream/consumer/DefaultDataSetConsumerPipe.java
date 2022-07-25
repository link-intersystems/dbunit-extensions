package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.stream.IDataSetConsumer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDataSetConsumerPipe extends AbstractDataSetConsumerDelegate implements DataSetConsumerPipe {

    private IDataSetConsumer subsequentConsumer;

    @Override
    public void setSubsequentConsumer(IDataSetConsumer subsequentConsumer) {
        this.subsequentConsumer = subsequentConsumer;
    }

    @Override
    public IDataSetConsumer getDelegate() {
        return subsequentConsumer;
    }
}
