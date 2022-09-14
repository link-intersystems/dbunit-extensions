package com.link_intersystems.dbunit.stream.producer.support;

import org.dbunit.dataset.stream.IDataSetProducer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDataSetProducerSupport implements DataSetProducerSupport {

    private IDataSetProducer dataSetProducer;

    public IDataSetProducer getDataSetProducer() {
        return dataSetProducer;
    }

    @Override
    public void setDataSetProducer(IDataSetProducer dataSetProducer) {
        this.dataSetProducer = dataSetProducer;
    }
}
