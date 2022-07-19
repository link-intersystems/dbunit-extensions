package com.link_intersystems.dbunit.stream.producer;

import org.dbunit.dataset.stream.IDataSetProducer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetSourceSupport extends DataSetProducerSupport {

    default public void setDataSetProducer(IDataSetProducer dataSetProducer) {
        DataSetSource dataSetSource;

        if (dataSetProducer instanceof DataSetSource) {
            dataSetSource = (DataSetSource) dataSetProducer;
        } else {
            dataSetSource = new DataSetSourceProducer(dataSetProducer);
        }

        setDataSetSource(dataSetSource);
    }

    public void setDataSetSource(DataSetSource dataSetSource);
}
