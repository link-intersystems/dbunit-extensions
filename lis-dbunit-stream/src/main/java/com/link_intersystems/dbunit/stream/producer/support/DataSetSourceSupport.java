package com.link_intersystems.dbunit.stream.producer.support;

import com.link_intersystems.dbunit.dataset.DataSetSupplier;
import com.link_intersystems.dbunit.stream.producer.DataSetSourceProducer;
import org.dbunit.dataset.stream.IDataSetProducer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetSourceSupport extends DataSetProducerSupport {

    default public void setDataSetProducer(IDataSetProducer dataSetProducer) {
        DataSetSupplier dataSetSource;

        if (dataSetProducer instanceof DataSetSupplier) {
            dataSetSource = (DataSetSupplier) dataSetProducer;
        } else {
            dataSetSource = new DataSetSourceProducer(dataSetProducer);
        }

        setDataSetSource(dataSetSource);
    }

    public void setDataSetSource(DataSetSupplier dataSetSource);
}
