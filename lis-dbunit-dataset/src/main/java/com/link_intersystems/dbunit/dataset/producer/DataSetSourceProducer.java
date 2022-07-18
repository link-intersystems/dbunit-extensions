package com.link_intersystems.dbunit.dataset.producer;

import com.link_intersystems.dbunit.dataset.consumer.CopyDataSetConsumer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.IDataSetProducer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetSourceProducer implements DataSetSource {

    private IDataSetProducer dataSetProducer;

    public DataSetSourceProducer(IDataSetProducer dataSetProducer) {
        this.dataSetProducer = requireNonNull(dataSetProducer);
    }

    @Override
    public IDataSet get() throws DataSetException {
        CopyDataSetConsumer copyDataSetConsumer = new CopyDataSetConsumer();
        dataSetProducer.setConsumer(copyDataSetConsumer);

        dataSetProducer.produce();

        return copyDataSetConsumer.getDataSet();
    }
}
