package com.link_intersystems.dbunit.stream.producer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetSourceProducerAdapter implements IDataSetProducer, DataSetSource {

    private IDataSet dataSet;

    private static final Logger logger = LoggerFactory.getLogger(DataSetSourceProducerAdapter.class);
    private static final IDataSetConsumer EMPTY_CONSUMER = new DefaultConsumer();
    private IDataSetConsumer consumer = EMPTY_CONSUMER;

    public DataSetSourceProducerAdapter(IDataSet dataSet) {
        this.dataSet = Objects.requireNonNull(dataSet);
    }

    @Override
    public IDataSet get() {
        return dataSet;
    }

    public void setConsumer(IDataSetConsumer consumer) {
        logger.debug("setConsumer(consumer) - start");
        this.consumer = consumer;
    }

    public void produce() throws DataSetException {
        DataSetProducerAdapter producerAdapter = new DataSetProducerAdapter(dataSet);
        producerAdapter.setConsumer(consumer);
        producerAdapter.produce();
    }
}
