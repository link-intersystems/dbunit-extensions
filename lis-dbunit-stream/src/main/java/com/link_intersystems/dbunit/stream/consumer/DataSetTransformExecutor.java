package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.stream.producer.DataSetProducerSupport;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetTransformExecutor implements DataSetProducerSupport, DataSetConsumerSupport {

    private IDataSetProducer dataSetProducer;
    private IDataSetConsumer dataSetConsumer;

    private DataSetTransormer dataSetTransformer;

    @Override
    public void setDataSetProducer(IDataSetProducer dataSetProducer) {
        this.dataSetProducer = dataSetProducer;
    }

    @Override
    public void setDataSetConsumer(IDataSetConsumer dataSetConsumer) {
        this.dataSetConsumer = dataSetConsumer;
    }

    public void setDataSetTransformer(DataSetTransormer dataSetTransformer) {
        this.dataSetTransformer = dataSetTransformer;
    }

    public void exec() throws DataSetException {
        IDataSetConsumer inputConsumer = dataSetTransformer.getInputConsumer();
        dataSetProducer.setConsumer(inputConsumer);
        dataSetTransformer.setOutputConsumer(dataSetConsumer);

        dataSetProducer.produce();

    }


}
