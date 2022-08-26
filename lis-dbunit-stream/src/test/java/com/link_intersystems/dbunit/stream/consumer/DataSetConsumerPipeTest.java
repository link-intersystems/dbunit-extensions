package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.stream.producer.DefaultDataSetProducerSupport;
import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetConsumerPipeTest {

    private DefaultChainableDataSetConsumer pipe1;
    private DefaultChainableDataSetConsumer pipe2;
    private CopyDataSetConsumer targetConsumer;
    private DataSetConsumerPipe dataSetConsumerPipe;
    private IDataSet tinySakilaDataSet;

    @BeforeEach
    void setUp() throws DataSetException, IOException {
        dataSetConsumerPipe = new DataSetConsumerPipe();

        pipe1 = new DefaultChainableDataSetConsumer();
        pipe2 = new DefaultChainableDataSetConsumer();
        targetConsumer = new CopyDataSetConsumer();

        tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
    }

    @Test
    void outputConsumerOnly() throws DataSetException {
        dataSetConsumerPipe.setOutputConsumer(targetConsumer);

        chainProduce();
        assertTransformerChainWorks();
    }

    @Test
    void emptyChain() throws DataSetException {
        chainProduce();
    }

    @Test
    void setOutputConsumerBeforeAddElements() throws DataSetException {
        dataSetConsumerPipe.setOutputConsumer(targetConsumer);

        dataSetConsumerPipe.add(pipe1);
        dataSetConsumerPipe.add(pipe2);

        chainProduce();
        assertTransformerChainWorks();
    }

    @Test
    void setOutputConsumerAfterAddElements() throws DataSetException {
        dataSetConsumerPipe.add(pipe1);
        dataSetConsumerPipe.add(pipe2);

        dataSetConsumerPipe.setOutputConsumer(targetConsumer);

        chainProduce();
        assertTransformerChainWorks();
    }

    @Test
    void firstElementConstructor() throws DataSetException {
        dataSetConsumerPipe = new DataSetConsumerPipe(pipe1);
        dataSetConsumerPipe.add(pipe2);

        dataSetConsumerPipe.setOutputConsumer(targetConsumer);

        chainProduce();
        assertTransformerChainWorks();
    }

    private void assertTransformerChainWorks() throws DataSetException {


        DBUnitAssertions.STRICT.assertDataSetEquals(tinySakilaDataSet, targetConsumer.getDataSet());
    }

    private void chainProduce() throws DataSetException {
        DefaultDataSetProducerSupport producerSupport = new DefaultDataSetProducerSupport();
        producerSupport.setDataSetProducer(tinySakilaDataSet);

        IDataSetProducer dataSetProducer = producerSupport.getDataSetProducer();
        dataSetProducer.setConsumer(dataSetConsumerPipe);
        dataSetProducer.produce();
    }
}