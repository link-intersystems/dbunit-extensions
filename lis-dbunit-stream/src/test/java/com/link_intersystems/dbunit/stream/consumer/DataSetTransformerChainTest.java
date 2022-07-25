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
class DataSetTransformerChainTest {


    private DefaultDataSetConsumerPipe pipe1;
    private DefaultDataSetConsumerPipe pipe2;
    private CopyDataSetConsumer targetConsumer;
    private DataSetTransformerChain transformerChain;
    private IDataSet tinySakilaDataSet;

    @BeforeEach
    void setUp() throws DataSetException, IOException {
        transformerChain = new DataSetTransformerChain();

        pipe1 = new DefaultDataSetConsumerPipe();
        pipe2 = new DefaultDataSetConsumerPipe();
        targetConsumer = new CopyDataSetConsumer();

        tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
    }

    @Test
    void outputConsumerOnly() throws DataSetException {
        transformerChain.setOutputConsumer(targetConsumer);

        chainProduce();
        assertTransformerChainWorks();
    }

    @Test
    void emptyChain() throws DataSetException {
        chainProduce();
    }

    @Test
    void setOutputConsumerBeforeAddElements() throws DataSetException {
        transformerChain.setOutputConsumer(targetConsumer);

        transformerChain.add(pipe1);
        transformerChain.add(pipe2);

        chainProduce();
        assertTransformerChainWorks();
    }

    @Test
    void setOutputConsumerAfterAddElements() throws DataSetException {
        transformerChain.add(pipe1);
        transformerChain.add(pipe2);

        transformerChain.setOutputConsumer(targetConsumer);

        chainProduce();
        assertTransformerChainWorks();
    }

    @Test
    void firstElementConstructor() throws DataSetException {
        transformerChain = new DataSetTransformerChain(pipe1);
        transformerChain.add(pipe2);

        transformerChain.setOutputConsumer(targetConsumer);

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
        dataSetProducer.setConsumer(transformerChain.getInputConsumer());
        dataSetProducer.produce();
    }
}