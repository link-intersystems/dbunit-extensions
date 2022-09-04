package com.link_intersystems.dbunit.stream.producer;

import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class AutocloseDataSetProducerTest {

    private IDataSetProducer dataSetProducer;
    private AutocloseDataSetProducer autocloseDataSetProducer;
    private IDataSetConsumer dataSetConsumer;
    private DataSetException expectedException;

    @BeforeEach
    void setUp() throws DataSetException, IOException {
        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        dataSetProducer = new DataSetProducerAdapter(tinySakilaDataSet);
        dataSetConsumer = mock(IDataSetConsumer.class);
        autocloseDataSetProducer = new AutocloseDataSetProducer(dataSetProducer);
        autocloseDataSetProducer.setConsumer(dataSetConsumer);

        expectedException = new DataSetException();
    }

    @Test
    void exceptionOnStartDataSet() throws DataSetException {
        doThrow(expectedException).when(dataSetConsumer).startDataSet();

        produceAndAssertEndDataSetInvoked(expectedException);
    }

    @Test
    void exceptionOnStartTable() throws DataSetException {
        doThrow(expectedException).when(dataSetConsumer).startTable(any());

        produceAndAssertEndDataSetInvoked(expectedException);
    }

    @Test
    void exceptionOnRow() throws DataSetException {
        doThrow(expectedException).when(dataSetConsumer).row(any());

        produceAndAssertEndDataSetInvoked(expectedException);
    }

    @Test
    void exceptionOnEndTable() throws DataSetException {
        doThrow(expectedException).when(dataSetConsumer).endTable();

        produceAndAssertEndDataSetInvoked(expectedException);
    }

    @Test
    void exceptionOnEndDataSet() throws DataSetException {
        doThrow(expectedException).when(dataSetConsumer).endDataSet();

        produceAndAssertEndDataSetInvoked(expectedException);
    }


    private void produceAndAssertEndDataSetInvoked(DataSetException expectedException) throws DataSetException {
        DataSetException dataSetException = assertThrows(DataSetException.class, autocloseDataSetProducer::produce);
        assertEquals(expectedException, dataSetException);

        verify(dataSetConsumer, times(1)).endDataSet();
    }

}