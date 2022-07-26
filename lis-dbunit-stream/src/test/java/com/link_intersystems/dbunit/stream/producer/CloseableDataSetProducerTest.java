package com.link_intersystems.dbunit.stream.producer;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class CloseableDataSetProducerTest {

    private IDataSetConsumer consumer;
    private CloseableDataSetProducer closeableDataSetProducer;
    private AutoCloseable closeable;

    @BeforeEach
    void setUp() throws DataSetException {
        IDataSetProducer dataSetProducer = new IDataSetProducer() {

            private IDataSetConsumer consumer;

            @Override
            public void setConsumer(IDataSetConsumer consumer) throws DataSetException {
                this.consumer = consumer;
            }

            @Override
            public void produce() throws DataSetException {
                consumer.startDataSet();
                consumer.startTable(new DefaultTableMetaData("test", new Column[0]));
                consumer.row(new Object[0]);
                consumer.endTable();
                consumer.endDataSet();

            }
        };

        consumer = mock(IDataSetConsumer.class);
        closeable = mock(AutoCloseable.class);
        dataSetProducer.setConsumer(consumer);
        closeableDataSetProducer = new CloseableDataSetProducer(dataSetProducer, closeable);
        closeableDataSetProducer.setConsumer(consumer);
    }

    @Test
    void closeAfterProduce() throws Exception {

        closeableDataSetProducer.produce();

        verify(closeable, times(1)).close();
    }

    @Test
    void closeAfterProduceOnStartDataSetException() throws Exception {
        DataSetException expectedException = new DataSetException();
        doThrow(expectedException).when(consumer).startDataSet();

        produceAndVerifyClosed(expectedException);
    }

    @Test
    void closeAfterProduceOnStartTableException() throws Exception {
        DataSetException expectedException = new DataSetException();
        doThrow(expectedException).when(consumer).startTable(Mockito.any());

        produceAndVerifyClosed(expectedException);
    }

    @Test
    void closeAfterProduceOnRowException() throws Exception {
        DataSetException expectedException = new DataSetException();
        doThrow(expectedException).when(consumer).row(Mockito.any());

        produceAndVerifyClosed(expectedException);
    }

    @Test
    void closeAfterProduceOnEndTableException() throws Exception {
        DataSetException expectedException = new DataSetException();
        doThrow(expectedException).when(consumer).endTable();

        produceAndVerifyClosed(expectedException);
    }

    @Test
    void closeAfterProduceOnEndDataSetException() throws Exception {
        DataSetException expectedException = new DataSetException();
        doThrow(expectedException).when(consumer).endDataSet();

        produceAndVerifyClosed(expectedException);
    }


    @Test
    void exceptionOnEnsureEndDataSet() throws Exception {
        DataSetException expectedException = new DataSetException();
        doThrow(expectedException).when(consumer).endTable();
        doThrow(expectedException).when(consumer).endDataSet();

        produceAndVerifyClosed(expectedException);
    }

    @Test
    void exceptionOnClose() throws Exception {
        DataSetException expectedException = new DataSetException();
        doThrow(expectedException).when(closeable).close();

        closeableDataSetProducer.produce();

        verify(closeable, times(1)).close();
    }


    private void produceAndVerifyClosed(DataSetException expectedException) throws Exception {
        DataSetException thrownException = assertThrows(DataSetException.class, () -> closeableDataSetProducer.produce());
        assertSame(expectedException, thrownException);

        verify(closeable, times(1)).close();
    }


}