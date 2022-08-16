package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.slf4j.Logger;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class CloseableDataSetConsumerTest extends AbstractDataSetConsumerDelegateTest {

    private AutoCloseable autoCloseable;
    private CloseableDataSetConsumer closeableDataSetConsumer;
    private IDataSetConsumer target;
    private Logger logger;

    @Override
    protected AbstractDataSetConsumerDelegate createDataSetConsumerDelegate(IDataSetConsumer target) {
        this.target = target;
        autoCloseable = mock(AutoCloseable.class);
        logger = mock(Logger.class);
        return closeableDataSetConsumer = new CloseableDataSetConsumer(target, autoCloseable) {
            @Override
            protected Logger getLogger() {
                Logger defaultLogger = super.getLogger();
                assertNotNull(defaultLogger);
                return CloseableDataSetConsumerTest.this.logger;
            }
        };
    }

    @Test
    @Override
    void startDataSet() throws Exception {
        super.startDataSet();

        verify(autoCloseable, never()).close();
    }

    @Test
    @Override
    void startTable() throws Exception {
        super.startTable();

        verify(autoCloseable, never()).close();
    }

    @Test
    @Override
    void row() throws Exception {
        super.row();

        verify(autoCloseable, never()).close();
    }

    @Test
    @Override
    void endTable() throws Exception {
        super.endTable();

        verify(autoCloseable, never()).close();
    }

    @Test
    void endDataSet() throws Exception {
        super.endDataSet();

        verify(autoCloseable, times(1)).close();
    }

    @Test
    void closeOnEndDataSetException() throws Exception {
        DataSetException dataSetException = new DataSetException(new RuntimeException());
        doThrow(dataSetException).when(target).endDataSet();

        DataSetException thrownException = assertThrows(DataSetException.class, () -> closeableDataSetConsumer.endDataSet());

        assertSame(dataSetException, thrownException);

        verify(autoCloseable, times(1)).close();
    }

    @Test
    void datasetExceptionShouldNotBeOverriddenByCloseableException() throws Exception {
        DataSetException dataSetException = new DataSetException(new RuntimeException());
        doThrow(dataSetException).when(target).endDataSet();

        IOException ioException = new IOException();
        doThrow(ioException).when(autoCloseable).close();

        DataSetException thrownException = assertThrows(DataSetException.class, () -> closeableDataSetConsumer.endDataSet());

        assertSame(dataSetException, thrownException);

        verify(autoCloseable, times(1)).close();
        verify(logger, times(1)).error(ArgumentMatchers.any(String.class), ArgumentMatchers.eq(ioException));
    }

}