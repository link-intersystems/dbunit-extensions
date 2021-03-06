package com.link_intersystems.dbunit.stream.consumer;

import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import static org.mockito.Mockito.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class AbstractDataSetConsumerDelegateTest {

    private IDataSetConsumer target;
    private AbstractDataSetConsumerDelegate dataSetConsumer;

    @BeforeEach
    void setUp() {
        target = Mockito.mock(IDataSetConsumer.class);

        dataSetConsumer = createDataSetConsumerDelegate(target);
    }

    protected AbstractDataSetConsumerDelegate createDataSetConsumerDelegate(IDataSetConsumer target) {
        return new AbstractDataSetConsumerDelegate() {
            @Override
            protected IDataSetConsumer getDelegate() {
                return target;
            }
        };
    }

    @Test
    void startDataSet() throws Exception {
        dataSetConsumer.startDataSet();

        verify(target, times(1)).startDataSet();

        verify(target, never()).endDataSet();
        verify(target, never()).startTable(ArgumentMatchers.any());
        verify(target, never()).endTable();
        verify(target, never()).row(ArgumentMatchers.any());
    }

    @Test
    void endDataSet() throws Exception {
        dataSetConsumer.endDataSet();

        verify(target, times(1)).endDataSet();

        verify(target, never()).startDataSet();
        verify(target, never()).startTable(ArgumentMatchers.any());
        verify(target, never()).endTable();
        verify(target, never()).row(ArgumentMatchers.any());
    }

    @Test
    void startTable() throws Exception {
        ITableMetaData metaData = Mockito.mock(ITableMetaData.class);

        dataSetConsumer.startTable(metaData);

        verify(target, times(1)).startTable(metaData);

        verify(target, never()).startDataSet();
        verify(target, never()).endDataSet();
        verify(target, never()).endTable();
        verify(target, never()).row(ArgumentMatchers.any());
    }

    @Test
    void endTable() throws Exception {
        dataSetConsumer.endTable();

        verify(target, times(1)).endTable();

        verify(target, never()).startDataSet();
        verify(target, never()).endDataSet();
        verify(target, never()).startTable(ArgumentMatchers.any());
        verify(target, never()).row(ArgumentMatchers.any());
    }

    @Test
    void row() throws Exception {
        Object[] row = new Object[]{1, "2"};

        dataSetConsumer.row(row);

        verify(target, times(1)).row(row);

        verify(target, never()).startDataSet();
        verify(target, never()).endDataSet();
        verify(target, never()).startTable(ArgumentMatchers.any());
        verify(target, never()).endTable();
    }

}