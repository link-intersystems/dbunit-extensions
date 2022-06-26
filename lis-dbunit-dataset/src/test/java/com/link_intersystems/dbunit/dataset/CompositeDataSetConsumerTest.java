package com.link_intersystems.dbunit.dataset;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class CompositeDataSetConsumerTest {

    private CompositeDataSetConsumer compositeDataSetConsumer;
    private IDataSetConsumer dataSetConsumer1;
    private IDataSetConsumer dataSetConsumer2;

    @BeforeEach
    void setUp() {
        compositeDataSetConsumer = new CompositeDataSetConsumer();

        dataSetConsumer1 = mock(IDataSetConsumer.class);
        dataSetConsumer2 = mock(IDataSetConsumer.class);

        compositeDataSetConsumer.add(dataSetConsumer1);
        compositeDataSetConsumer.add(dataSetConsumer2);
    }


    @Test
    void startDataSet() throws DataSetException {
        compositeDataSetConsumer.startDataSet();

        verify(dataSetConsumer1, times(1)).startDataSet();
        verify(dataSetConsumer2, times(1)).startDataSet();
    }

    @Test
    void endDataSet() throws DataSetException {
        compositeDataSetConsumer.endDataSet();

        verify(dataSetConsumer1, times(1)).endDataSet();
        verify(dataSetConsumer2, times(1)).endDataSet();
    }

    @Test
    void startTable() throws DataSetException {
        ITableMetaData tableMetaData = mock(ITableMetaData.class);
        compositeDataSetConsumer.startTable(tableMetaData);

        verify(dataSetConsumer1, times(1)).startTable(tableMetaData);
        verify(dataSetConsumer2, times(1)).startTable(tableMetaData);
    }

    @Test
    void endTable() throws DataSetException {
        compositeDataSetConsumer.endTable();

        verify(dataSetConsumer1, times(1)).endTable();
        verify(dataSetConsumer2, times(1)).endTable();
    }

    @Test
    void row() throws DataSetException {
        Object[] row = new Object[]{1, 2, "H"};
        compositeDataSetConsumer.row(row);

        verify(dataSetConsumer1, times(1)).row(row);
        verify(dataSetConsumer2, times(1)).row(row);
    }

    @Test
    void remove() throws DataSetException {
        compositeDataSetConsumer.remove(dataSetConsumer2);

        compositeDataSetConsumer.startDataSet();

        verify(dataSetConsumer1, times(1)).startDataSet();
        verify(dataSetConsumer2, never()).startDataSet();
    }
}
