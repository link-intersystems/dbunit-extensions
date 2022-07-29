package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.test.DBUnitAssertions;
import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.DataSetProducerAdapter;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

        dataSetConsumer1 = Mockito.mock(IDataSetConsumer.class);
        dataSetConsumer2 = Mockito.mock(IDataSetConsumer.class);

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
        ITableMetaData tableMetaData = Mockito.mock(ITableMetaData.class);
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

    @Test
    void isEmpty() {
        assertFalse(compositeDataSetConsumer.isEmpty());

        compositeDataSetConsumer.remove(dataSetConsumer1);
        compositeDataSetConsumer.remove(dataSetConsumer2);

        assertTrue(compositeDataSetConsumer.isEmpty());
    }

    @Test
    void copyRealDataSet() throws DataSetException, IOException {
        CopyDataSetConsumer copyDataSetConsumer1 = new CopyDataSetConsumer();
        CopyDataSetConsumer copyDataSetConsumer2 = new CopyDataSetConsumer();

        CompositeDataSetConsumer compositeDataSetConsumer = new CompositeDataSetConsumer(copyDataSetConsumer1, copyDataSetConsumer2);

        IDataSet tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
        DataSetProducerAdapter dataSetProducerAdapter = new DataSetProducerAdapter(tinySakilaDataSet);
        dataSetProducerAdapter.setConsumer(compositeDataSetConsumer);
        dataSetProducerAdapter.produce();

        DBUnitAssertions.STRICT.assertDataSetEquals(tinySakilaDataSet, copyDataSetConsumer1.getDataSet());
        DBUnitAssertions.STRICT.assertDataSetEquals(tinySakilaDataSet, copyDataSetConsumer2.getDataSet());
    }
}
