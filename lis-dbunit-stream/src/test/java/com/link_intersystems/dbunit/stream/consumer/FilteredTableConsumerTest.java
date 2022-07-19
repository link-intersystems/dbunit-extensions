package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.table.TableUtil;
import com.link_intersystems.dbunit.test.TestDataSets;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Mockito.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class FilteredTableConsumerTest {

    private IDataSetConsumer dataSetConsumer;
    private FilteredTableConsumer filteredTableConsumer;
    private IDataSet tinySakilaDataSet;

    @BeforeEach
    void setUp() throws DataSetException, IOException {
        dataSetConsumer = mock(IDataSetConsumer.class);
        filteredTableConsumer = new FilteredTableConsumer(dataSetConsumer, "actor"::equals);

        tinySakilaDataSet = TestDataSets.getTinySakilaDataSet();
    }

    @Test
    void startDataSet() throws DataSetException {
        filteredTableConsumer.startDataSet();

        verify(dataSetConsumer, times(1)).startDataSet();
    }

    @Test
    void startTableAccepted() throws DataSetException {
        startDataSet();

        ITableMetaData actorMetaData = tinySakilaDataSet.getTableMetaData("actor");
        filteredTableConsumer.startTable(actorMetaData);
        verify(dataSetConsumer, times(1)).startTable(actorMetaData);
    }

    @Test
    void startTableSkipped() throws DataSetException {
        startDataSet();

        ITableMetaData filmMetaData = tinySakilaDataSet.getTableMetaData("film");
        filteredTableConsumer.startTable(filmMetaData);
        verify(dataSetConsumer, never()).startTable(Mockito.any(ITableMetaData.class));
    }

    @Test
    void rowsAccepted() throws DataSetException {
        startTableAccepted();

        ITable actorTable = tinySakilaDataSet.getTable("actor");
        TableUtil actorUtil = new TableUtil(actorTable);
        Object[] rowValues = actorUtil.getRow(0).toArray();
        filteredTableConsumer.row(rowValues);
        verify(dataSetConsumer, times(1)).row(rowValues);
    }

    @Test
    void rowsSkipped() throws DataSetException {
        startTableSkipped();

        ITable filmTable = tinySakilaDataSet.getTable("film");
        TableUtil filmUtil = new TableUtil(filmTable);
        Object[] rowValues = filmUtil.getRow(0).toArray();
        filteredTableConsumer.row(rowValues);
        verify(dataSetConsumer, never()).row(Mockito.any(Object[].class));
    }

    @Test
    void endTableAccepted() throws DataSetException {
        rowsAccepted();

        filteredTableConsumer.endTable();
        verify(dataSetConsumer, times(1)).endTable();
    }

    @Test
    void endTableSkipped() throws DataSetException {
        rowsSkipped();

        filteredTableConsumer.endTable();
        verify(dataSetConsumer, never()).endTable();
    }

    @Test
    void endDataSetAccepted() throws DataSetException {
        endTableAccepted();

        filteredTableConsumer.endDataSet();
        verify(dataSetConsumer, times(1)).endDataSet();
    }

    @Test
    void endDataSetSkipped() throws DataSetException {
        endTableSkipped();

        filteredTableConsumer.endDataSet();
        verify(dataSetConsumer, times(1)).endDataSet();
    }

}