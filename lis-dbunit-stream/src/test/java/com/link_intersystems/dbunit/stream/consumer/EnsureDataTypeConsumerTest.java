package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.meta.ColumnListBuilder;
import com.link_intersystems.dbunit.meta.OngoingColumnBuild;
import com.link_intersystems.dbunit.meta.TableMetaDataBuilder;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class EnsureDataTypeConsumerTest {

    private ITableMetaData tableMetaData;
    private EnsureDataTypeConsumer ensureDataTypeConsumer;
    private IDataSetConsumer dataSetConsumer;

    @BeforeEach
    void setUp() {
        TableMetaDataBuilder testTableBuilder = new TableMetaDataBuilder("test");
        ColumnListBuilder columnListBuilder = new ColumnListBuilder();
        OngoingColumnBuild ongoingColumnBuild = columnListBuilder.newColumn("name", DataType.BIGINT);
        ongoingColumnBuild.build();
        testTableBuilder.setColumns(columnListBuilder.build().toArray());
        tableMetaData = testTableBuilder.build();

        ensureDataTypeConsumer = new EnsureDataTypeConsumer();
        dataSetConsumer = mock(IDataSetConsumer.class);
        ensureDataTypeConsumer.setSubsequentConsumer(dataSetConsumer);
    }

    @Test
    void row() throws DataSetException {
        ensureDataTypeConsumer.startDataSet();
        ensureDataTypeConsumer.startTable(tableMetaData);
        ensureDataTypeConsumer.row(new Object[]{"123456789"});

        ArgumentCaptor<Object[]> rowCaptor = ArgumentCaptor.forClass(Object[].class);
        verify(dataSetConsumer, times(1)).row(rowCaptor.capture());

        Object[] value = rowCaptor.getValue();
        assertEquals(new BigInteger("123456789"), value[0]);
    }
}