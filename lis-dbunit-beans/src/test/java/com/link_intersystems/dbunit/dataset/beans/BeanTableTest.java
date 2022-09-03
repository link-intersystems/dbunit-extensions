package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.dbunit.meta.ColumnList;
import com.link_intersystems.test.UnitTest;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
@UnitTest
class BeanTableTest {

    private IBeanTableMetaData metaDataProvider;
    private BeanTable beanTable;
    private Column valueColumn;

    @BeforeEach
    public void setUp() {
        BeanList<String> beanList = new BeanList<>(String.class, Arrays.asList("A", "B"));
        metaDataProvider = mock(BeanTableMetaData.class);

        valueColumn = new Column("value", DataType.VARCHAR);
        beanTable = new BeanTable(beanList, metaDataProvider);
    }

    @Test
    void getTableMetaData() {
        when(metaDataProvider.getTableName()).thenReturn("test");

        ITableMetaData tableMetaData = beanTable.getTableMetaData();

        assertEquals("test", tableMetaData.getTableName());
    }

    @Test
    void getRowCount() {

        assertEquals(2, beanTable.getRowCount());
    }

    @Test
    void getValue() throws DataSetException {
        when(metaDataProvider.getColumnList()).thenReturn(new ColumnList(valueColumn));
        when(metaDataProvider.getValue("B", valueColumn)).thenReturn("B");

        Object value = beanTable.getValue(1, "value");

        assertEquals("B", value);
    }
}