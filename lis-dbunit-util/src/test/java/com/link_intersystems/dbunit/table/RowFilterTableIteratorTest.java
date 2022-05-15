package com.link_intersystems.dbunit.table;

import com.link_intersystems.dbunit.ComponentTest;
import org.dbunit.dataset.*;
import org.dbunit.dataset.filter.IRowFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
@ComponentTest
class RowFilterTableIteratorTest {

    private IRowFilter rowFilter;
    private RowFilterTableIterator rowFilterTableIterator;

    @BeforeEach
    void setUp() throws DataSetException {
        DefaultDataSet beanDataSet = new DefaultDataSet();
        DefaultTable employeeTable = new DefaultTable("Employee");
        beanDataSet.addTable(employeeTable);
        employeeTable.addRow(new Object[]{});
        employeeTable.addRow(new Object[]{});
        rowFilter = Mockito.mock(IRowFilter.class);
        rowFilterTableIterator = new RowFilterTableIterator(beanDataSet.iterator(), t -> rowFilter);
    }

    @Test
    void next() throws DataSetException {
        assertTrue(rowFilterTableIterator.next());
        assertFalse(rowFilterTableIterator.next());
    }

    @Test
    void getTableMetaData() throws DataSetException {
        assertTrue(rowFilterTableIterator.next());

        ITableMetaData tableMetaData = rowFilterTableIterator.getTableMetaData();
        assertNotNull(tableMetaData);

        assertEquals("Employee", tableMetaData.getTableName());

    }

    @Test
    void getTableNoFilter() throws DataSetException {
        assertTrue(rowFilterTableIterator.next());
        when(rowFilter.accept(any())).thenReturn(true);

        ITable table = rowFilterTableIterator.getTable();
        assertNotNull(table);

        assertEquals(2, table.getRowCount());
    }

    @Test
    void getTableWithFilter() throws DataSetException {
        assertTrue(rowFilterTableIterator.next());
        when(rowFilter.accept(any())).thenReturn(true).thenReturn(false);

        ITable table = rowFilterTableIterator.getTable();
        assertNotNull(table);

        assertEquals(1, table.getRowCount());
    }

}