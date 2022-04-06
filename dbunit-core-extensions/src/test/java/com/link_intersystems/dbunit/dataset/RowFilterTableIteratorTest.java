package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.ComponentTest;
import com.link_intersystems.dbunit.dataset.beans.BeanList;
import com.link_intersystems.dbunit.dataset.beans.BeanTableIterator;
import com.link_intersystems.dbunit.dataset.beans.BeanTableMetaDataProvider;
import com.link_intersystems.dbunit.dataset.beans.java.JavaBeanTableMetaData;
import com.link_intersystems.dbunit.dataset.dbunit.dataset.bean.EmployeeBean;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.filter.IRowFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
@ComponentTest
class RowFilterTableIteratorTest {

    private IRowFilter rowFilter;
    private RowFilterTableIterator rowFilterTableIterator;
    private BeanList<EmployeeBean> beanList;

    @BeforeEach
    void setUp() {
        EmployeeBeanFixture employeeBeanFixture = new EmployeeBeanFixture();
        beanList = employeeBeanFixture.createBeanList();
        BeanTableIterator beanTableIterator = new BeanTableIterator(singletonList(beanList), JavaBeanTableMetaData::new);

        rowFilter = mock(IRowFilter.class);
        rowFilterTableIterator = new RowFilterTableIterator(beanTableIterator, t -> rowFilter);
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

        assertEquals("EmployeeBean", tableMetaData.getTableName());

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

    @Test
    void exceptionInBeanTableMetaDataProvider() throws Exception {
        BeanTableMetaDataProvider beanTableMetaDataProvider = mock(BeanTableMetaDataProvider.class);
        RuntimeException runtimeException = new RuntimeException();
        when(beanTableMetaDataProvider.getMetaData(any())).thenThrow(runtimeException);


        BeanTableIterator beanTableIterator = new BeanTableIterator(singletonList(beanList), beanTableMetaDataProvider);
        beanTableIterator.next();
        DataSetException dataSetException = assertThrows(DataSetException.class, beanTableIterator::getTable);

        assertSame(runtimeException, dataSetException.getCause());
    }
}