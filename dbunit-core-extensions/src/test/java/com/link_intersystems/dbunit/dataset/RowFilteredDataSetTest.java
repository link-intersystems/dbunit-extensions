package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.ComponentTest;
import com.link_intersystems.beans.BeanClassException;
import com.link_intersystems.dbunit.dataset.beans.BeanDataSet;
import com.link_intersystems.dbunit.dataset.beans.BeanList;
import com.link_intersystems.dbunit.dataset.beans.BeanTableMetaDataProvider;
import com.link_intersystems.dbunit.dataset.dbunit.dataset.bean.EmployeeBean;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.filter.IRowFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ComponentTest
class RowFilteredDataSetTest {

    private IRowFilter rowFilter;
    private BeanDataSet beanDataSet;

    @BeforeEach
    void setUp() throws BeanClassException {
        EmployeeBeanFixture employeeBeanFixture = new EmployeeBeanFixture();
        BeanList<EmployeeBean> beanList = employeeBeanFixture.createBeanList();
        BeanTableMetaDataProvider beanMetaDataProvider = employeeBeanFixture.createBeanMetaDataProvider();
        beanDataSet = BeanDataSet.singletonSet(beanList, beanMetaDataProvider);

        rowFilter = mock(IRowFilter.class);
    }

    @Test
    void unfiltered() throws DataSetException {
        when(rowFilter.accept(any())).thenReturn(true);
        RowFilteredDataSet rowFilteredDataSet = new RowFilteredDataSet(beanDataSet, t -> rowFilter);

        ITable employeeTable = rowFilteredDataSet.getTable("EmployeeBean");

        assertEquals(2, employeeTable.getRowCount());
    }

    @Test
    void filtered() throws DataSetException {
        when(rowFilter.accept(any())).thenReturn(true).thenReturn(false);
        RowFilteredDataSet rowFilteredDataSet = new RowFilteredDataSet(beanDataSet, t -> rowFilter);

        ITable employeeTable = rowFilteredDataSet.getTable("EmployeeBean");

        assertEquals(1, employeeTable.getRowCount());
    }
}