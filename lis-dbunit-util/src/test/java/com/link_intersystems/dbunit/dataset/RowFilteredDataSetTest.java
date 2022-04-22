package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.ComponentTest;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
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
    private DefaultDataSet beanDataSet;

    @BeforeEach
    void setUp() throws DataSetException {
        beanDataSet = new DefaultDataSet();
        DefaultTable employeeTable = new DefaultTable("EmployeeBean");
        beanDataSet.addTable(employeeTable);
        employeeTable.addRow(new Object[]{});
        employeeTable.addRow(new Object[]{});

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