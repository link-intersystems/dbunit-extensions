package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.ComponentTest;
import com.link_intersystems.dbunit.dataset.beans.java.JavaBeanTableMetaData;
import com.link_intersystems.dbunit.dataset.dbunit.dataset.bean.DepartmentBean;
import com.link_intersystems.dbunit.dataset.dbunit.dataset.bean.EmployeeBean;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;

/**
 *  @author - René Link &lt;rene.link@link-intersystems.com&gt;
 */
@ComponentTest
class BeanDataSetTest {

    private BeanDataSet beanDataSet;

    @BeforeEach
    void setUp() {
        List<BeanList<?>> beanLists = new ArrayList<>();

        BeanList<EmployeeBean> employeeBeans = new BeanList<>(EmployeeBean.class, asList(EmployeeBean.king(), EmployeeBean.blake()));
        beanLists.add(employeeBeans);

        BeanList<DepartmentBean> departmentBeans = new BeanList<>(DepartmentBean.class, asList(DepartmentBean.accounting(), DepartmentBean.sales(), DepartmentBean.research()));
        beanLists.add(departmentBeans);

        beanDataSet = new BeanDataSet(beanLists, JavaBeanTableMetaData::new);
    }

    @Test
    public void iterator() throws DataSetException {

        ITableIterator tableIterator = beanDataSet.createIterator(false);

        assertNextEmployeeTable(tableIterator);
        assertNextDepartmentTable(tableIterator);
    }


    @Test
    public void reversedIterator() throws DataSetException {

        ITableIterator tableIterator = beanDataSet.createIterator(true);

        assertNextDepartmentTable(tableIterator);
        assertNextEmployeeTable(tableIterator);
    }

    private void assertNextEmployeeTable(ITableIterator tableIterator) throws DataSetException {
        assertNextTable(tableIterator, 2);
        ITable table = tableIterator.getTable();
        assertEquals("EmployeeBean", table.getTableMetaData().getTableName());
    }

    private void assertNextDepartmentTable(ITableIterator tableIterator) throws DataSetException {
        assertNextTable(tableIterator, 3);
        ITable table = tableIterator.getTable();
        assertEquals("DepartmentBean", table.getTableMetaData().getTableName());
    }

    private void assertNextTable(ITableIterator tableIterator, int expected) throws DataSetException {
        assertTrue(tableIterator.next());
        ITable employeeBeanTable = tableIterator.getTable();
        assertNotNull(employeeBeanTable);
        assertEquals(expected, employeeBeanTable.getRowCount());
    }
}