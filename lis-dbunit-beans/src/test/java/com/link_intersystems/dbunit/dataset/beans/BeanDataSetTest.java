package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClassException;
import com.link_intersystems.dbunit.dataset.beans.fixtures.EmployeeBeanFixture;
import com.link_intersystems.dbunit.dataset.beans.fixtures.DepartmentBean;
import com.link_intersystems.dbunit.dataset.beans.fixtures.EmployeeBean;
import com.link_intersystems.test.ComponentTest;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
@ComponentTest
class BeanDataSetTest {

    private BeanDataSet beanDataSet;
    private List<BeanList<?>> beanLists;

    @BeforeEach
    void setUp() throws BeanClassException {
        beanLists = new ArrayList<>();

        BeanList<EmployeeBean> employeeBeans1 = new BeanList<>(EmployeeBean.class, asList(EmployeeBean.king(), EmployeeBean.blake()));
        beanLists.add(employeeBeans1);

        BeanList<DepartmentBean> departmentBeans = new BeanList<>(DepartmentBean.class, asList(DepartmentBean.accounting(), DepartmentBean.sales(), DepartmentBean.research()));
        beanLists.add(departmentBeans);

        beanDataSet = new BeanDataSet(beanLists);
    }

    @Test
    public void oneBeanListPerBeanClass() throws DataSetException {
        BeanList<EmployeeBean> employeeBeans2 = new BeanList<>(EmployeeBean.class, asList(EmployeeBean.clark(), EmployeeBean.jones()));
        beanLists.add(employeeBeans2);
        beanDataSet = new BeanDataSet(beanLists);

        ITableIterator tableIterator = beanDataSet.iterator();

        assertNextEmployeeTable(tableIterator, 4);
        assertNextDepartmentTable(tableIterator,3);
    }

    @Test
    public void singletonSet() throws DataSetException {
        BeanList<EmployeeBean> employeeBeans = new BeanList<>(EmployeeBean.class, Collections.singletonList(EmployeeBean.clark()));
        beanDataSet = BeanDataSet.singletonSet(employeeBeans);

        ITableIterator tableIterator = beanDataSet.iterator();

        assertNextEmployeeTable(tableIterator, 1);
        assertFalse(tableIterator.next());
    }

    @Test
    public void iterator() throws DataSetException {
        ITableIterator tableIterator = beanDataSet.createIterator(false);

        assertNextEmployeeTable(tableIterator, 2);
        assertNextDepartmentTable(tableIterator, 3);
    }


    @Test
    public void reversedIterator() throws DataSetException {
        ITableIterator tableIterator = beanDataSet.createIterator(true);

        assertNextDepartmentTable(tableIterator, 3);
        assertNextEmployeeTable(tableIterator, 2);
    }

    private void assertNextEmployeeTable(ITableIterator tableIterator, int expectedEmployeeCount) throws DataSetException {
        assertNextTable(tableIterator, expectedEmployeeCount);
        ITable table = tableIterator.getTable();
        assertEquals("EmployeeBean", table.getTableMetaData().getTableName());
    }

    private void assertNextDepartmentTable(ITableIterator tableIterator, int expectedDepartmentCount) throws DataSetException {
        assertNextTable(tableIterator, expectedDepartmentCount);
        ITable table = tableIterator.getTable();
        assertEquals("DepartmentBean", table.getTableMetaData().getTableName());
    }

    private void assertNextTable(ITableIterator tableIterator, int expected) throws DataSetException {
        assertTrue(tableIterator.next());
        ITable table = tableIterator.getTable();
        assertNotNull(table);
        assertEquals(expected, table.getRowCount());
    }
}