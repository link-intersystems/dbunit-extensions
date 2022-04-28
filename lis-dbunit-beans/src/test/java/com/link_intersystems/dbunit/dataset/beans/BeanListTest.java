package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.dbunit.dataset.beans.fixtures.DepartmentBean;
import com.link_intersystems.dbunit.dataset.beans.fixtures.EmployeeBean;
import com.link_intersystems.test.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
@UnitTest
class BeanListTest {

    private BeanList<EmployeeBean> employeeBeans;

    @BeforeEach
    void setUp() {
        List<EmployeeBean> beans = asList(EmployeeBean.king(), EmployeeBean.blake());
        employeeBeans = new BeanList<>(EmployeeBean.class, beans);
    }


    @Test
    void getBeanClass() {
        assertEquals(EmployeeBean.class, employeeBeans.getBeanClass());
    }

    @Test
    void get() {
        assertEquals(EmployeeBean.blake(), employeeBeans.get(1));
    }

    @Test
    void size() {
        assertEquals(2, employeeBeans.size());
    }

    @Test
    void wrongElements() {
        List elements = new ArrayList<>();
        elements.add("A");

        assertThrows(IllegalArgumentException.class, () -> new BeanList<>(EmployeeBean.class, elements));
    }

    @Test
    void nullElementsAreNotWrong() {
        List elements = new ArrayList<>();
        elements.add(null);

        new BeanList<>(EmployeeBean.class, elements);
    }


    @Test
    void joinWrongTypes() {
        BeanList employeeBeans = new BeanList<>(EmployeeBean.class, Collections.emptyList());
        BeanList departmentBeans = new BeanList<>(DepartmentBean.class, Collections.emptyList());

        assertThrows(IllegalArgumentException.class, () -> employeeBeans.join(departmentBeans));
    }

    @Test
    void join() {
        BeanList<EmployeeBean> employeeBeans1 = new BeanList<>(EmployeeBean.class, Arrays.asList(EmployeeBean.blake(), EmployeeBean.king()));
        BeanList<EmployeeBean> employeeBeans2 = new BeanList<>(EmployeeBean.class, Arrays.asList(EmployeeBean.clark(), EmployeeBean.jones()));

        BeanList<EmployeeBean> joinedList = employeeBeans1.join(employeeBeans2);

        assertEquals(4, joinedList.size());

        assertTrue(joinedList.contains(EmployeeBean.blake()));
        assertTrue(joinedList.contains(EmployeeBean.king()));
        assertTrue(joinedList.contains(EmployeeBean.clark()));
        assertTrue(joinedList.contains(EmployeeBean.jones()));
    }
}