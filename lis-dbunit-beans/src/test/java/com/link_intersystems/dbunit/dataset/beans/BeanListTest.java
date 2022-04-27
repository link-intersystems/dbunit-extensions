package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.dbunit.dataset.beans.fixtures.EmployeeBean;
import com.link_intersystems.test.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
}