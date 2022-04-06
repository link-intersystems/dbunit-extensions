package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.dbunit.dataset.beans.BeanList;
import com.link_intersystems.dbunit.dataset.dbunit.dataset.bean.EmployeeBean;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class EmployeeBeanFixture {
    BeanList<EmployeeBean> createBeanList() {
        List<EmployeeBean> employeeBeans = asList(EmployeeBean.blake(), EmployeeBean.king());
        return new BeanList<>(EmployeeBean.class, employeeBeans);
    }
}
