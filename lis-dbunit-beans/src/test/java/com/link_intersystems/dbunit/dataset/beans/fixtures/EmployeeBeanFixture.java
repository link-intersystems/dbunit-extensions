package com.link_intersystems.dbunit.dataset.beans.fixtures;

import com.link_intersystems.beans.*;
import com.link_intersystems.dbunit.dataset.beans.BeanList;
import com.link_intersystems.dbunit.dataset.beans.BeanTableMetaDataProvider;
import com.link_intersystems.dbunit.dataset.beans.DefaultBeanTableMetaDataProvider;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class EmployeeBeanFixture {
    private BeanClass<EmployeeBean> employeeBeanClass;

    public EmployeeBeanFixture() {
        BeansFactory beansFactory = BeansFactory.getDefault();
        employeeBeanClass = beansFactory.createBeanClass(EmployeeBean.class);
    }

    public BeanClass<EmployeeBean> getEmployeeBeanClass() {
        return employeeBeanClass;
    }

    public BeanList<EmployeeBean> createBeanList() {
        List<EmployeeBean> employeeBeans = asList(EmployeeBean.blake(), EmployeeBean.king());
        return new BeanList<>(EmployeeBean.class, employeeBeans);
    }

    public BeanTableMetaDataProvider createBeanMetaDataProvider() throws BeanClassException {
        DefaultBeanTableMetaDataProvider metaDataProvider = new DefaultBeanTableMetaDataProvider();
        metaDataProvider.registerBeanClass(EmployeeBean.class);
        metaDataProvider.registerBeanClass(DepartmentBean.class);
        return metaDataProvider;
    }


    public Object[] toRow(EmployeeBean employee) throws PropertyException {
        PropertyDescList properties = employeeBeanClass.getProperties();
        List<Object> values = new ArrayList<>();

        for (PropertyDesc propertyDesc : properties) {
            if (propertyDesc.getName().equals("class")) {
                continue;
            }
            Object value = propertyDesc.getPropertyValue(employee);
            values.add(value);
        }

        return values.toArray();
    }
}
