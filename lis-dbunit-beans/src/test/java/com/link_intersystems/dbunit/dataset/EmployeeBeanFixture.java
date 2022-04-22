package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.beans.*;
import com.link_intersystems.beans.java.JavaBeanClass;
import com.link_intersystems.dbunit.dataset.beans.BeanList;
import com.link_intersystems.dbunit.dataset.beans.BeanTableMetaDataProvider;
import com.link_intersystems.dbunit.dataset.beans.DefaultBeanTableMetaDataProvider;
import com.link_intersystems.dbunit.dataset.dbunit.dataset.bean.DepartmentBean;
import com.link_intersystems.dbunit.dataset.dbunit.dataset.bean.EmployeeBean;

import java.beans.IntrospectionException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class EmployeeBeanFixture {
    private BeanClass<EmployeeBean> employeeBeanClass;

    public EmployeeBeanFixture() {
        try {
            employeeBeanClass = new JavaBeanClass<>(EmployeeBean.class);
        } catch (IntrospectionException e) {
            throw new RuntimeException(e);
        }
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
