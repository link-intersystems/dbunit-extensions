package com.link_intersystems.dbunit.dataset;

import com.link_intersystems.beans.Property;
import com.link_intersystems.beans.PropertyAccessException;
import com.link_intersystems.beans.PropertyDescs;
import com.link_intersystems.beans.java.JavaBean;
import com.link_intersystems.beans.java.JavaBeanClass;
import com.link_intersystems.beans.java.JavaPropertyDesc;
import com.link_intersystems.dbunit.dataset.beans.BeanList;
import com.link_intersystems.dbunit.dataset.beans.BeanTableMetaDataProvider;
import com.link_intersystems.dbunit.dataset.beans.java.JavaBeanTableMetaDataProvider;
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
    private JavaBeanClass<EmployeeBean> employeeBeanClass;

    public EmployeeBeanFixture() {
        employeeBeanClass = JavaBeanClass.get(EmployeeBean.class);
    }

    public JavaBeanClass<EmployeeBean> getEmployeeBeanClass() {
        return employeeBeanClass;
    }

    public BeanList<EmployeeBean> createBeanList() {
        List<EmployeeBean> employeeBeans = asList(EmployeeBean.blake(), EmployeeBean.king());
        return new BeanList<>(EmployeeBean.class, employeeBeans);
    }

    public BeanTableMetaDataProvider createBeanMetaDataProvider() throws IntrospectionException {
        JavaBeanTableMetaDataProvider metaDataProvider = new JavaBeanTableMetaDataProvider();
        metaDataProvider.registerBeanClass(EmployeeBean.class);
        metaDataProvider.registerBeanClass(DepartmentBean.class);
        return metaDataProvider;
    }


    public Object[] toRow(EmployeeBean employee) throws PropertyAccessException {
        PropertyDescs<JavaPropertyDesc<?>> properties = employeeBeanClass.getProperties();
        List<Object> values = new ArrayList<>();

        JavaBean<EmployeeBean> bean = employeeBeanClass.getBean(employee);
        for (JavaPropertyDesc<?> propertyDesc : properties) {
            if (propertyDesc.getName().equals("class")) {
                continue;
            }
            Property<?> property = bean.getProperty(propertyDesc);
            Object value = property.getValue();
            values.add(value);
        }

        return values.toArray();
    }
}
