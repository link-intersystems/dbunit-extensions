package com.link_intersystems.dbunit.dataset.bean.java;

import com.link_intersystems.beans.Property;
import com.link_intersystems.beans.java.JavaBeanClass;
import com.link_intersystems.beans.java.JavaBeanProperty;
import com.link_intersystems.dbunit.dataset.bean.BeanTableMetaData;
import com.link_intersystems.dbunit.dataset.bean.PropertyDataTypeMapping;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.DataType;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
public class JavaBeanTableMetaData extends BeanTableMetaData {

    private final JavaBeanClass beanClass;
    private final PropertyDataTypeMapping propertyDataTypeMapping;

    public JavaBeanTableMetaData(Class<?> beanClass) throws IntrospectionException {
        this(beanClass, DefaultJavaBeanPropertyDataTypeMapping.INSTANCE);
    }

    public JavaBeanTableMetaData(Class<?> beanClass, PropertyDataTypeMapping propertyDataTypeMapping) throws IntrospectionException {
        this.beanClass = new JavaBeanClass(beanClass);
        this.propertyDataTypeMapping = requireNonNull(propertyDataTypeMapping);
    }

    @Override
    public JavaBeanClass getBeanClass() {
        return beanClass;
    }

    @Override
    protected DataType getDataType(Property property) {
        PropertyDescriptor pd = getPropertyDescriptor(property);
        Class<?> propertyType = pd.getPropertyType();
        return propertyDataTypeMapping.getDataType(propertyType);
    }

    private PropertyDescriptor getPropertyDescriptor(Property property) {
        JavaBeanProperty javaBeanProperty = (JavaBeanProperty) property;
        return javaBeanProperty.getPropertyDescriptor();
    }

    @Override
    protected Object getValue(Object bean, Property property) throws DataSetException {
        PropertyDescriptor pd = getPropertyDescriptor(property);
        Method readMethod = pd.getReadMethod();

        try {
            makeAccessible(bean, readMethod);
            return readMethod.invoke(bean);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new DataSetException(e);
        }
    }

    protected void makeAccessible(Object bean, Method readMethod) {
        if (!readMethod.canAccess(bean)) {
            readMethod.setAccessible(true);
        }
    }

    @Override
    protected boolean isIdentityProperty(Property property) {
        return false;
    }
}
