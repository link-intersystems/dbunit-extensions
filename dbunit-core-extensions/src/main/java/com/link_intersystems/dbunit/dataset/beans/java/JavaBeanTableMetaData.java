package com.link_intersystems.dbunit.dataset.beans.java;

import com.link_intersystems.beans.Property;
import com.link_intersystems.beans.PropertyReadException;
import com.link_intersystems.beans.java.JavaBeanClass;
import com.link_intersystems.dbunit.dataset.beans.AbstractBeanTableMetaData;
import com.link_intersystems.dbunit.dataset.beans.PropertyDataTypeMapping;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.DataType;

import java.beans.IntrospectionException;

import static java.util.Objects.requireNonNull;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JavaBeanTableMetaData extends AbstractBeanTableMetaData {

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
        return propertyDataTypeMapping.getDataType(getBeanClass(), property);
    }

    @Override
    protected Object getValue(Object bean, Property property) throws DataSetException {
        try {
            return property.get(bean);
        } catch (PropertyReadException e) {
            throw new DataSetException(e);
        }
    }
}
