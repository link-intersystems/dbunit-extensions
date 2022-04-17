package com.link_intersystems.dbunit.dataset.beans.java;

import com.link_intersystems.beans.*;
import com.link_intersystems.beans.java.JavaBean;
import com.link_intersystems.beans.java.JavaBeanClass;
import com.link_intersystems.dbunit.dataset.beans.AbstractBeanTableMetaData;
import com.link_intersystems.dbunit.dataset.beans.PropertyDataTypeMapping;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.DataType;

import static java.util.Objects.requireNonNull;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JavaBeanTableMetaData extends AbstractBeanTableMetaData {

    private final BeanClass<Object> beanClass;
    private final PropertyDataTypeMapping propertyDataTypeMapping;

    public JavaBeanTableMetaData(Class<?> beanClass) {
        this(beanClass, DefaultJavaBeanPropertyDataTypeMapping.INSTANCE);
    }

    public JavaBeanTableMetaData(Class<?> beanClass, PropertyDataTypeMapping propertyDataTypeMapping) {
        this.beanClass = JavaBeanClass.get((Class<Object>) beanClass);
        this.propertyDataTypeMapping = requireNonNull(propertyDataTypeMapping);
    }

    @Override
    public BeanClass<Object> getBeanClass() {
        return beanClass;
    }

    @Override
    protected DataType getDataType(PropertyDesc<?> property) {
        return propertyDataTypeMapping.getDataType(getBeanClass(), property);
    }

    @Override
    protected Object getValue(Object bean, PropertyDesc<?> propertyDesc) throws DataSetException {
        try {
            Bean<Object> javaBean = getBeanClass().getBean(bean);
            Property<?> property = javaBean.getProperty(propertyDesc);
            return property.getValue();
        } catch (PropertyAccessException e) {
            throw new DataSetException(e);
        }
    }
}
