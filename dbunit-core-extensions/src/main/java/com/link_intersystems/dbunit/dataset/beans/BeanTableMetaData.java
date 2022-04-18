package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.PropertyDesc;
import com.link_intersystems.beans.PropertyReadException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.DataType;

import static java.util.Objects.requireNonNull;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BeanTableMetaData extends AbstractBeanTableMetaData {

    private final BeanClass<?> beanClass;
    private final PropertyDataTypeMapping propertyDataTypeMapping;

    public BeanTableMetaData(BeanClass<?> beanClass) {
        this(beanClass, DefaultPropertyDataTypeMapping.INSTANCE);
    }

    public BeanTableMetaData(BeanClass<?> beanClass, PropertyDataTypeMapping propertyDataTypeMapping) {
        this.beanClass = beanClass;
        this.propertyDataTypeMapping = requireNonNull(propertyDataTypeMapping);
    }

    @Override
    public BeanClass<?> getBeanClass() {
        return beanClass;
    }

    @Override
    protected DataType getDataType(PropertyDesc property) {
        return propertyDataTypeMapping.getDataType(getBeanClass(), property);
    }

    @Override
    protected Object getValue(Object bean, PropertyDesc propertyDesc) throws DataSetException {
        try {
            return propertyDesc.getPropertyValue(bean);
        } catch (PropertyReadException e) {
            throw new DataSetException(e);
        }
    }
}
