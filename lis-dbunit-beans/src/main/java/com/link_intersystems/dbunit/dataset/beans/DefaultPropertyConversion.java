package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.PropertyDesc;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.DataType;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultPropertyConversion implements PropertyConversion {

    private DataTypeRegistry dataTypeRegistry;
    private PropertyTypeRegistry propertyTypeRegistry;

    public DefaultPropertyConversion() {
        this(new DefaultDataTypeRegistry(), new DefaultPropertyTypeRegistry());
    }

    public DefaultPropertyConversion(DataTypeRegistry dataTypeRegistry, PropertyTypeRegistry propertyTypeRegistry) {
        this.dataTypeRegistry = requireNonNull(dataTypeRegistry);
        this.propertyTypeRegistry = requireNonNull(propertyTypeRegistry);
    }

    @Override
    public DataType toDataType(BeanClass<?> beanClass, PropertyDesc propertyDesc) {
        return getDataType(beanClass, propertyDesc);
    }

    @Override
    public Object toColumnValue(Object propertyValue, Column column) throws DataSetException {
        DataType dataType = column.getDataType();
        return dataType.typeCast(propertyValue);
    }

    @Override
    public Object toPropertyValue(Object columnValue, PropertyDesc propertyDesc) throws DataSetException {
        Class<?> propertyClass = propertyDesc.getType();
        PropertyType propertyType = propertyTypeRegistry.getPropertyType(propertyClass);
        return propertyType.typeCast(columnValue);
    }


    public DataType getDataType(BeanClass<?> beanClass, PropertyDesc property) {
        Class<?> targetType = property.getType();
        return dataTypeRegistry.getDataType(targetType);
    }


}
