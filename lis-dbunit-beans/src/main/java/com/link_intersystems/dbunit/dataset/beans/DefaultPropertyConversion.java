package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.PropertyDesc;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultPropertyConversion implements PropertyConversion {

    private ValueConverterRegistry valueConverterRegistry = new DataTypeValueConverterRegistry();
    private PropertyDataTypeMapping propertyDataTypeMapping = new DefaultPropertyDataTypeMapping();

    @Override
    public DataType toDataType(BeanClass<?> beanClass, PropertyDesc propertyDesc) {
        return propertyDataTypeMapping.getDataType(beanClass, propertyDesc);
    }

    @Override
    public Object toColumnValue(Object propertyValue, Column column) throws DataSetException {
        DataType dataType = column.getDataType();
        return dataType.typeCast(propertyValue);
    }

    @Override
    public Object toPropertyValue(Object columnValue, PropertyDesc propertyDesc) throws DataSetException {
        Class<?> propertyType = propertyDesc.getType();
        ValueConverter valueConverter = valueConverterRegistry.getValueConverter(propertyType);
        try {
            return valueConverter.convert(columnValue);
        } catch (TypeConversionException e) {
            throw new TypeCastException(e);
        }
    }
}
