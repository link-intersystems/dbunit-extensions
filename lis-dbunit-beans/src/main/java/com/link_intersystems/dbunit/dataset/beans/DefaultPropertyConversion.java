package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.PropertyDesc;
import com.link_intersystems.lang.ClassHierarchyComparator;
import com.link_intersystems.lang.Primitives;
import com.link_intersystems.lang.reflect.Constants;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.dbunit.dataset.datatype.DataType.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultPropertyConversion implements PropertyConversion {


    public static final ValueConverter UNKNOWN_VALUE_CONVERTER = new DataTypeValueConverter(DataType.UNKNOWN);

    private Map<Class<?>, DataType> dataTypeMap;
    private Map<Class<?>, DataTypeValueConverter> valueConverters = new HashMap<>();

    public DefaultPropertyConversion() {
        Constants<DataType> dataTypeConstants = new Constants<>(DataType.class);
        dataTypeConstants.stream().sorted(ClassHierarchyComparator.objectsComparator()).forEach(this::registerDataTypeConverter);
    }


    protected Map<Class<?>, DataType> getDataTypeMap() {
        if(dataTypeMap == null){
            dataTypeMap = initDataTypeMap();
        }
        return dataTypeMap;
    }

    protected Map<Class<?>, DataType> initDataTypeMap() {
        return new DefaultDataTypeMap();
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
        Class<?> propertyType = propertyDesc.getType();
        ValueConverter valueConverter = getValueConverter(propertyType);
        try {
            return valueConverter.convert(columnValue);
        } catch (TypeConversionException e) {
            throw new TypeCastException(e);
        }
    }


    public DataType getDataType(BeanClass<?> beanClass, PropertyDesc property) {
        Class<?> javaType = property.getType();
        return getDataTypeMap().get(javaType);
    }

    private void registerDataTypeConverter(DataType dataType) {
        DataTypeValueConverter valueConverter = new DataTypeValueConverter(dataType);
        Class<?> typeClass = dataType.getTypeClass();
        valueConverters.put(typeClass, valueConverter);
    }

    public ValueConverter getValueConverter(Class<?> targetType) {
        if (targetType.isPrimitive()) {
            targetType = Primitives.getWrapperType(targetType);
        }

        ValueConverter valueConverter = valueConverters.get(targetType);

        if (valueConverter == null) {
            Optional<ValueConverter> accessibleConverter = findAccessibleConverter(targetType);
            valueConverter = accessibleConverter.orElse(UNKNOWN_VALUE_CONVERTER);
        }

        return valueConverter;
    }

    private Optional<ValueConverter> findAccessibleConverter(Class<?> targetType) {
        Optional<ValueConverter> accessibleValueConverter = Optional.empty();

        for (Map.Entry<Class<?>, DataTypeValueConverter> valueConverterEntry : valueConverters.entrySet()) {
            Class<?> converterTargetClass = valueConverterEntry.getKey();
            if (targetType.isAssignableFrom(converterTargetClass)) {
                ValueConverter valueConverter = valueConverterEntry.getValue();
                accessibleValueConverter = Optional.of(valueConverter);
                break;
            }
        }

        return accessibleValueConverter;
    }
}
