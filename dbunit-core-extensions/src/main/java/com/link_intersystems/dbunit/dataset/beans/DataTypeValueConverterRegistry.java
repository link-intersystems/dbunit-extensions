package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.lang.Constants;
import com.link_intersystems.lang.Primitives;
import com.link_intersystems.util.TypeConversionException;
import com.link_intersystems.util.ValueConverter;
import com.link_intersystems.util.ValueConverterRegistry;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link ValueConverterRegistry} that uses dbunit {@link DataType}s as {@link ValueConverter}s.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataTypeValueConverterRegistry implements ValueConverterRegistry {

    public static final ValueConverter UNKNOWN_VALUE_CONVERTER = new DataTypeValueConverter(DataType.UNKNOWN);

    private static class DataTypeValueConverter implements ValueConverter {

        private DataType dataType;

        public DataTypeValueConverter(DataType dataType) {
            this.dataType = dataType;
        }

        @Override
        public Object convert(Object source) throws TypeConversionException {
            try {
                return dataType.typeCast(source);
            } catch (TypeCastException e) {
                throw new TypeConversionException(e);
            }
        }

        public boolean isMoreGeneral(DataTypeValueConverter otherConverter) {
            Class<?> otherTypeClass = otherConverter.dataType.getTypeClass();
            Class<?> thisTypeClass = dataType.getTypeClass();
            return thisTypeClass.isAssignableFrom(otherTypeClass);
        }
    }


    private Map<Class<?>, DataTypeValueConverter> valueConverters = new HashMap<>();

    public DataTypeValueConverterRegistry() {
        Constants<DataType> dataTypeConstants = new Constants<>(DataType.class);
        dataTypeConstants.forEach(this::registerDataTypeConverter);
    }

    private void registerDataTypeConverter(DataType dataType) {
        DataTypeValueConverter valueConverter = new DataTypeValueConverter(dataType);

        Class<?> typeClass = dataType.getTypeClass();
        DataTypeValueConverter existingConverter = valueConverters.get(typeClass);
        if (existingConverter != null && valueConverter.isMoreGeneral(existingConverter)) {
            return;
        }

        valueConverters.put(typeClass, valueConverter);
    }

    @Override
    public ValueConverter getValueConverter(Class<?> targetType) {
        if (targetType.isPrimitive()) {
            targetType = Primitives.getWrapperType(targetType);
        }

        ValueConverter valueConverter = valueConverters.get(targetType);

        if (valueConverter == null) {
            Optional<ValueConverter> accessibleConverter = tryFindAccessibleConverter(targetType);
            valueConverter = accessibleConverter.orElse(UNKNOWN_VALUE_CONVERTER);
        }

        return valueConverter;
    }

    private Optional<ValueConverter> tryFindAccessibleConverter(Class<?> targetType) {
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
