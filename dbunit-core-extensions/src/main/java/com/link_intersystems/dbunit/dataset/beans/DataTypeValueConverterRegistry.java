package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.lang.Primitives;
import com.link_intersystems.util.TypeConversionException;
import com.link_intersystems.util.ValueConverter;
import com.link_intersystems.util.ValueConverterRegistry;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.lang.reflect.Modifier.*;
import static java.util.Arrays.stream;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataTypeValueConverterRegistry implements ValueConverterRegistry {

    public static final ValueConverter UNKNOWN_VALUE_CONVERTER = createValueConverterAdapter(DataType.UNKNOWN);



    private Map<Class<?>, ValueConverter> valueConverters = new HashMap<>();

    public DataTypeValueConverterRegistry() {
        stream(DataType.class.getDeclaredFields())
                .filter(this::isPublicStaticFinal)
                .filter(this::isDataType)
                .map(this::getDataType)
                .map(DataType.class::cast)
                .forEach(this::registerDataTypeConverter);
    }

    private DataType getDataType(Field constant) {
        try {
            return (DataType) constant.get(DataType.class);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    private void registerDataTypeConverter(DataType dataType) {
        Class<?> typeClass = dataType.getTypeClass();
        ValueConverter valueConverter = createValueConverterAdapter(dataType);
        valueConverters.put(typeClass, valueConverter);
    }

    private static ValueConverter createValueConverterAdapter(DataType dataType) {
        return o -> {
            try {
                return dataType.typeCast(o);
            } catch (TypeCastException e) {
                throw new TypeConversionException(e);
            }
        };
    }

    private boolean isDataType(Field field) {
        return DataType.class.equals(field.getType());
    }

    private boolean isPublicStaticFinal(Member member) {
        int m = member.getModifiers();
        return isPublic(m) && isStatic(m) && isFinal(m);
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

        for (Map.Entry<Class<?>, ValueConverter> valueConverterEntry : valueConverters.entrySet()) {
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
