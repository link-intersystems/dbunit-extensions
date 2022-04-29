package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.lang.Primitives;
import com.link_intersystems.lang.reflect.Constants;
import org.dbunit.dataset.datatype.DataType;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultPropertyTypeRegistry implements PropertyTypeRegistry {

    public static final PropertyType UNKNOWN_VALUE_CONVERTER = new DataTypePropertyType(DataType.UNKNOWN);

    private Map<Class<?>, PropertyType> propertyTypes = new HashMap<>();

    public DefaultPropertyTypeRegistry() {
        Constants<DataType> dataTypeConstants = new Constants<>(DataType.class);

        Map<Class<?>, DataType> mostGenericTypes = new HashMap<>();

        for (DataType dataType : dataTypeConstants) {
            Class typeClass = dataType.getTypeClass();

            DataType existentType = mostGenericTypes.get(typeClass);
            if (existentType == null || isMoreGeneric(dataType, existentType)) {
                mostGenericTypes.put(typeClass, dataType);
            }
        }

        mostGenericTypes.values().stream().forEach(this::registerPropertyType);
    }

    private boolean isMoreGeneric(DataType dataType, DataType compareToDataType) {
        return dataType.getClass().isAssignableFrom(compareToDataType.getTypeClass());
    }

    private void registerPropertyType(DataType dataType) {
        DataTypePropertyType valueConverter = new DataTypePropertyType(dataType);
        Class<?> typeClass = dataType.getTypeClass();
        propertyTypes.put(typeClass, valueConverter);
    }

    @Override
    public PropertyType getPropertyType(Class<?> targetType) {
        if (targetType.isPrimitive()) {
            targetType = Primitives.getWrapperType(targetType);
        }

        PropertyType propertyType = propertyTypes.get(targetType);

        if (propertyType == null) {
            Optional<PropertyType> accessibleConverter = findAssignablePropertyType(targetType);
            propertyType = accessibleConverter.orElse(UNKNOWN_VALUE_CONVERTER);
        }

        return propertyType;
    }


    private Optional<PropertyType> findAssignablePropertyType(Class<?> targetType) {
        Optional<PropertyType> accessibleValueConverter = Optional.empty();

        for (Map.Entry<Class<?>, PropertyType> valueConverterEntry : propertyTypes.entrySet()) {
            Class<?> converterTargetClass = valueConverterEntry.getKey();
            if (targetType.isAssignableFrom(converterTargetClass)) {
                PropertyType valueConverter = valueConverterEntry.getValue();
                accessibleValueConverter = Optional.of(valueConverter);
                break;
            }
        }

        return accessibleValueConverter;
    }
}
