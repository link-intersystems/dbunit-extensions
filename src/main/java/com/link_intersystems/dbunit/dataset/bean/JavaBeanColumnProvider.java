package com.link_intersystems.dbunit.dataset.bean;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Arrays.stream;

public class JavaBeanColumnProvider implements BeanColumnProvider {

    private PropertyDataTypeMapping defaultPropertyDataTypeMapping = new DefaultPropertyDataTypeMapping();
    private Map<Class<?>, PropertyDataTypeMapping> propertyDataTypeMappingRegistry = new HashMap<>();

    @Override
    public Column[] getColumns(Class<?> beanClass) throws Exception {
        BeanInfo beanInfo = Introspector.getBeanInfo(beanClass, Object.class);
        PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
        return stream(propertyDescriptors).map(this::toColumn).filter(Objects::nonNull).toArray(Column[]::new);
    }

    private Column toColumn(PropertyDescriptor pd) {
        String name = pd.getName();

        Class<?> declaredType = getDeclaredType(pd);
        PropertyDataTypeMapping propertyDataTypeMapping = getPropertyDataTypeMapping(declaredType);

        DataType dataType = propertyDataTypeMapping.getDataType(pd);
        if (dataType == null) {
            return null;
        }

        return new Column(name, dataType);
    }

    private PropertyDataTypeMapping getPropertyDataTypeMapping(Class<?> declaredType) {
        return Optional.ofNullable(propertyDataTypeMappingRegistry.get(declaredType)).orElse(defaultPropertyDataTypeMapping);
    }

    private Class<?> getDeclaredType(PropertyDescriptor pd) {
        Method readMethod = pd.getReadMethod();
        if (readMethod != null) {
            return readMethod.getDeclaringClass();
        }

        Method writeMethod = pd.getWriteMethod();
        return writeMethod.getDeclaringClass();
    }
}
