package com.link_intersystems.dbunit.dataset.beans.java;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.PropertyDesc;
import com.link_intersystems.dbunit.dataset.beans.PropertyDataTypeMapping;
import org.dbunit.dataset.datatype.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.dbunit.dataset.datatype.DataType.*;

/**
 * Maps Java bean property types to dbunit {@link DataType}s.
 *
 * Implemented mappings:
 *
 * <ul>
 *     <li>{@link Byte} => TINYINT</li>
 *     <li>{@link Short} => SMALLINT</li>
 *     <li>{@link Integer} => INTEGER</li>
 *     <li>{@link Long} => BIGINT</li>
 *     <li>{@link Float} => FLOAT</li>
 *     <li>{@link Double} => DOUBLE</li>
 *     <li>{@link Boolean} => BOOLEAN</li>
 *     <li>{@link Character} => CHAR</li>
 *
 *     <li>{@link String} => VARCHAR</li>
 *     <li>{@link CharSequence} => CLOB</li>
 *     <li>{@link Date} => DATE</li>
 *     <li>{@link BigInteger} => BIGINT</li>
 *     <li>{@link BigDecimal} => DECIMAL</li>
 * </ul>
 */
public class DefaultJavaBeanPropertyDataTypeMapping implements PropertyDataTypeMapping {

    public static final PropertyDataTypeMapping INSTANCE = new DefaultJavaBeanPropertyDataTypeMapping();

    private Map<Class<?>, DataType> dataTypeMap = new HashMap<>();

    DefaultJavaBeanPropertyDataTypeMapping() {
        configurePrimitiveTypes();

        dataTypeMap.put(String.class, VARCHAR);
        dataTypeMap.put(CharSequence.class, CLOB);
        dataTypeMap.put(Date.class, DATE);
        dataTypeMap.put(BigInteger.class, BIGINT);
        dataTypeMap.put(BigDecimal.class, DECIMAL);
    }

    private void configurePrimitiveTypes() {
        dataTypeMap.put(Byte.TYPE, TINYINT);
        dataTypeMap.put(Byte.class, TINYINT);

        dataTypeMap.put(Short.TYPE, SMALLINT);
        dataTypeMap.put(Short.class, SMALLINT);

        dataTypeMap.put(Integer.TYPE, INTEGER);
        dataTypeMap.put(Integer.class, INTEGER);

        dataTypeMap.put(Long.TYPE, BIGINT);
        dataTypeMap.put(Long.class, BIGINT);

        dataTypeMap.put(Float.TYPE, FLOAT);
        dataTypeMap.put(Float.class, FLOAT);

        dataTypeMap.put(Double.TYPE, DOUBLE);
        dataTypeMap.put(Double.class, DOUBLE);

        dataTypeMap.put(Boolean.TYPE, BOOLEAN);
        dataTypeMap.put(Boolean.class, BOOLEAN);

        dataTypeMap.put(Character.TYPE, CHAR);
        dataTypeMap.put(Character.class, CHAR);
    }

    @Override
    public DataType getDataType(BeanClass<?> beanClass, PropertyDesc<?> property) {
        Class<?> javaType = property.getType();
        return dataTypeMap.get(javaType);
    }
}
