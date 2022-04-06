package com.link_intersystems.dbunit.dataset.beans.java;

import com.link_intersystems.dbunit.dataset.beans.PropertyDataTypeMapping;
import org.dbunit.dataset.datatype.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.dbunit.dataset.datatype.DataType.*;

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
    public DataType getDataType(Class<?> javaType) {
        return dataTypeMap.get(javaType);
    }
}