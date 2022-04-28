package com.link_intersystems.dbunit.dataset.beans;

import org.dbunit.dataset.datatype.DataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.dbunit.dataset.datatype.DataType.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDataTypeRegistry implements DataTypeRegistry {

    private Map<Class<?>, DataType> dataTypes = new HashMap<>();

    public DefaultDataTypeRegistry() {
        configurePrimitiveTypes();

        registerDataType(String.class, VARCHAR);
        registerDataType(CharSequence.class, CLOB);
        registerDataType(Date.class, DATE);
        registerDataType(BigInteger.class, BIGINT);
        registerDataType(BigDecimal.class, DECIMAL);
    }

    private void configurePrimitiveTypes() {
        registerDataType(Byte.TYPE, TINYINT);
        registerDataType(Byte.class, TINYINT);

        registerDataType(Short.TYPE, SMALLINT);
        registerDataType(Short.class, SMALLINT);

        registerDataType(Integer.TYPE, INTEGER);
        registerDataType(Integer.class, INTEGER);

        registerDataType(Long.TYPE, BIGINT);
        registerDataType(Long.class, BIGINT);

        registerDataType(Float.TYPE, FLOAT);
        registerDataType(Float.class, FLOAT);

        registerDataType(Double.TYPE, DOUBLE);
        registerDataType(Double.class, DOUBLE);

        registerDataType(Boolean.TYPE, BOOLEAN);
        registerDataType(Boolean.class, BOOLEAN);

        registerDataType(Character.TYPE, CHAR);
        registerDataType(Character.class, CHAR);
    }

    private void registerDataType(Class<?> clazz, DataType dataType) {
        dataTypes.put(clazz, dataType);
    }

    @Override
    public DataType getDataType(Class<?> targetType) {
        return dataTypes.get(targetType);
    }
}
