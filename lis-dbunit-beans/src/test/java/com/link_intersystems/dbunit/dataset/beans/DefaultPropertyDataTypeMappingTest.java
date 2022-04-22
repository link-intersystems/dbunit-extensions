package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.PropertyDesc;
import com.link_intersystems.test.UnitTest;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.function.Supplier;

import static java.text.MessageFormat.format;
import static org.dbunit.dataset.datatype.DataType.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class DefaultPropertyDataTypeMappingTest {

    private DefaultPropertyDataTypeMapping propertyDataTypeMapping;

    private BeanClass<?> beanClass;
    private PropertyDesc property;

    @BeforeEach
    void setUp() {
        propertyDataTypeMapping = new DefaultPropertyDataTypeMapping();

        beanClass = Mockito.mock(BeanClass.class);
        property = Mockito.mock(PropertyDesc.class);
    }

    @Test
    void primitiveTypes() {
        assertDataTypeMapping(Byte.TYPE, DataType.TINYINT);
        assertDataTypeMapping(Byte.class, DataType.TINYINT);

        assertDataTypeMapping(Short.TYPE, SMALLINT);
        assertDataTypeMapping(Short.class, SMALLINT);

        assertDataTypeMapping(Integer.TYPE, INTEGER);
        assertDataTypeMapping(Integer.class, INTEGER);

        assertDataTypeMapping(Long.TYPE, BIGINT);
        assertDataTypeMapping(Long.class, BIGINT);

        assertDataTypeMapping(Float.TYPE, FLOAT);
        assertDataTypeMapping(Float.class, FLOAT);

        assertDataTypeMapping(Double.TYPE, DOUBLE);
        assertDataTypeMapping(Double.class, DOUBLE);

        assertDataTypeMapping(Boolean.TYPE, BOOLEAN);
        assertDataTypeMapping(Boolean.class, BOOLEAN);

        assertDataTypeMapping(Character.TYPE, CHAR);
        assertDataTypeMapping(Character.class, CHAR);
    }

    @Test
    void coommonTypes() {
        assertDataTypeMapping(String.class, DataType.VARCHAR);
        assertDataTypeMapping(CharSequence.class, DataType.CLOB);
        assertDataTypeMapping(Date.class, DataType.DATE);
        assertDataTypeMapping(BigInteger.class, DataType.BIGINT);
        assertDataTypeMapping(BigDecimal.class, DataType.DECIMAL);
    }

    private void assertDataTypeMapping(Class propertyType, DataType expectedDataType) {
        when(property.getType()).thenReturn(propertyType);
        DataType dataType = propertyDataTypeMapping.getDataType(beanClass, property);

        Supplier<String> msg = () -> format("Property of type ''{0}'' should be mapped to ''{1}''", propertyType.getSimpleName(), expectedDataType);
        assertEquals(expectedDataType, dataType, msg);
    }
}