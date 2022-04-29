package com.link_intersystems.dbunit.dataset.beans;

import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataTypePropertyTypeTest {

    private DataType dataType;
    private DataTypePropertyType propertyType;

    @BeforeEach
    void setUp() {
        dataType = mock(DataType.class);
        propertyType = new DataTypePropertyType(dataType);
    }

    @Test
    void typeCast() throws TypeCastException, TypeConversionException {
        when(dataType.typeCast("123")).thenReturn(123L);

        Object result = propertyType.typeCast("123");

        assertEquals(123L, result);
    }

    @Test
    void typeCastException() throws TypeCastException {
        when(dataType.typeCast("123")).thenThrow(new TypeCastException(new RuntimeException()));

        assertThrows(TypeConversionException.class, () -> propertyType.typeCast("123"));
    }
}