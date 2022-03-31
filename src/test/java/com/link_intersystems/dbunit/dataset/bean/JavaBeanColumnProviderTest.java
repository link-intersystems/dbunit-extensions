package com.link_intersystems.dbunit.dataset.bean;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import static com.link_intersystems.dbunit.dataset.ColumnAssertions.assertContains;
import static com.link_intersystems.dbunit.dataset.ColumnAssertions.assertNotContains;

class JavaBeanColumnProviderTest {


    private JavaBeanColumnProvider columnProvider;

    @BeforeEach
    public void setup() {
        columnProvider = new JavaBeanColumnProvider();
    }

    @Test
    void primitiveTypes() throws Exception {
        Column[] columns = columnProvider.getColumns(PrimitiveTypesBean.class);

        assertContains(columns, "byteValue", DataType.TINYINT);
        assertContains(columns, "shortValue", DataType.SMALLINT);
        assertContains(columns, "intValue", DataType.INTEGER);
        assertContains(columns, "longValue", DataType.BIGINT);
        assertContains(columns, "floatValue", DataType.FLOAT);
        assertContains(columns, "doubleValue", DataType.DOUBLE);
        assertContains(columns, "booleanValue", DataType.BOOLEAN);
        assertContains(columns, "charValue", DataType.CHAR);

    }


    @Test
    void primitiveWrapperTypes() throws Exception {
        Column[] columns = columnProvider.getColumns(PrimitiveWrapperTypesBean.class);

        assertContains(columns, "byteValue", DataType.TINYINT);
        assertContains(columns, "shortValue", DataType.SMALLINT);
        assertContains(columns, "intValue", DataType.INTEGER);
        assertContains(columns, "longValue", DataType.BIGINT);
        assertContains(columns, "floatValue", DataType.FLOAT);
        assertContains(columns, "doubleValue", DataType.DOUBLE);
        assertContains(columns, "booleanValue", DataType.BOOLEAN);
        assertContains(columns, "charValue", DataType.CHAR);
    }

    @Test
    void objectTypes() throws Exception {
        Column[] columns = columnProvider.getColumns(ObjectTypesBean.class);

        assertContains(columns, "stringValue", DataType.VARCHAR);
        assertContains(columns, "charSequenceValue", DataType.CLOB);
        assertContains(columns, "dateValue", DataType.DATE);
        assertContains(columns, "bigIntegerValue", DataType.BIGINT);
        assertContains(columns, "bigDecimalValue", DataType.DECIMAL);

        assertNotContains(columns, "unknownType");
    }

}

