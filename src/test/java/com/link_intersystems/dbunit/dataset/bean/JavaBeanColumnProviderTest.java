package com.link_intersystems.dbunit.dataset.bean;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import static java.text.MessageFormat.format;
import static java.util.Arrays.asList;
import static org.dbunit.dataset.datatype.DataType.UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private void assertContains(Column[] columns, String columnName, DataType dataType) {
        boolean contains = contains(columns, columnName, dataType);
        assertTrue(contains, () -> format("Excepted column {0} is not contained in {1}", columnName, asList(columns)));
    }

    private void assertNotContains(Column[] columns, String columnName) {
        boolean contains = contains(columns, columnName, UNKNOWN);
        assertFalse(contains, () -> format("Excepted column {0} is contained but should not be contained in {1}", columnName, asList(columns)));
    }

    private boolean contains(Column[] columns, String columnName, DataType dataType) {
        boolean contains = false;
        Column expectedColumn = new Column(columnName, dataType);

        for (int i = 0; i < columns.length && !contains; i++) {
            Column column = columns[i];
            contains = column.equals(expectedColumn);
        }
        return contains;
    }

}

abstract class PrimitiveTypesBean {

    public abstract byte getByteValue();

    public abstract short getShortValue();

    public abstract int getIntValue();

    public abstract long getLongValue();

    public abstract float getFloatValue();

    public abstract double getDoubleValue();

    public abstract boolean isBooleanValue();

    public abstract char getCharValue();
}

abstract class PrimitiveWrapperTypesBean {

    public abstract Byte getByteValue();

    public abstract Short getShortValue();

    public abstract Integer getIntValue();

    public abstract Long getLongValue();

    public abstract Float getFloatValue();

    public abstract Double getDoubleValue();

    public abstract Boolean getBooleanValue();

    public abstract Character getCharValue();
}

abstract class ObjectTypesBean {

    public abstract String getStringValue();

    public abstract CharSequence getCharSequenceValue();

    public abstract Date getDateValue();

    public abstract BigInteger getBigIntegerValue();

    public abstract BigDecimal getBigDecimalValue();

    public abstract PrimitiveTypesBean getUnknownType();
}