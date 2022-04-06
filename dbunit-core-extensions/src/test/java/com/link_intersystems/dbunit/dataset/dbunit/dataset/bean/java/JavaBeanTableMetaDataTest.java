package com.link_intersystems.dbunit.dataset.dbunit.dataset.bean.java;

import com.link_intersystems.UnitTest;
import com.link_intersystems.dbunit.dataset.ColumnList;
import com.link_intersystems.dbunit.dataset.beans.AbstractBeanTableMetaData;
import com.link_intersystems.dbunit.dataset.beans.java.JavaBeanTableMetaData;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@UnitTest
class JavaBeanTableMetaDataTest {

    private AbstractBeanTableMetaData tableMetaData;

    @BeforeEach
    public void setUp() throws IntrospectionException {
        tableMetaData = new JavaBeanTableMetaData(PrimitiveTypesBean.class);
    }

    @Test
    void getTableName() {
        assertEquals("PrimitiveTypesBean", tableMetaData.getTableName());
    }

    @Test
    void getColumns() {
        assertEquals(8, tableMetaData.getColumns().length);
    }

    @Test
    void getPrimaryKeys() {
        assertEquals(0, tableMetaData.getPrimaryKeys().length);
    }

    @Test
    void getPropertyValue() throws DataSetException {
        PrimitiveTypesBean primitiveTypesBean = new PrimitiveTypesBean();
        primitiveTypesBean.setIntValue(1234);


        ColumnList columnList = tableMetaData.getColumnList();

        Column intValueColumn = columnList.getColumn("intValue");
        Object value = tableMetaData.getValue(primitiveTypesBean, intValueColumn);
        assertEquals(1234, value);
    }
}