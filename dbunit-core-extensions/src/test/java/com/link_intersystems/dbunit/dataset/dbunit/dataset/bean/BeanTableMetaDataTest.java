package com.link_intersystems.dbunit.dataset.dbunit.dataset.bean;

import com.link_intersystems.ComponentTest;
import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.BeanClassException;
import com.link_intersystems.beans.BeansFactory;
import com.link_intersystems.dbunit.dataset.ColumnList;
import com.link_intersystems.dbunit.dataset.beans.IBeanTableMetaData;
import com.link_intersystems.dbunit.dataset.beans.BeanTableMetaData;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ComponentTest
class BeanTableMetaDataTest {

    private IBeanTableMetaData tableMetaData;

    @BeforeEach
    public void setUp() throws IntrospectionException, BeanClassException {
        BeanClass<PrimitiveTypesBean> beanClass = BeansFactory.getDefault().createBeanClass(PrimitiveTypesBean.class);
        tableMetaData = new BeanTableMetaData(beanClass);
    }

    @Test
    void getTableName() {
        assertEquals("PrimitiveTypesBean", tableMetaData.getTableName());
    }

    @Test
    void getColumns() throws DataSetException {
        assertEquals(8, tableMetaData.getColumns().length);
    }

    @Test
    void getPrimaryKeys() throws DataSetException {
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