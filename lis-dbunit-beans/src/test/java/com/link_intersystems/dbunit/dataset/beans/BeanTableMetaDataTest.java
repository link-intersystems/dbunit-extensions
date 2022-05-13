package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.*;
import com.link_intersystems.dbunit.table.ColumnList;
import com.link_intersystems.dbunit.dataset.beans.fixtures.PrimitiveTypesBean;
import com.link_intersystems.test.ComponentTest;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ComponentTest
class BeanTableMetaDataTest {

    private BeanTableMetaData tableMetaData;

    @BeforeEach
    public void setUp() throws BeanClassException {
        BeanClass<PrimitiveTypesBean> beanClass = BeansFactory.getDefault().createBeanClass(PrimitiveTypesBean.class);
        tableMetaData = new BeanTableMetaData(beanClass);
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
    void getPrimaryKeys() throws DataSetException {
        assertEquals(0, tableMetaData.getPrimaryKeys().length);
    }

    @Test
    void getPrimaryKeysException() throws Exception {
        BeanIdentity beanIdentity = mock(BeanIdentity.class);
        when(beanIdentity.getIdProperties(any())).thenThrow(new RuntimeException());
        tableMetaData.setBeanIdentity(beanIdentity);

        assertThrows(DataSetException.class, tableMetaData::getPrimaryKeys);
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

    @Test
    void getPropertyValueException() {
        PropertyDesc propertyDesc = mock(PropertyDesc.class);
        when(propertyDesc.getPropertyValue(any())).thenThrow(new PropertyReadException(Object.class, "class"));

        assertThrows(DataSetException.class, () -> tableMetaData.getValue("", propertyDesc));
    }

}