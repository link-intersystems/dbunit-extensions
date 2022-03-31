package com.link_intersystems.dbunit.dataset.bean;

import org.dbunit.dataset.DataSetException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BeanTableMetaDataTest {

    private BeanTableMetaData tableMetaData;

    @BeforeEach
    public void setUp() {
        tableMetaData = new BeanTableMetaData(PrimitiveTypesBean.class);
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
}