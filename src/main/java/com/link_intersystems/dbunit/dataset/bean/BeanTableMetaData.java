package com.link_intersystems.dbunit.dataset.bean;

import org.dbunit.dataset.AbstractTableMetaData;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class BeanTableMetaData extends AbstractTableMetaData {

    private Class<?> beanClass;

    private BeanTableNameProvider tableNameProvider = bc -> bc.getSimpleName();
    private BeanColumnProvider columnProvider = new JavaBeanColumnProvider();

    public BeanTableMetaData(Class<?> beanClass) {

        this.beanClass = requireNonNull(beanClass);
    }

    public void setColumnProvider(BeanColumnProvider columnProvider) {
        this.columnProvider = Objects.requireNonNull(columnProvider);
    }

    public void setTableNameProvider(BeanTableNameProvider tableNameProvider) {
        this.tableNameProvider = requireNonNull(tableNameProvider);
    }

    @Override
    public String getTableName() {
        return tableNameProvider.getTableName(beanClass);
    }

    @Override
    public Column[] getColumns() throws DataSetException {
        try {
            return columnProvider.getColumns(beanClass);
        } catch (Exception e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public Column[] getPrimaryKeys() throws DataSetException {
        return new Column[0];
    }
}
