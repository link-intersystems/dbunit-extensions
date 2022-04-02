package com.link_intersystems.dbunit.dataset.bean;

import com.link_intersystems.dbunit.dataset.ColumnList;
import org.dbunit.dataset.AbstractTable;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import static java.util.Objects.requireNonNull;

public class BeanTable<E> extends AbstractTable {

    private final BeanList<E> beanList;
    private BeanTableMetaData beanTableMetaData;

    public BeanTable(BeanList<E> beanList, BeanTableMetaData beanTableMetaData) {
        this.beanList = requireNonNull(beanList);
        this.beanTableMetaData = requireNonNull(beanTableMetaData);
    }

    @Override
    public ITableMetaData getTableMetaData() {
        return beanTableMetaData;
    }

    @Override
    public int getRowCount() {
        return beanList.size();
    }

    @Override
    public Object getValue(int row, String columnName) throws DataSetException {
        E bean = beanList.get(row);
        ColumnList columnList = beanTableMetaData.getColumnList();
        Column column = columnList.getColumn(columnName);
        return beanTableMetaData.getValue(bean, column);
    }
}
