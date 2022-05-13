package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.dbunit.table.ColumnList;
import org.dbunit.dataset.AbstractTable;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

import static java.util.Objects.requireNonNull;

/**
 * A {@link org.dbunit.dataset.ITable} adaption of a {@link BeanList}.
 * <p>
 * Each property of the beans in this {@link BeanTable} will be available as columns.
 */
public class BeanTable extends AbstractTable {

    private final BeanList<?> beanList;
    private IBeanTableMetaData beanTableMetaData;

    /**
     * Create an {@link org.dbunit.dataset.ITable} adaption of the given {@link BeanList}.
     *
     * @param beanList          the {@link BeanList} to adapt.
     * @param beanTableMetaData the metadata provider for the beans.
     */
    public BeanTable(BeanList<?> beanList, IBeanTableMetaData beanTableMetaData) {
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
        Object bean = beanList.get(row);
        ColumnList columnList = beanTableMetaData.getColumnList();
        Column column = columnList.getColumn(columnName);
        return beanTableMetaData.getValue(bean, column);
    }
}
