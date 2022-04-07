package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.dbunit.dataset.ColumnList;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

/**
 * {@link ITableMetaData} that is based on Java beans.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface IBeanTableMetaData extends ITableMetaData {
    /**
     * @return the {@link BeanClass} of the elements that this {@link ITableMetaData} represents.
     */
    BeanClass getBeanClass();

    /**
     * @return a {@link ColumnList} for convenient access to {@link Column}s.
     */
    ColumnList getColumnList();

    /**
     * @param bean   a bean of the {@link #getBeanClass()}'s type.
     * @param column a {@link ITableMetaData} column.
     * @return the value extracted of the property of the bean that the column represents.
     * @throws DataSetException if the column can not be found or if the value can not be extracted.
     */
    Object getValue(Object bean, Column column) throws DataSetException;
}
