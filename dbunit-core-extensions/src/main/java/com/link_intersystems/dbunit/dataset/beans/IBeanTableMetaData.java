package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.dbunit.dataset.ColumnList;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface IBeanTableMetaData extends ITableMetaData {
    BeanClass getBeanClass();

    ColumnList getColumnList();

    Object getValue(Object bean, Column column) throws DataSetException;
}
