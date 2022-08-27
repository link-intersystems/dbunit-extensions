package com.link_intersystems.dbunit.table;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IRowValueProvider;
import org.dbunit.dataset.ITableMetaData;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class PrimaryKey extends AbstractList<Object> {

    private List<Object> pkValues = new ArrayList<>();

    public PrimaryKey(Row row) throws DataSetException {
        this(row.getMetaData(), row::getValue);

    }

    public PrimaryKey(ITableMetaData metaData, IRowValueProvider rowValueProvider) throws DataSetException {
        Column[] primaryKeys = metaData.getPrimaryKeys();

        for (int i = 0; i < primaryKeys.length; i++) {
            Column primaryKeyColumn = primaryKeys[i];
            pkValues.add(rowValueProvider.getColumnValue(primaryKeyColumn.getColumnName()));
        }

    }

    @Override
    public Object get(int index) {
        return pkValues.get(index);
    }

    @Override
    public int size() {
        return pkValues.size();
    }

}
