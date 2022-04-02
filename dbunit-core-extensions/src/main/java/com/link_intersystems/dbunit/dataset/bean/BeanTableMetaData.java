package com.link_intersystems.dbunit.dataset.bean;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.Property;
import com.link_intersystems.dbunit.dataset.ColumnList;
import org.dbunit.dataset.AbstractTableMetaData;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.DataType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public abstract class BeanTableMetaData extends AbstractTableMetaData {

    public abstract BeanClass getBeanClass();

    private Map<Column, Property> columnMap;
    private List<Property> idProperties;

    @Override
    public String getTableName() {
        return getBeanClass().getSimpleName();
    }

    @Override
    public Column[] getColumns() {
        if (columnMap == null) {
            Map<Column, Property> columnMap = new LinkedHashMap<>();
            List<Property> properties = getBeanClass().getProperties();
            for (Property property : properties) {
                Column column = toColumn(property);
                columnMap.put(column, property);
            }
            this.columnMap = columnMap;
        }
        return columnMap.keySet().toArray(new Column[0]);
    }

    public ColumnList getColumnList() {
        return new ColumnList(getColumns());
    }


    private Column toColumn(Property property) {
        String name = property.getName();

        DataType dataType = getDataType(property);
        if (dataType == null) {
            return null;
        }

        return new Column(name, dataType);
    }

    protected abstract DataType getDataType(Property property);

    @Override
    public Column[] getPrimaryKeys() {
        if (idProperties == null) {
            idProperties = getBeanClass().getProperties().stream().filter(this::isIdentityProperty).collect(toList());
        }
        Column[] columns = getColumns();
        return stream(columns).filter(columnMap::containsKey).toArray(Column[]::new);
    }

    protected abstract boolean isIdentityProperty(Property property);

    public Object getValue(Object bean, Column column) throws DataSetException {
        Property property = columnMap.get(column);
        return getValue(bean, property);
    }

    protected abstract Object getValue(Object bean, Property property) throws DataSetException;
}
