package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.*;
import com.link_intersystems.dbunit.table.ColumnList;
import org.dbunit.dataset.AbstractTableMetaData;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.DataType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BeanTableMetaData extends AbstractTableMetaData implements IBeanTableMetaData {

    private final BeanClass<?> beanClass;
    private Map<Column, PropertyDesc> propertyDescByColumn;
    private List<PropertyDesc> idProperties;
    private BeanIdentity beanIdentity = new DefaultBeanIdentity();

    private PropertyConversion propertyConversion;

    public BeanTableMetaData(BeanClass<?> beanClass) {
        this(beanClass, new DefaultPropertyConversion());
    }

    public BeanTableMetaData(BeanClass<?> beanClass, PropertyConversion propertyConversion) {
        this.beanClass = requireNonNull(beanClass);
        this.propertyConversion = requireNonNull(propertyConversion);
    }

    @Override
    public BeanClass<?> getBeanClass() {
        return beanClass;
    }

    protected DataType getDataType(PropertyDesc property) {
        return propertyConversion.toDataType(getBeanClass(), property);
    }

    public void setBeanIdentity(BeanIdentity beanIdentity) {
        this.beanIdentity = requireNonNull(beanIdentity);
    }

    @Override
    public String getTableName() {
        return getBeanClass().getName();
    }

    @Override
    public Column[] getColumns() {
        if (propertyDescByColumn == null) {
            Map<Column, PropertyDesc> columnMap = new LinkedHashMap<>();
            PropertyDescList properties = getBeanClass().getProperties();
            for (PropertyDesc property : properties) {
                if (property.getName().equals("class")) {
                    continue;
                }
                Column column = createColumn(property);
                columnMap.put(column, property);
            }
            this.propertyDescByColumn = columnMap;
        }
        return propertyDescByColumn.keySet().toArray(new Column[0]);
    }

    @Override
    public ColumnList getColumnList() {
        return new ColumnList(getColumns());
    }

    /**
     * Returns a {@link Column} that represents the given {@link Property}.
     *
     * @param property a property of the {@link BeanClass}.
     * @return a {@link Column} that represents the given {@link Property}.
     */
    protected Column createColumn(PropertyDesc property) {
        String name = property.getName();

        DataType dataType = getDataType(property);

        return new Column(name, dataType);
    }

    @Override
    public Column[] getPrimaryKeys() throws DataSetException {
        List<PropertyDesc> idProperties = getIdProperties();
        Column[] columns = getColumns();
        Predicate<Column> idColumnPredicate = c -> idProperties.contains(propertyDescByColumn.get(c));
        return stream(columns).filter(idColumnPredicate).toArray(Column[]::new);
    }

    protected List<PropertyDesc> getIdProperties() throws DataSetException {
        if (idProperties == null) {
            try {
                idProperties = beanIdentity.getIdProperties(getBeanClass());
            } catch (Exception e) {
                throw new DataSetException(e);
            }
        }
        return idProperties;
    }

    @Override
    public Object getValue(Object bean, Column column) throws DataSetException {
        PropertyDesc property = propertyDescByColumn.get(column);
        Object propertyValue = getValue(bean, property);
        return propertyConversion.toColumnValue(propertyValue, column);
    }

    protected Object getValue(Object bean, PropertyDesc propertyDesc) throws DataSetException {
        try {
            return propertyDesc.getPropertyValue(bean);
        } catch (PropertyReadException e) {
            throw new DataSetException(e);
        }
    }

    @Override
    public void setValue(Object bean, Column column, Object columnValue) throws DataSetException {
        PropertyDesc propertyDesc = propertyDescByColumn.get(column);
        setValue(bean, columnValue, propertyDesc);
    }

    protected void setValue(Object bean, Object columnValue, PropertyDesc propertyDesc) throws DataSetException {
        Object propertyValue = propertyConversion.toPropertyValue(columnValue, propertyDesc);
        propertyDesc.setPropertyValue(bean, propertyValue);
    }
}
