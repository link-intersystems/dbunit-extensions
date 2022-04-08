package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.Property;
import com.link_intersystems.beans.PropertyWriteException;
import com.link_intersystems.dbunit.dataset.ColumnList;
import com.link_intersystems.util.TypeConversionException;
import com.link_intersystems.util.ValueConverter;
import com.link_intersystems.util.ValueConverterRegistry;
import org.dbunit.dataset.AbstractTableMetaData;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.DataType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;

/**
 * Template methods for easier implementation of {@link IBeanTableMetaData}.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractBeanTableMetaData extends AbstractTableMetaData implements IBeanTableMetaData {

    private Map<Column, Property> columnMap;
    private List<Property> idProperties;
    private BeanIdentity beanIdentity = BeanIdentity.NULL_IDENTITY;
    private ValueConverterRegistry valueConverterRegistry = new DataTypeValueConverterRegistry();

    protected void setBeanIdentity(BeanIdentity beanIdentity) {
        this.beanIdentity = requireNonNull(beanIdentity);
    }

    public void setValueConverterRegistry(ValueConverterRegistry valueConverterRegistry) {
        this.valueConverterRegistry = requireNonNull(valueConverterRegistry);
    }

    @Override
    public String getTableName() {
        return getBeanClass().getName();
    }

    @Override
    public Column[] getColumns() {
        if (columnMap == null) {
            Map<Column, Property> columnMap = new LinkedHashMap<>();
            List<Property> properties = getBeanClass().getProperties();
            for (Property property : properties) {
                Column column = createColumn(property);
                columnMap.put(column, property);
            }
            this.columnMap = columnMap;
        }
        return columnMap.keySet().toArray(new Column[0]);
    }

    @Override
    public ColumnList getColumnList() {
        return new ColumnList(getColumns());
    }

    /**
     * Returns a {@link Column} that represents the given {@link Property}.
     *
     * @param property a property of the {@link com.link_intersystems.beans.BeanClass}.
     * @return a {@link Column} that represents the given {@link Property}.
     */
    protected Column createColumn(Property property) {
        String name = property.getName();

        DataType dataType = getDataType(property);
        if (dataType == null) {
            return null;
        }

        return new Column(name, dataType);
    }

    /**
     * @param property a property of the {@link com.link_intersystems.beans.BeanClass}.
     * @return The {@link DataType} that the property's type is mapped to.
     */
    protected abstract DataType getDataType(Property property);

    @Override
    public Column[] getPrimaryKeys() throws DataSetException {
        getIdProperties();
        Column[] columns = getColumns();
        return stream(columns).filter(c -> idProperties.contains(columnMap.get(c))).toArray(Column[]::new);
    }

    protected void getIdProperties() throws DataSetException {
        if (idProperties == null) {
            try {
                idProperties = beanIdentity.getIdProperties(getBeanClass());
            } catch (Exception e) {
                throw new DataSetException(e);
            }
        }
    }

    @Override
    public Object getValue(Object bean, Column column) throws DataSetException {
        Property property = columnMap.get(column);
        Object propertyValue = getValue(bean, property);
        DataType dataType = column.getDataType();
        return dataType.typeCast(propertyValue);
    }

    /**
     * @param bean     a bean of the {@link com.link_intersystems.beans.BeanClass}'s type.
     * @param property property of the {@link com.link_intersystems.beans.BeanClass}.
     * @return the properties value.
     * @throws DataSetException if the value can not be determined.
     */
    protected abstract Object getValue(Object bean, Property property) throws DataSetException;

    @Override
    public void setValue(Object bean, Column column, Object columnValue) throws DataSetException {
        Property property = columnMap.get(column);
        try {
            Class<?> propertyType = property.getType();
            ValueConverter valueConverter = valueConverterRegistry.getValueConverter(propertyType);
            Object propertyValue = valueConverter.convert(columnValue);
            property.set(bean, propertyValue);
        } catch (PropertyWriteException | TypeConversionException e) {
            throw new DataSetException(e);
        }
    }
}
