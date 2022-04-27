package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.PropertyDesc;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.datatype.DataType;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface PropertyConversion {

    DataType toDataType(BeanClass<?> beanClass, PropertyDesc propertyDesc);

    public Object toColumnValue(Object propertyValue, Column column) throws DataSetException;

    public Object toPropertyValue(Object columnValue, PropertyDesc propertyDesc) throws DataSetException;
}
