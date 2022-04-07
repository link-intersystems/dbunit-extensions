package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.Property;
import org.dbunit.dataset.datatype.DataType;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface PropertyDataTypeMapping {
    DataType getDataType(BeanClass beanClass, Property property);
}
