package com.link_intersystems.dbunit.dataset.beans;

import org.dbunit.dataset.datatype.DataType;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataTypeRegistry {

    DataType getDataType(Class<?> targetType);
}
