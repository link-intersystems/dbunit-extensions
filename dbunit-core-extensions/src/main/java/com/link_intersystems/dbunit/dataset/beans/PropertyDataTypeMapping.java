package com.link_intersystems.dbunit.dataset.beans;

import org.dbunit.dataset.datatype.DataType;

public interface PropertyDataTypeMapping {
    DataType getDataType(Class<?> javaType);
}
