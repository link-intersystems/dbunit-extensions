package com.link_intersystems.dbunit.dataset.bean;

import org.dbunit.dataset.datatype.DataType;

public interface PropertyDataTypeMapping {
    DataType getDataType(Class<?> javaType);
}
