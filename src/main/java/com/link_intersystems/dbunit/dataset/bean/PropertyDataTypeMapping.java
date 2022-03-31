package com.link_intersystems.dbunit.dataset.bean;

import org.dbunit.dataset.datatype.DataType;

import java.beans.PropertyDescriptor;

public interface PropertyDataTypeMapping {
    DataType getDataType(PropertyDescriptor pd);
}
