package com.link_intersystems.dbunit.dataset.beans;

import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataTypeValueConverter implements ValueConverter {

    private DataType dataType;

    public DataTypeValueConverter(DataType dataType) {
        this.dataType = requireNonNull(dataType);
    }

    @Override
    public Object convert(Object source) throws TypeConversionException {
        try {
            return dataType.typeCast(source);
        } catch (TypeCastException e) {
            throw new TypeConversionException(e);
        }
    }
}
