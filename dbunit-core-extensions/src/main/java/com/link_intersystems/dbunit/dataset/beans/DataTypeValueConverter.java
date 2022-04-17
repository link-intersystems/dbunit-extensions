package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.util.TypeConversionException;
import com.link_intersystems.util.ValueConverter;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.TypeCastException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataTypeValueConverter implements ValueConverter {

    private DataType dataType;

    public DataTypeValueConverter(DataType dataType) {
        this.dataType = dataType;
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
