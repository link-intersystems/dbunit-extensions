package com.link_intersystems.util;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface ValueConverter {

    public Object convert(Object source) throws TypeConversionException;
}
