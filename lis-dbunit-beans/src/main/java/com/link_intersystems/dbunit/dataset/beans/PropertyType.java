package com.link_intersystems.dbunit.dataset.beans;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface PropertyType {

    public Object typeCast(Object source) throws TypeConversionException;
}
