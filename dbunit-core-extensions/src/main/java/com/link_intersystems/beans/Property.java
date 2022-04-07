package com.link_intersystems.beans;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface Property {
    String getName();

    Class<?> getType();

    Object get(Object bean) throws PropertyReadException;

    void set(Object bean, Object value) throws PropertyWriteException;
}
