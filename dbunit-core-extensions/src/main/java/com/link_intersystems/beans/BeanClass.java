package com.link_intersystems.beans;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface BeanClass {

    Class<?> getType();

    String getName();

    PropertyList getProperties();

    Object newInstance() throws BeanInstantiationException;
}
