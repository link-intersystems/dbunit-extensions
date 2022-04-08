package com.link_intersystems.util;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface ValueConverterRegistry {

    public ValueConverter getValueConverter(Class<?> targetType);
}
