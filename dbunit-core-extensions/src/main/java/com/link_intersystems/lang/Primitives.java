package com.link_intersystems.lang;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods for primitive types.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class Primitives {

    public static final Map<Class<?>, Class<?>> primitiveWrapperMap = new HashMap<>();

    static {
        primitiveWrapperMap.put(boolean.class, Boolean.class);
        primitiveWrapperMap.put(byte.class, Byte.class);
        primitiveWrapperMap.put(short.class, Short.class);
        primitiveWrapperMap.put(char.class, Character.class);
        primitiveWrapperMap.put(int.class, Integer.class);
        primitiveWrapperMap.put(long.class, Long.class);
        primitiveWrapperMap.put(float.class, Float.class);
        primitiveWrapperMap.put(double.class, Double.class);
    }

    public static Class<?> getWrapperType(Class<?> primitiveType) {
        return primitiveWrapperMap.get(primitiveType);
    }

}
