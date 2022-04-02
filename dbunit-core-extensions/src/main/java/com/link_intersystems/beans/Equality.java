package com.link_intersystems.beans;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
public interface Equality<T> {
    boolean isEqual(T o1, T o2);
}
