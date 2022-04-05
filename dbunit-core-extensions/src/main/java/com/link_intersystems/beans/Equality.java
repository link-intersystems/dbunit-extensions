package com.link_intersystems.beans;

/**
 *  @author - René Link &lt;rene.link@link-intersystems.com&gt;
 */
public interface Equality<T> {
    boolean isEqual(T o1, T o2);
}
