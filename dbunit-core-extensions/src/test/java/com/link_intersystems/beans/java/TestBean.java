package com.link_intersystems.beans.java;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
abstract class TestBean {
    public abstract String getWritableStringProp();

    public abstract void setWritableStringProp(String value);

    public abstract String getReadOnlyStringProp();

    public abstract void setWriteOnlyStringProp(String value);
}
