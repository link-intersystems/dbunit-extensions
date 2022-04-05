package com.link_intersystems.beans.java;

/**
 *  @author - René Link &lt;rene.link@link-intersystems.com&gt;
 */
abstract class TestBean {
    public abstract String getWritableStringProp();

    public abstract void setWritableStringProp(String value);

    public abstract String getReadOnlyStringProp();

    public abstract void setWriteOnlyStringProp(String value);
}
