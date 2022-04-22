package com.link_intersystems.beans.java;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TestBean {
    String modifiableProperty;
    String readOnlyProperty;
    String writeOnlyProperty;

    public String getModifiableProperty() {
        return modifiableProperty;
    }

    public void setModifiableProperty(String modifiableProperty) {
        this.modifiableProperty = modifiableProperty;
    }

    public String getReadOnlyProperty() {
        return readOnlyProperty;
    }

    public void setWriteOnlyProperty(String value) {
        this.writeOnlyProperty = value;
    }
}
