package com.link_intersystems.beans.java;

import com.link_intersystems.beans.Property;

import java.beans.PropertyDescriptor;

/**
 *  @author - René Link &lt;rene.link@link-intersystems.com&gt;
 */
public class JavaBeanProperty implements Property {

    private PropertyDescriptor propertyDescriptor;

    JavaBeanProperty(PropertyDescriptor propertyDescriptor) {

        this.propertyDescriptor = propertyDescriptor;
    }

    @Override
    public String getName() {
        return propertyDescriptor.getName();
    }

    public PropertyDescriptor getPropertyDescriptor() {
        return propertyDescriptor;
    }
}
