package com.link_intersystems.beans.java;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.Property;
import com.link_intersystems.beans.PropertyList;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.util.List;

import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
public class JavaBeanClass implements BeanClass {

    private final BeanInfo beanInfo;

    public JavaBeanClass(Class<?> beanClass) throws IntrospectionException {
        this(beanClass, Object.class);
    }

    public JavaBeanClass(Class<?> beanClass, Class<?> stopClass) throws IntrospectionException {

        beanInfo = Introspector.getBeanInfo(requireNonNull(beanClass), stopClass);

    }

    @Override
    public String getSimpleName() {
        return beanInfo.getBeanDescriptor().getName();
    }

    @Override
    public PropertyList getProperties() {
        PropertyDescriptor[] pds = beanInfo.getPropertyDescriptors();
        List<Property> properties = stream(pds).map(JavaBeanProperty::new).collect(toList());
        return new PropertyList(properties);
    }
}
