package com.link_intersystems.beans.java;

import com.link_intersystems.beans.Property;
import com.link_intersystems.beans.PropertyReadException;
import com.link_intersystems.beans.PropertyWriteException;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;

import static java.text.MessageFormat.format;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
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

    @Override
    public Class<?> getType() {
        return propertyDescriptor.getPropertyType();
    }

    @Override
    public Object get(Object bean) throws PropertyReadException {
        Method readMethod = propertyDescriptor.getReadMethod();

        if (readMethod == null) {
            String msg = format("{0} is not readable.", getPropertyDescriptor());
            throw new PropertyReadException(msg);
        }

        makeAccessible(readMethod);

        try {
            return readMethod.invoke(bean);
        } catch (Exception e) {
            String msg = format("{0} could not be read.", getPropertyDescriptor());
            throw new PropertyReadException(msg, e);
        }
    }

    protected void makeAccessible(Method method) {
        if (!method.isAccessible()) {
            method.setAccessible(true);
        }
    }

    @Override
    public void set(Object bean, Object value) throws PropertyWriteException {
        Method writeMethod = propertyDescriptor.getWriteMethod();

        if (writeMethod == null) {
            String msg = format("{0} is not writable.", this);
            throw new PropertyWriteException(msg);
        }

        makeAccessible(writeMethod);

        try {
            writeMethod.invoke(bean, value);
        } catch (Exception e) {
            String msg = format("{0} could not be written.", this);
            throw new PropertyWriteException(msg, e);
        }
    }

    public PropertyDescriptor getPropertyDescriptor() {
        return propertyDescriptor;
    }

    @Override
    public String toString() {
        return getName() + '<' + getType().getName() + '>';
    }
}
