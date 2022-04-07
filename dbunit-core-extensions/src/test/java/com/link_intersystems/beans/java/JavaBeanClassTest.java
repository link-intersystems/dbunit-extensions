package com.link_intersystems.beans.java;

import com.link_intersystems.ComponentTest;
import com.link_intersystems.beans.Property;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *  @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
@ComponentTest
class JavaBeanClassTest {

    @Test
    void getProperties() throws IntrospectionException {
        JavaBeanClass javaBeanClass = new JavaBeanClass(TestBean.class);

        List<Property> properties = javaBeanClass.getProperties();
        assertEquals(3, properties.size());
    }

    @Test
    void getSimpleName() throws IntrospectionException {
        JavaBeanClass javaBeanClass = new JavaBeanClass(TestBean.class);

        assertEquals("TestBean", javaBeanClass.getName());
    }
}