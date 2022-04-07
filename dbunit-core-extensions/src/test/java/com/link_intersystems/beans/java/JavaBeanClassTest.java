package com.link_intersystems.beans.java;

import com.link_intersystems.ComponentTest;
import com.link_intersystems.beans.BeanInstantiationException;
import com.link_intersystems.beans.Property;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
@ComponentTest
class JavaBeanClassTest {

    private JavaBeanClass javaBeanClass;

    @BeforeEach
    void setUp() throws IntrospectionException {
        javaBeanClass = new JavaBeanClass(TestBean.class);
    }

    @Test
    void getProperties() {

        List<Property> properties = javaBeanClass.getProperties();
        assertEquals(3, properties.size());
    }

    @Test
    void getSimpleName() {
        assertEquals("TestBean", javaBeanClass.getName());
    }

    @Test
    void newInstance() throws BeanInstantiationException {
        assertNotNull(javaBeanClass.newInstance());
    }

    @Test
    void newInstanceThrowsException() throws IntrospectionException {
        class NoBeanClass {
            private NoBeanClass() {
            }
        }
        javaBeanClass = new JavaBeanClass(NoBeanClass.class);

        assertThrows(BeanInstantiationException.class, () -> javaBeanClass.newInstance());
    }
}