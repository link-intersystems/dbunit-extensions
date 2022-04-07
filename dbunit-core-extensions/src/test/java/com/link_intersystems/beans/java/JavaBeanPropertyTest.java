package com.link_intersystems.beans.java;

import com.link_intersystems.UnitTest;
import com.link_intersystems.beans.Property;
import com.link_intersystems.beans.PropertyList;
import com.link_intersystems.beans.PropertyReadException;
import com.link_intersystems.beans.PropertyWriteException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.beans.IntrospectionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class JavaBeanPropertyTest {


    private TestBean testBean;
    private Property modifiableProperty;
    private Property readOnlyProperty;
    private Property writeOnlyProperty;

    @BeforeEach
    void setUp() throws IntrospectionException {
        JavaBeanClass javaBeanClass = new JavaBeanClass(TestBean.class);
        PropertyList properties = javaBeanClass.getProperties();
        modifiableProperty = properties.getByName("modifiableProperty");
        readOnlyProperty = properties.getByName("readOnlyProperty");
        writeOnlyProperty = properties.getByName("writeOnlyProperty");
        testBean = new TestBean();
    }


    @Test
    void getName() {
        assertEquals("modifiableProperty", modifiableProperty.getName());
        assertEquals("readOnlyProperty", readOnlyProperty.getName());
    }

    @Test
    void getProperty() throws PropertyReadException {
        testBean.readOnlyProperty = "test";
        assertEquals("test", readOnlyProperty.get(testBean));

        testBean.modifiableProperty = "test";
        assertEquals("test", modifiableProperty.get(testBean));
    }

    @Test
    void setProperty() throws PropertyWriteException {
        modifiableProperty.set(testBean, "test");
        assertEquals("test", testBean.modifiableProperty);
    }

    @Test
    void setReadonlyProperty() {
        assertThrows(PropertyWriteException.class, () -> readOnlyProperty.set(testBean, "test"));
    }

    @Test
    void getWriteonlyProperty() {
        assertThrows(PropertyReadException.class, () -> writeOnlyProperty.get(testBean));
    }

    @Test
    void getterException() {

        assertThrows(PropertyReadException.class, () -> readOnlyProperty.get(new Object()));
    }

    @Test
    void setterException() {

        assertThrows(PropertyWriteException.class, () -> writeOnlyProperty.set(testBean, new Object()));
    }
}