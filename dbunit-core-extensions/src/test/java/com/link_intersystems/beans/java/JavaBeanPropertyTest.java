package com.link_intersystems.beans.java;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.beans.PropertyDescriptor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
class JavaBeanPropertyTest {

    private PropertyDescriptor propertyDescriptor;

    @BeforeEach
    void setUp() {
        propertyDescriptor = mock(PropertyDescriptor.class);
    }


    @Test
    void getName() {
        when(propertyDescriptor.getName()).thenReturn("propName");

        JavaBeanProperty javaBeanProperty = new JavaBeanProperty(propertyDescriptor);

        Assertions.assertEquals("propName", javaBeanProperty.getName());

    }
}