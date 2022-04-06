package com.link_intersystems.beans;

import com.link_intersystems.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *  @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class PropertyListTest {

    private Property property1;
    private Property property2;
    private PropertyList properties;

    @BeforeEach
    void setUp() {
        property1 = mock(Property.class);
        property2 = mock(Property.class);

        when(property1.getName()).thenReturn("property1");
        when(property2.getName()).thenReturn("property2");

        properties = new PropertyList(asList(property1, property2));
    }

    @Test
    void getByName() {
        assertEquals(property1, properties.getByName("property1"));
        assertNull(properties.getByName("PROPERTY1"));
    }

    @Test
    void getByNameIgnoreCase() {
        assertEquals(property1, properties.getByName("PROPERTY1", String::equalsIgnoreCase));
    }

    @Test
    void get() {
        assertEquals(property1, properties.get(0));
        assertEquals(property2, properties.get(1));
    }

    @Test
    void size() {
        assertEquals(2, properties.size());
    }
}