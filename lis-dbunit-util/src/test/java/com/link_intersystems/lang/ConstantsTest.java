package com.link_intersystems.lang;

import com.link_intersystems.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class ConstantsTest {

    private Constants<String> testConstants;

    @BeforeEach
    void setUp() {
        testConstants = new Constants<>(ConstantsTestValues.class, String.class);
    }

    @Test
    void byName() {
        assertEquals("PUBLIC_CONSTANT_VALUE", testConstants.getValue("PUBLIC_CONSTANT"));
    }

    @Test
    void byNameNotExistent() {
        assertNull(testConstants.getValue("TEST"));
    }

    @Test
    void size() {
        assertEquals(2, testConstants.size());
    }

    @Test
    void get() {
        assertEquals("PUBLIC_CONSTANT_VALUE", testConstants.get(0));
        assertEquals("ANOTHER_PUBLIC_CONSTANT_VALUE", testConstants.get(1));
    }
}