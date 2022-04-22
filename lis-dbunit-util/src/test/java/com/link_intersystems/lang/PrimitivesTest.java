package com.link_intersystems.lang;

import com.link_intersystems.UnitTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class PrimitivesTest {

    @Test
    void getWrapperType() {
        assertEquals(Byte.class, Primitives.getWrapperType(Byte.TYPE));
        assertEquals(Short.class, Primitives.getWrapperType(Short.TYPE));
        assertEquals(Integer.class, Primitives.getWrapperType(Integer.TYPE));
        assertEquals(Long.class, Primitives.getWrapperType(Long.TYPE));
        assertEquals(Float.class, Primitives.getWrapperType(Float.TYPE));
        assertEquals(Double.class, Primitives.getWrapperType(Double.TYPE));
        assertEquals(Boolean.class, Primitives.getWrapperType(Boolean.TYPE));
        assertEquals(Character.class, Primitives.getWrapperType(Character.TYPE));
    }
}