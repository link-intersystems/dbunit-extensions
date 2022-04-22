package com.link_intersystems.util;

import com.link_intersystems.UnitTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class TypeConversionExceptionTest {

    @Test
    void cause() {
        RuntimeException re = new RuntimeException();
        TypeConversionException typeConversionException = new TypeConversionException(re);

        assertSame(re, typeConversionException.getCause());
    }

}