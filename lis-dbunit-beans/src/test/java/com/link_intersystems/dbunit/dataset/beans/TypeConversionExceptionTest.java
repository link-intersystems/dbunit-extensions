package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.test.UnitTest;
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

        Throwable cause = typeConversionException.getCause();
        assertSame(re, cause);
    }

}