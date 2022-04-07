package com.link_intersystems.beans;

import com.link_intersystems.UnitTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class BeanInstantiationExceptionTest {

    @Test
    void newInstance() {
        RuntimeException re = new RuntimeException();
        BeanInstantiationException beanInstantiationException = new BeanInstantiationException(re);

        assertEquals(re, beanInstantiationException.getCause());
    }
}