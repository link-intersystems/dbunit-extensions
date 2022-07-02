package com.link_intersystems.dbunit.dataset.browser.main;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class TableBrowseExceptionTest {

    @Test
    void messageAndCause() {
        RuntimeException cause = new RuntimeException();
        TableBrowseException tableBrowseException = new TableBrowseException("message", cause);


        assertEquals("message", tableBrowseException.getMessage());
        assertSame(cause, tableBrowseException.getCause());
    }
}