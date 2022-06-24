package com.link_intersystems.dbunit.dataset.browser.persistence.file;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class FileExtensionTest {

    @Test
    void createFilename() {
        FileExtension txt = new FileExtension("txt");

        assertEquals("someFile.txt", txt.createFilename("someFile"));
    }

    @Test
    void createFilenameEmptyTxt() {
        assertThrows(IllegalArgumentException.class, () -> new FileExtension("  "));
    }
}