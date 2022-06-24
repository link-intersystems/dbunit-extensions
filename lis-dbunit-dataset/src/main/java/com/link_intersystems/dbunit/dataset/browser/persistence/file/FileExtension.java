package com.link_intersystems.dbunit.dataset.browser.persistence.file;

import java.util.Objects;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FileExtension {

    private String extension;

    public FileExtension(String extension) {
        Objects.requireNonNull(extension);
        extension = extension.trim();
        if (extension.isEmpty()) {
            throw new IllegalArgumentException("extension must not be blank");
        }
        this.extension = extension;
    }

    public String createFilename(String name) {
        if (extension != null) {
            return name + "." + extension;
        }
        return name;
    }
}
