package com.link_intersystems.dbunit.dataset.browser.persistence;

import java.io.FileNotFoundException;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ModelPersistenceException extends Exception {
    public ModelPersistenceException(Throwable cause) {
        super(cause);
    }

    public ModelPersistenceException() {
    }

    public ModelPersistenceException(String message) {
        super(message);
    }

    public ModelPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
