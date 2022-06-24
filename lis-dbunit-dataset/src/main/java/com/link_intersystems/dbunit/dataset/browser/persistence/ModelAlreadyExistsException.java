package com.link_intersystems.dbunit.dataset.browser.persistence;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ModelAlreadyExistsException extends ModelPersistenceException {
    public ModelAlreadyExistsException(Throwable cause) {
        super(cause);
    }

    public ModelAlreadyExistsException() {
    }

    public ModelAlreadyExistsException(String message) {
        super(message);
    }

    public ModelAlreadyExistsException(String message, Throwable cause) {
        super(message, cause);
    }
}
