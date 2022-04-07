package com.link_intersystems.beans;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class PropertyWriteException extends PropertyAccessException {

    public PropertyWriteException(String message) {
        super(message);
    }

    public PropertyWriteException(String message, Throwable cause) {
        super(message, cause);
    }
}
