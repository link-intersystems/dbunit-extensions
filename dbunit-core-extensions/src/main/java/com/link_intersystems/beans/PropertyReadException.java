package com.link_intersystems.beans;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class PropertyReadException extends PropertyAccessException {
    public PropertyReadException(String message) {
        super(message);
    }

    public PropertyReadException(String message, Throwable cause) {
        super(message, cause);
    }
}
