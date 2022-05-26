package com.link_intersystems.dbunit.dataset.browser;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class InOperatorFormat implements SqlOperatorFormat {
    @Override
    public SqlOperator format(Object value) {
        int size = -1;
        List<Object> arguments = new ArrayList<>();

        if (value instanceof Collection) {
            Collection collection = (Collection) value;
            size = collection.size();
            arguments = new ArrayList<>(collection);
        } else if (value != null && value.getClass().isArray()) {
            size = Array.getLength(value);
            Object[] array = (Object[]) value;
            arguments = Arrays.asList(array);
        }

        String argumentPlaceholder = Collections.nCopies(size, "?").stream().collect(Collectors.joining(", "));

        return new SqlOperator("in (" + argumentPlaceholder + ")", arguments);
    }
}
