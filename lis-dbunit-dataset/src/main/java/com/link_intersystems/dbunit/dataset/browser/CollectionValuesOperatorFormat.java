package com.link_intersystems.dbunit.dataset.browser;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.joining;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CollectionValuesOperatorFormat extends AbstractOperatorFormat {

    public CollectionValuesOperatorFormat(String operator) {
        super(operator);
    }


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

        String argumentPlaceholder = nCopies(size, "?").stream().collect(joining(", "));

        return new SqlOperator(getOperator() + " (" + argumentPlaceholder + ")", arguments);
    }
}
