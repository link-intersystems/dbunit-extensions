package com.link_intersystems.lang;

import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.util.*;
import java.util.function.BinaryOperator;

import static java.lang.reflect.Modifier.*;
import static java.util.stream.Collectors.toMap;

/**
 * Convenience class to access constants defined in some class.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class Constants<T> extends AbstractList<T> {

    private Map<String, Field> byName;
    private List<Field> constantFields;
    private Map<Field, T> cachedConstantValues = new HashMap<>();

    public Constants(Class<T> contantType) {
        this(contantType, contantType);
    }

    public Constants(Class<?> constantDeclaringClass, Class<T> constantType) {
        byName = Arrays.stream(constantDeclaringClass.getDeclaredFields())
                .filter(this::isPublicStaticFinal)
                .filter(f -> constantType.equals(f.getType()))
                .collect(toMap(
                        Field::getName,
                        f -> f,
                        throwingMerger(),
                        LinkedHashMap::new));


        constantFields = new ArrayList<>(byName.values());
    }

    private static <T> BinaryOperator<T> throwingMerger() {
        return (u, v) -> {
            throw new IllegalStateException(String.format("Duplicate key %s", u));
        };
    }


    private boolean isPublicStaticFinal(Member member) {
        int m = member.getModifiers();
        return isPublic(m) && isStatic(m) && isFinal(m);
    }

    public T getValue(String name) {
        Field constantField = byName.get(name);
        return getConstantValue(constantField);
    }

    @Override
    public T get(int index) {
        Field constantField = constantFields.get(index);
        return getConstantValue(constantField);
    }

    private T getConstantValue(Field constantField) {
        if (constantField == null) {
            return null;
        }
        return cachedConstantValues.computeIfAbsent(constantField, this::getFieldValue);
    }

    private T getFieldValue(Field field) {
        try {
            return (T) field.get(field.getDeclaringClass());
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public int size() {
        return constantFields.size();
    }
}
