package com.link_intersystems.lang;

import java.util.Comparator;

/**
 * Compares {@link Class} based on their assignability.
 *
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ClassHierarchyComparator implements Comparator<Class<?>> {

    public static final ClassHierarchyComparator INSTANCE = new ClassHierarchyComparator();

    public static <T> Comparator<T> objectsComparator() {
        return (o1, o2) -> {
            Class<?> c1 = o1.getClass();
            Class<?> c2 = o2.getClass();
            return INSTANCE.compare(c1, c2);
        };
    }

    /**
     * Compares {@link Class}es based on their assignability.
     *
     * @param c1 the first class.
     * @param c2 the second class.
     * @return <ul>
     * <li>0 - if class c1 is equal to class c2 or if the classes are not assignable to each other.</li>
     * <li>1 - If class c1 is assignable from class c2</li>
     * <li>-1 - If class c2 is assignable from class c1</li>
     * </ul>
     */
    @Override
    public int compare(Class<?> c1, Class<?> c2) {
        if (c1.equals(c2)) {
            return 0;
        } else if (c1.isAssignableFrom(c2)) {
            return 1;
        } else if (c2.isAssignableFrom(c1)) {
            return -1;
        } else {
            return 0;
        }
    }
}
