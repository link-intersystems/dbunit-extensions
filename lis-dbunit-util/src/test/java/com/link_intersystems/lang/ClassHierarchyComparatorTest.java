package com.link_intersystems.lang;

import com.link_intersystems.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class ClassHierarchyComparatorTest {
    class A {
    }

    class BextendsA extends A {
    }

    class CextendsA extends A {
    }

    private ClassHierarchyComparator comparator;
    private ComparatorAssertions assertions;

    @BeforeEach
    void setUp() {
        comparator = new ClassHierarchyComparator();
        assertions = new ComparatorAssertions(comparator);
    }

    @Test
    void sameClass() {
        assertions.assertEqual(A.class, A.class);
    }

    @Test
    void superClassWithSubClass() {
        assertions.assertGreater(A.class, BextendsA.class);
    }

    @Test
    void subClassWithSuperClass() {
        assertions.assertLower(BextendsA.class, A.class);
    }

    @Test
    void unrelatedClassesByName() {
        assertions.assertEqual(String.class, A.class);
    }

    @Test
    void sortList() {
        List<Class<?>> classes = asList(BextendsA.class, A.class, CextendsA.class);
        Collections.sort(classes, comparator);

        List<Class<?>> expected = asList(BextendsA.class, CextendsA.class, A.class);
        assertEquals(expected, classes);
    }

    @Test
    void objectClassSuperWithSubClass() {
        Comparator<Number> comparator = ClassHierarchyComparator.objectsComparator();
        ComparatorAssertions comparatorAssertions = new ComparatorAssertions(comparator);

        comparatorAssertions.assertEqual(new A(), new A());
    }

    @Test
    void objectClassSubClassWithSuperClass() {


        Comparator<Object> comparator = ClassHierarchyComparator.objectsComparator();
        ComparatorAssertions comparatorAssertions = new ComparatorAssertions(comparator);
        comparatorAssertions.assertGreater(new A(), new BextendsA());
    }

    @Test
    void objectClassEqual() {
        Comparator<Number> comparator = ClassHierarchyComparator.objectsComparator();
        ComparatorAssertions comparatorAssertions = new ComparatorAssertions(comparator);

        comparatorAssertions.assertLower(new BextendsA(), new A());
    }

}