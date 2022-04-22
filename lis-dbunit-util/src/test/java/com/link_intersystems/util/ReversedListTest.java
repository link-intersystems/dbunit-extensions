package com.link_intersystems.util;

import com.link_intersystems.UnitTest;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 *  @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class ReversedListTest {

    @Test
    void get() {
        ReversedList<Integer> reversedList = new ReversedList<>(asList(0, 1, 2, 3, 4, 5));

        assertEquals(asList(5, 4, 3, 2, 1, 0), reversedList);
    }

    @Test
    void size() {
        ReversedList<Integer> reversedList = new ReversedList<>(asList(0, 1, 2, 3, 4, 5));

        assertEquals(6, reversedList.size());
    }
}