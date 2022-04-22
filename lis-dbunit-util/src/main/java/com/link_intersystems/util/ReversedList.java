package com.link_intersystems.util;

import java.util.AbstractList;
import java.util.List;

import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 *  @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ReversedList<E> extends AbstractList<E> {

    private List<E> sourceList;

    public ReversedList(List<E> sourceList) {

        this.sourceList = requireNonNull(sourceList);
    }

    @Override
    public E get(int index) {
        int reversedIndex = reverseIndex(index);
        return sourceList.get(reversedIndex);
    }

    private int reverseIndex(int index) {
        return max(size() - index - 1, 0);
    }

    @Override
    public int size() {
        return sourceList.size();
    }
}
