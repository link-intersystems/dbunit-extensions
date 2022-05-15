package com.link_intersystems.dbunit.table;

import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ListSnapshot<E> {

    private List<E> snapshot = new ArrayList<>();

    public ListSnapshot(List<E> list) {
        snapshot.addAll(list);
    }

    public List<E> diff(ListSnapshot<E> otherSnapshot) {
        return diff(otherSnapshot.snapshot);
    }

    public List<E> diff(List<E> otherList) {
        List<E> copy = new ArrayList<>(otherList);
        copy.removeAll(snapshot);
        return copy;
    }
}
