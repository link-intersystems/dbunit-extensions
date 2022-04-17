package com.link_intersystems.lang;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class HierarchyOrderedList<E>  extends AbstractList<E> {

    private final ArrayList<E> sourceList;

    public HierarchyOrderedList(List<E> sourceList){
        this.sourceList = new ArrayList<>(sourceList);


    }

    private List<Class<?>> getRootClasses(List<E> list){
        List<Class<?>> rootClasses = new ArrayList<>();

        for (E element : list) {
            Class<?> elementClass = element.getClass();

        }

        return rootClasses;
    }

    @Override
    public E get(int index) {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }
}
