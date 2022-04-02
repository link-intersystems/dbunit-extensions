package com.link_intersystems.dbunit.dataset.bean;

import java.util.AbstractList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
public class BeanList<E> extends AbstractList<E> {

    private Class<E> beanClass;
    private List<E> beans;

    public BeanList(Class<E> beanClass, List<E> beans) {
        this.beanClass = requireNonNull(beanClass);
        this.beans = requireNonNull(beans);
    }

    public Class<E> getBeanClass() {
        return beanClass;
    }

    @Override
    public E get(int index) {
        return beans.get(index);
    }

    @Override
    public int size() {
        return beans.size();
    }
}
