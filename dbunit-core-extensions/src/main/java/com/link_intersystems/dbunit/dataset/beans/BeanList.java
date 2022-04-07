package com.link_intersystems.dbunit.dataset.beans;

import java.util.AbstractList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A collection of Java beans that belong to the {@link com.link_intersystems.beans.BeanClass}.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BeanList<E> extends AbstractList<E> {

    private Class<? extends E> beanClass;
    private List<E> beans;

    /**
     * Creates a {@link BeanList} based on the given {@link com.link_intersystems.beans.BeanClass} and beans.
     *
     * @param beanClass the {@link com.link_intersystems.beans.BeanClass} that all beans belong to.
     * @param beans     the bean elements of this list.
     */
    public BeanList(Class<? extends E> beanClass, List<E> beans) {
        this.beanClass = requireNonNull(beanClass);
        this.beans = requireNonNull(beans);
    }

    /**
     * Returns the {@link com.link_intersystems.beans.BeanClass} of this {@link BeanList}.
     */
    public Class<? extends E> getBeanClass() {
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
