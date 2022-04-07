package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.util.ReversedList;
import org.dbunit.dataset.AbstractDataSet;
import org.dbunit.dataset.ITableIterator;

import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * A {@link org.dbunit.dataset.IDataSet} perspective on a set of Java beans.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BeanDataSet extends AbstractDataSet {

    /**
     * returns a {@link BeanDataSet} based on a single collection of Java beans.
     *
     * @param beanList                  the list of Java beans that returned {@link org.dbunit.dataset.IDataSet}
     *                                  should contain.
     * @param beanTableMetaDataProvider the {@link BeanTableMetaDataProvider} used to resolve
     *                                  {@link org.dbunit.dataset.ITableMetaData} for the beans.
     */
    public static BeanDataSet singletonSet(BeanList<?> beanList, BeanTableMetaDataProvider beanTableMetaDataProvider) {
        List<BeanList<?>> beanLists = singletonList(beanList);
        return new BeanDataSet(beanLists, beanTableMetaDataProvider);
    }

    private final List<BeanList<?>> beansDataSet;
    private final BeanTableMetaDataProvider beanTableMetaDataProvider;


    /**
     * Creates a {@link BeanDataSet} based on many collections of Java beans.
     *
     * @param beansDataSet              a list of {@link BeanList} that this {@link BeanDataSet} should contain.
     * @param beanTableMetaDataProvider the {@link BeanTableMetaDataProvider} used to resolve
     *                                  {@link org.dbunit.dataset.ITableMetaData} for the beans.
     */
    public BeanDataSet(List<BeanList<?>> beansDataSet, BeanTableMetaDataProvider beanTableMetaDataProvider) {

        this.beansDataSet = beansDataSet;
        this.beanTableMetaDataProvider = beanTableMetaDataProvider;
    }

    @Override
    protected ITableIterator createIterator(boolean reversed) {
        List<BeanList<?>> beanLists = Collections.unmodifiableList(beansDataSet);

        if (reversed) {
            beanLists = new ReversedList<>(beanLists);
        }

        return new BeanTableIterator(beanLists, beanTableMetaDataProvider);
    }

}
