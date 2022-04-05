package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.util.ReversedList;
import org.dbunit.dataset.AbstractDataSet;
import org.dbunit.dataset.ITableIterator;

import java.util.Collections;
import java.util.List;

public class BeanDataSet extends AbstractDataSet {

    private final List<BeanList<?>> beansDataSet;
    private final BeanTableMetaDataProvider beanTableMetaDataProvider;

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
