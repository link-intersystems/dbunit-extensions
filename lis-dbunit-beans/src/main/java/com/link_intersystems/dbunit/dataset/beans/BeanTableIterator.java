package com.link_intersystems.dbunit.dataset.beans;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;

import java.util.Iterator;
import java.util.List;

/**
 * Adapts {@link BeanList}s to an {@link ITableIterator}.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BeanTableIterator implements ITableIterator {

    private final Iterator<BeanList<?>> beanIterator;
    private BeanTableMetaDataProvider beanTableMetaDataProvider;

    private BeanList<?> beanList;

    /**
     * Creates an {@link ITableIterator} that provides {@link ITable} apations to all
     * {@link BeanList}s it was constructed with.
     *
     * @param beanLists                 the list of {@link BeanList}s that this iterator should iterate through.
     * @param beanTableMetaDataProvider the metadata provider for the beans.
     */
    public BeanTableIterator(List<BeanList<?>> beanLists, BeanTableMetaDataProvider beanTableMetaDataProvider) {
        this.beanIterator = beanLists.iterator();
        this.beanTableMetaDataProvider = beanTableMetaDataProvider;
    }

    @Override
    public boolean next() {
        beanList = null;

        if (beanIterator.hasNext()) {
            beanList = beanIterator.next();
        }

        return beanList != null;
    }

    @Override
    public ITableMetaData getTableMetaData() throws DataSetException {
        return getTable().getTableMetaData();
    }

    @Override
    public ITable getTable() throws DataSetException {
        Class<?> beanClass = beanList.getBeanClass();
        try {
            IBeanTableMetaData beanTableMetaData = beanTableMetaDataProvider.getMetaData(beanClass);
            return new BeanTable<>(beanList, beanTableMetaData);
        } catch (Exception e) {
            throw new DataSetException(e);
        }
    }
}
