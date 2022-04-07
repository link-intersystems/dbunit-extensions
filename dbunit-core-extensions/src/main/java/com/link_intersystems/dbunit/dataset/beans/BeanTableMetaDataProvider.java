package com.link_intersystems.dbunit.dataset.beans;

import org.dbunit.dataset.ITableMetaData;

/**
 * Provides {@link IBeanTableMetaData} for a given {@link Class}.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface BeanTableMetaDataProvider {

    /**
     * Returns the {@link IBeanTableMetaData} for a given bean class.
     *
     * @param beanClass the bean class.
     * @throws Exception if the metadata can not be resolved.
     */
    IBeanTableMetaData getMetaData(Class<?> beanClass) throws Exception;

    IBeanTableMetaData getMetaData(ITableMetaData tableMetaData);
}
