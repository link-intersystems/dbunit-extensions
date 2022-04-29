package com.link_intersystems.dbunit.dataset.beans;

import org.dbunit.dataset.DataSetException;

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
     * @throws DataSetException if the metadata can not be resolved.
     */
    IBeanTableMetaData getMetaData(Class<?> beanClass) throws DataSetException;

    /**
     * Returns a {@link IBeanTableMetaData} for a given table name.
     *
     * @param tableName the table name.
     */
    IBeanTableMetaData getMetaData(String tableName);
}
