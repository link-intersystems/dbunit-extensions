package com.link_intersystems.dbunit.dataset.beans;

/**
 *  @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface BeanTableMetaDataProvider {
    BeanTableMetaData getMetaData(Class<?> beanClass) throws Exception;
}
