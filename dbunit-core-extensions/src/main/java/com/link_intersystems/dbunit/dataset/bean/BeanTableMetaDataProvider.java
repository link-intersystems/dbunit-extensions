package com.link_intersystems.dbunit.dataset.bean;

import java.beans.IntrospectionException;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
public interface BeanTableMetaDataProvider {
    BeanTableMetaData getMetaData(Class<?> beanClass) throws Exception;
}
