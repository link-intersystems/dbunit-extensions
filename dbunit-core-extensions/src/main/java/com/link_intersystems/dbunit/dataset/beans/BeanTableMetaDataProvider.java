package com.link_intersystems.dbunit.dataset.beans;

/**
 *  @author - René Link &lt;rene.link@link-intersystems.com&gt;
 */
public interface BeanTableMetaDataProvider {
    BeanTableMetaData getMetaData(Class<?> beanClass) throws Exception;
}
