package com.link_intersystems.dbunit.dataset.beans.java;

import com.link_intersystems.dbunit.dataset.beans.BeanTableMetaDataProvider;
import com.link_intersystems.dbunit.dataset.beans.IBeanTableMetaData;
import org.dbunit.dataset.ITableMetaData;

import java.beans.IntrospectionException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class JavaBeanTableMetaDataProvider implements BeanTableMetaDataProvider {

    private Map<Class<?>, IBeanTableMetaData> beanTableMetaDataByType = new HashMap<>();
    private Map<String, IBeanTableMetaData> beanTableMetaDataByName = new HashMap<>();

    public void registerBeanClass(Class<?> beanClass) throws IntrospectionException {
        JavaBeanTableMetaData beanTableMetaData = new JavaBeanTableMetaData(beanClass);
        beanTableMetaDataByType.put(beanClass, beanTableMetaData);
        beanTableMetaDataByName.put(beanTableMetaData.getTableName(), beanTableMetaData);
    }

    @Override
    public IBeanTableMetaData getMetaData(Class<?> beanClass) {
        return beanTableMetaDataByType.get(beanClass);
    }

    @Override
    public IBeanTableMetaData getMetaData(ITableMetaData tableMetaData) {
        return beanTableMetaDataByName.get(tableMetaData.getTableName());
    }
}
