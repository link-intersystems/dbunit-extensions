package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.BeanClassException;
import com.link_intersystems.beans.BeansFactory;
import org.dbunit.dataset.ITableMetaData;

import java.util.HashMap;
import java.util.Map;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultBeanTableMetaDataProvider implements BeanTableMetaDataProvider {

    private BeansFactory beansFactory;

    private Map<Class<?>, IBeanTableMetaData> beanTableMetaDataByType = new HashMap<>();
    private Map<String, IBeanTableMetaData> beanTableMetaDataByName = new HashMap<>();

    public DefaultBeanTableMetaDataProvider(Class<?>... beanClasses) throws BeanClassException {
        this(BeansFactory.getDefault(), beanClasses);
    }

    public DefaultBeanTableMetaDataProvider(BeansFactory beansFactory, Class<?>... beanClasses) throws BeanClassException {
        this.beansFactory = beansFactory;

        for (Class<?> beanClass : beanClasses) {
            registerBeanClass(beanClass);
        }
    }

    public void registerBeanClass(Class<?> clazz) throws BeanClassException {
        BeanClass<?> beanClass = beansFactory.createBeanClass(clazz);
        BeanTableMetaData beanTableMetaData = new BeanTableMetaData(beanClass);
        beanTableMetaDataByType.put(beanClass.getType(), beanTableMetaData);
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
