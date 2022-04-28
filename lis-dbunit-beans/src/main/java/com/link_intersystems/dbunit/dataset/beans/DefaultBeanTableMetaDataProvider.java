package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.BeanClassException;
import com.link_intersystems.beans.BeansFactory;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultBeanTableMetaDataProvider implements BeanTableMetaDataProvider {

    private BeansFactory beansFactory;

    private Map<Class<?>, BeanTableMetaData> beanTableMetaDataByType = new HashMap<>();
    private Map<String, BeanTableMetaData> beanTableMetaDataByName = new HashMap<>();
    private PropertyConversion propertyConversion;

    public DefaultBeanTableMetaDataProvider(Class<?>... beanClasses) throws BeanClassException {
        this(BeansFactory.getDefault(), new DefaultPropertyConversion(), beanClasses);
    }

    public DefaultBeanTableMetaDataProvider(PropertyConversion propertyConversion, Class<?>... beanClasses) throws BeanClassException {
        this(BeansFactory.getDefault(), propertyConversion, beanClasses);
    }

    public DefaultBeanTableMetaDataProvider(BeansFactory beansFactory, Class<?>... beanClasses) throws BeanClassException {
        this(beansFactory, new DefaultPropertyConversion(), beanClasses);
    }


    public DefaultBeanTableMetaDataProvider(BeansFactory beansFactory, PropertyConversion propertyConversion, Class<?>... beanClasses) throws BeanClassException {
        this.beansFactory = requireNonNull(beansFactory);
        this.propertyConversion = requireNonNull(propertyConversion);

        for (Class<?> beanClass : beanClasses) {
            registerBeanClass(beanClass);
        }
    }

    public void registerBeanClass(Class<?> clazz) throws BeanClassException {
        BeanClass<?> beanClass = beansFactory.createBeanClass(clazz);
        BeanTableMetaData beanTableMetaData = new BeanTableMetaData(beanClass, propertyConversion);
        beanTableMetaDataByType.put(beanClass.getType(), beanTableMetaData);
        beanTableMetaDataByName.put(beanTableMetaData.getTableName(), beanTableMetaData);
    }

    @Override
    public IBeanTableMetaData getMetaData(Class<?> beanClass) {
        return beanTableMetaDataByType.get(beanClass);
    }

    @Override
    public IBeanTableMetaData getMetaData(String tableName) {
        return beanTableMetaDataByName.get(tableName);
    }
}
