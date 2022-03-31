package com.link_intersystems.dbunit.dataset.bean;

public interface BeanTableNameProvider {

    public String getTableName(Class<?> beanClass);
}
