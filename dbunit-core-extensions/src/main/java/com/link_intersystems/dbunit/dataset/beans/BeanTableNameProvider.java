package com.link_intersystems.dbunit.dataset.beans;

public interface BeanTableNameProvider {

    public String getTableName(Class<?> beanClass);
}
