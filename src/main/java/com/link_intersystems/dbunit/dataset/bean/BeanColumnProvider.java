package com.link_intersystems.dbunit.dataset.bean;

import org.dbunit.dataset.Column;

public interface BeanColumnProvider {

    public Column[] getColumns(Class<?> beanClass) throws Exception;
}
