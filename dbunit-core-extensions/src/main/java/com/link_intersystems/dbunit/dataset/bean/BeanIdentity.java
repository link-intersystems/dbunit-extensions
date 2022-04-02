package com.link_intersystems.dbunit.dataset.bean;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.Property;

import java.util.List;

/**
 * @author René Link <rene.link@link-intersystems.com>
 */
public interface BeanIdentity {
    List<Property> getIdProperties(BeanClass beanClass) throws Exception;
}
