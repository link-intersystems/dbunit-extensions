package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.Property;

import java.util.List;

/**
 *  @author - René Link &lt;rene.link@link-intersystems.com&gt;
 */
public interface BeanIdentity {
    List<Property> getIdProperties(BeanClass beanClass) throws Exception;
}
