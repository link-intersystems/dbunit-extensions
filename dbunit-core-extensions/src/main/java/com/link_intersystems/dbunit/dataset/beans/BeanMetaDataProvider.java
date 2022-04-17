package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.Property;
import com.link_intersystems.beans.PropertyDesc;
import org.dbunit.dataset.Column;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface BeanMetaDataProvider {


    PropertyDesc<Object>[] getProperties(Column[] columns);
}
