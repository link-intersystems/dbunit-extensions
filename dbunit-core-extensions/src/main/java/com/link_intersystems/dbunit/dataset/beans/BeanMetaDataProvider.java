package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.Property;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.ITableMetaData;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface BeanMetaDataProvider {


    Property[] getProperties(Column[] columns);
}
