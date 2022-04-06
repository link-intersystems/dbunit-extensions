package com.link_intersystems.beans;

import java.util.List;

/**
 *  @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface BeanClass {

    String getSimpleName();

    List<Property> getProperties();
}
