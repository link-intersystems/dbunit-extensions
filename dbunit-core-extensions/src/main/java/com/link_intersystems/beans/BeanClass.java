package com.link_intersystems.beans;

import java.util.List;

/**
 *  @author - René Link &lt;rene.link@link-intersystems.com&gt;
 */
public interface BeanClass {

    String getSimpleName();

    List<Property> getProperties();
}
