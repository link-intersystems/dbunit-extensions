package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.Property;
import com.link_intersystems.beans.PropertyDesc;

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Resolved the identity {@link Property}s of a {@link BeanClass}.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface BeanIdentity {

    public static final BeanIdentity NULL_IDENTITY = beanClass -> emptyList();

    /**
     * Returns the identity properties of a Java bean class.
     *
     * @param beanClass the {@link BeanClass}.
     * @return the identity properties of a Java bean class. Never <code>null</code>, return an empty list instead.
     * @throws Exception if the identity properties can not be resolved for any reason.
     */
    List<PropertyDesc<?>> getIdProperties(BeanClass<?> beanClass) throws Exception;
}
