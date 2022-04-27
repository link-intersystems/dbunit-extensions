package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.PropertyDesc;
import com.link_intersystems.beans.PropertyDescList;

import java.util.*;
import java.util.function.Predicate;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultBeanIdentity implements BeanIdentity {

    private List<String> idPropertyNames = new ArrayList<>();

    public DefaultBeanIdentity() {
        setIdPropertyNames("id", "uuid");
    }

    public void setIdPropertyNames(String... idPropertyNames) {
        setIdPropertyNames(Arrays.asList(idPropertyNames));
    }

    public void setIdPropertyNames(List<String> idPropertyNames) {
        this.idPropertyNames = new ArrayList<>(idPropertyNames);
    }

    public List<String> getIdPropertyNames() {
        return Collections.unmodifiableList(idPropertyNames);
    }

    @Override
    public List<PropertyDesc> getIdProperties(BeanClass<?> beanClass) {
        List<PropertyDesc> propertyDescs = new ArrayList<>();

        PropertyDescList properties = beanClass.getProperties();

        Predicate<PropertyDesc> idPropertyDescPredicate = pd -> idPropertyNames.contains(pd.getName());
        Optional<PropertyDesc> idPropertyDescOptional = properties.stream()
                .filter(idPropertyDescPredicate)
                .findFirst();

        idPropertyDescOptional.ifPresent(propertyDescs::add);

        return propertyDescs;
    }
}

