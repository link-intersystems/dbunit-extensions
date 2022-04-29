package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.PropertyDesc;
import com.link_intersystems.beans.PropertyDescList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
        PropertyDescList properties = beanClass.getProperties();

        Predicate<PropertyDesc> idPropertyDescPredicate = pd -> idPropertyNames.contains(pd.getName());
        return properties.stream()
                .filter(idPropertyDescPredicate)
                .sorted(this::idPropertyNamesOrder)
                .collect(Collectors.toList());
    }

    private int idPropertyNamesOrder(PropertyDesc desc1, PropertyDesc desc2) {
        String name1 = desc1.getName();
        int index1 = idPropertyNames.indexOf(name1);

        String name2 = desc2.getName();
        int index2 = idPropertyNames.indexOf(name2);

        return Integer.compare(index1, index2);
    }
}

