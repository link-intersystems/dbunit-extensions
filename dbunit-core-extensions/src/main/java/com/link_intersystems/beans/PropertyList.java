package com.link_intersystems.beans;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class PropertyList extends AbstractList<Property> {

    private List<Property> propertyDescs = new ArrayList<>();

    public PropertyList(List<Property> propertyDescs) {

        this.propertyDescs.addAll(propertyDescs);
    }

    public Property getByName(String name) {
        return getByName(name, Objects::equals);
    }

    public Property getByName(String name, Equality<String> nameEquality) {
        return stream().filter(pd -> nameEquality.isEqual(name, pd.getName())).findFirst().orElse(null);
    }

    @Override
    public Property get(int index) {
        return propertyDescs.get(index);
    }

    @Override
    public int size() {
        return propertyDescs.size();
    }

    public Property[] asArray() {
        return toArray(new Property[size()]);
    }
}
