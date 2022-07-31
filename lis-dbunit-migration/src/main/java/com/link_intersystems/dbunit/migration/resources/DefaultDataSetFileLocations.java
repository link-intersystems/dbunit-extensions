package com.link_intersystems.dbunit.migration.resources;

import java.io.File;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDataSetFileLocations extends AbstractList<File> implements DataSetFileLocations {

    private List<File> paths = new ArrayList<>();

    @Override
    public List<File> getPaths() {
        return this;
    }

    @Override
    public void add(int index, File element) {
        paths.add(index, element);
    }

    @Override
    public File set(int index, File element) {
        return paths.set(index, element);
    }

    @Override
    public File remove(int index) {
        return paths.remove(index);
    }

    @Override
    public File get(int index) {
        return paths.get(index);
    }

    @Override
    public int size() {
        return paths.size();
    }
}
