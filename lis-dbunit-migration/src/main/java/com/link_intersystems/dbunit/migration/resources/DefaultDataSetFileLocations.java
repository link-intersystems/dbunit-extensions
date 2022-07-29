package com.link_intersystems.dbunit.migration.resources;

import com.link_intersystems.io.FilePath;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDataSetFileLocations extends AbstractList<FilePath> implements DataSetFileLocations {

    private List<FilePath> paths = new ArrayList<>();

    @Override
    public List<FilePath> getPaths() {
        return this;
    }

    @Override
    public void add(int index, FilePath element) {
        paths.add(index, element);
    }

    @Override
    public FilePath set(int index, FilePath element) {
        return paths.set(index, element);
    }

    @Override
    public FilePath remove(int index) {
        return paths.remove(index);
    }

    @Override
    public FilePath get(int index) {
        return paths.get(index);
    }

    @Override
    public int size() {
        return paths.size();
    }
}
