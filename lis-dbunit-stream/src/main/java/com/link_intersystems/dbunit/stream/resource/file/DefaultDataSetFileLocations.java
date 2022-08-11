package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFileLocations;

import java.io.File;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DefaultDataSetFileLocations extends AbstractList<File> implements DataSetFileLocations {

    private List<File> files = new ArrayList<>();

    @Override
    public List<File> getFiles() {
        return this;
    }

    @Override
    public void add(int index, File element) {
        files.add(index, element);
    }

    @Override
    public File set(int index, File element) {
        return files.set(index, element);
    }

    @Override
    public File remove(int index) {
        return files.remove(index);
    }

    @Override
    public File get(int index) {
        return files.get(index);
    }

    @Override
    public int size() {
        return files.size();
    }
}
