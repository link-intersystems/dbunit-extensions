package com.link_intersystems.dbunit.migration.resources;

import com.link_intersystems.io.FilePath;

import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetFileLocations {

    public List<FilePath> getPaths();
}
