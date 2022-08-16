package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.dbunit.stream.resource.DataSetResource;
import org.dbunit.dataset.DataSetException;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetFile extends DataSetResource {

    DataSetFile withNewFile(File file) throws DataSetException;

    File getFile();
}
