package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.io.FilePath;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetFileDetector {

    DataSetFile detect(FilePath filePath);
}
