package com.link_intersystems.dbunit.stream.resource.file;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetFileDetector {

    DataSetFile detect(File file);
}
