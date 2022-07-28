package com.link_intersystems.dbunit.stream.resource.file;

import java.nio.file.Path;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetFileDetector {

    DataSetFile detect(Path filePath);
}
