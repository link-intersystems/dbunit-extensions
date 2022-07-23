package com.link_intersystems.dbunit.migration;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public interface DataSetDetector {

    DataSetFile detect(File file);
}
