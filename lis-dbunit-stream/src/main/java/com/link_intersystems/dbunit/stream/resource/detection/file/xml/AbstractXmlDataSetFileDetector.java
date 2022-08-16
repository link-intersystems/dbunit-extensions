package com.link_intersystems.dbunit.stream.resource.detection.file.xml;

import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetector;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractXmlDataSetFileDetector implements DataSetFileDetector {
    @Override
    public DataSetFile detect(File file) {
        if (file.isDirectory()) {
            return null;
        }

        if (file.getName().endsWith(".xml")) {
            return detectXmlFile(file);
        }

        return null;
    }

    protected abstract DataSetFile detectXmlFile(File file);
}
