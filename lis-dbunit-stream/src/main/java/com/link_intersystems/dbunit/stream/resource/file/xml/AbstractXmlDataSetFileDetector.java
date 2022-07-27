package com.link_intersystems.dbunit.stream.resource.file.xml;

import com.link_intersystems.dbunit.stream.resource.file.DataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.DataSetFileDetector;
import com.link_intersystems.io.FilePath;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public abstract class AbstractXmlDataSetFileDetector implements DataSetFileDetector {
    @Override
    public DataSetFile detect(FilePath filePath) {
        File file = filePath.toAbsoluteFile();
        if (file.isDirectory()) {
            return null;
        }

        if (file.getName().endsWith(".xml")) {
            return detectXmlFile(filePath);
        }

        return null;
    }

    protected abstract DataSetFile detectXmlFile(FilePath filePath);
}
