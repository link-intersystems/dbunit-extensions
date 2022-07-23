package com.link_intersystems.dbunit.migration.detection.xml;

import com.link_intersystems.dbunit.migration.detection.DataSetFile;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XmlDataSetFile implements DataSetFile {

    private File file;

    XmlDataSetFile(File file) {
        this.file = file;
    }

    @Override
    public IDataSetProducer createProducer() {
        return null;
    }

    @Override
    public IDataSetConsumer createConsumer() {
        return null;
    }
}
