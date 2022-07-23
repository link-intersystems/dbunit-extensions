package com.link_intersystems.dbunit.migration.detection.xls;

import com.link_intersystems.dbunit.migration.detection.DataSetFile;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class XlsDataSetFile implements DataSetFile {

    private File file;

    XlsDataSetFile(File file) {
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
