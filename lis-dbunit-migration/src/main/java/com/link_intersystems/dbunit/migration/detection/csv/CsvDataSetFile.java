package com.link_intersystems.dbunit.migration.detection.csv;

import com.link_intersystems.dbunit.migration.detection.DataSetFile;
import org.dbunit.dataset.stream.IDataSetConsumer;
import org.dbunit.dataset.stream.IDataSetProducer;

import java.io.File;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class CsvDataSetFile implements DataSetFile {

    private File file;

    CsvDataSetFile(File file) {
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
