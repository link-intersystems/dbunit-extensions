package com.link_intersystems.dbunit.test;

import com.link_intersystems.io.Unzip;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TinySakilaDataSetFiles {

    private Path basepath;

    public TinySakilaDataSetFiles(Path basepath) {
        this.basepath = basepath;
    }

    public static TinySakilaDataSetFiles create(Path targetPath) {
        try {
            Unzip.unzip(TinySakilaDataSetFiles.class.getResourceAsStream("/tiny-sakila-dataset-files.zip"), targetPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new TinySakilaDataSetFiles(targetPath);
    }

    public Path getCsvDataSetDir() {
        return basepath.resolve("tiny-sakila-csv");
    }

    public Path getXlsDataSetFile() {
        return basepath.resolve("tiny-sakila.xls");
    }

    public Path getFlatXmlDataSetPath() {
        return basepath.resolve("tiny-sakila-flat.xml");
    }

    public Path getXmlDataSetPath() {
        return basepath.resolve("tiny-sakila.xml");
    }
}
