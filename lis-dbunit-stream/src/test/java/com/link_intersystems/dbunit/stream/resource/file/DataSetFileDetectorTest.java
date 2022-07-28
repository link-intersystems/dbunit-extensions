package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.dbunit.stream.resource.file.csv.CsvDataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xls.XlsDataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xml.FlatXmlDataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xml.XmlDataSetFile;
import com.link_intersystems.io.Unzip;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetFileDetectorTest {

    private Path tmpDir;
    private DataSetFileDetection dataSetFileDetector;

    @BeforeEach
    void setUp(@TempDir Path tmpDir) throws IOException {
        this.tmpDir = tmpDir;
        Unzip.unzip(DataSetFileDetectorTest.class.getResourceAsStream("/tiny-sakila-dataset-files.zip"), tmpDir);

        dataSetFileDetector = new DataSetFileDetection();
    }

    @Test
    void csv() {
        Path file = getFile("tiny-sakila-csv");

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(CsvDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void xls() {
        Path file = getFile("tiny-sakila.xls");

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(XlsDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void flatXml() {
        Path file = getFile("tiny-sakila-flat.xml");

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(FlatXmlDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void xml() {
        Path file = getFile("tiny-sakila.xml");

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(XmlDataSetFile.class, dataSetFile.getClass());
    }

    private Path getFile(String filename) {
        return tmpDir.resolve(filename);
    }
}