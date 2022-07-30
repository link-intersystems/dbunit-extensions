package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.dbunit.stream.resource.file.csv.CsvDataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xls.XlsDataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xml.FlatXmlDataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xml.XmlDataSetFile;
import com.link_intersystems.dbunit.test.TinySakilaDataSetFiles;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetFileDetectorTest {

    private DataSetFileDetection dataSetFileDetector;
    private TinySakilaDataSetFiles dataSetFiles;

    @BeforeEach
    void setUp(@TempDir Path tmpDir) {
        dataSetFiles = TinySakilaDataSetFiles.create(tmpDir);
        dataSetFileDetector = new DataSetFileDetection();
    }

    @Test
    void csv() {
        Path file = dataSetFiles.getCsvDataSetDir();

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(CsvDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void xls() {
        Path file = dataSetFiles.getXlsDataSetFile();

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(XlsDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void flatXml() {
        Path file = dataSetFiles.getFlatXmlDataSetPath();

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(FlatXmlDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void xml() {
        Path file = dataSetFiles.getXmlDataSetPath();

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(XmlDataSetFile.class, dataSetFile.getClass());
    }
}