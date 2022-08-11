package com.link_intersystems.dbunit.stream.resource.file;

import com.link_intersystems.dbunit.stream.resource.detection.DataSetFileDetection;
import com.link_intersystems.dbunit.stream.resource.file.csv.CsvDataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xls.XlsDataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xml.FlatXmlDataSetFile;
import com.link_intersystems.dbunit.stream.resource.file.xml.XmlDataSetFile;
import com.link_intersystems.dbunit.test.TinySakilaDataSetFiles;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
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
        File file = dataSetFiles.getCsvDataSetDir().toFile();

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(CsvDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void xls() {
        File file = dataSetFiles.getXlsDataSetFile().toFile();

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(XlsDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void flatXml() {
        File file = dataSetFiles.getFlatXmlDataSetPath().toFile();

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(FlatXmlDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void xml() {
        File file = dataSetFiles.getXmlDataSetPath().toFile();

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(XmlDataSetFile.class, dataSetFile.getClass());
    }
}