package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.migration.detection.DataSetFile;
import com.link_intersystems.dbunit.migration.detection.DataSetFileDetection;
import com.link_intersystems.dbunit.migration.detection.csv.CsvDataSetFile;
import com.link_intersystems.dbunit.migration.detection.xls.XlsDataSetFile;
import com.link_intersystems.dbunit.migration.detection.xml.FlatXmlDataSetFile;
import com.link_intersystems.dbunit.migration.detection.xml.XmlDataSetFile;
import com.link_intersystems.io.Unzip;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

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
    void csv(){
        File file = getFile("tiny-sakila-csv");

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(CsvDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void xls(){
        File file = getFile("tiny-sakila.xls");

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(XlsDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void flatXml(){
        File file = getFile("tiny-sakila-flat.xml");

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(FlatXmlDataSetFile.class, dataSetFile.getClass());
    }

    @Test
    void xml(){
        File file = getFile("tiny-sakila.xml");

        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertEquals(XmlDataSetFile.class, dataSetFile.getClass());
    }

    @NotNull
    private File getFile(String filename) {
        return Paths.get(tmpDir.toString(), filename).toFile();
    }
}