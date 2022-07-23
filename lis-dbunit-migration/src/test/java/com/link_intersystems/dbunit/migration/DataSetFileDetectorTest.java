package com.link_intersystems.dbunit.migration;

import com.link_intersystems.dbunit.migration.detection.DataSetFile;
import com.link_intersystems.dbunit.migration.detection.DataSetFileDetection;
import com.link_intersystems.io.Unzip;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DataSetFileDetectorTest {

    private Path tmpDir;

    @BeforeEach
    void setUp(@TempDir Path tmpDir) throws IOException {
        this.tmpDir = tmpDir;
        Unzip.unzip(DataSetFileDetectorTest.class.getResourceAsStream("/tiny-sakila-dataset-files.zip"), tmpDir);
    }

    @Test
    void csv(){
        DataSetFileDetection dataSetFileDetector = new DataSetFileDetection();
        File file = Paths.get(tmpDir.toString(), "tiny-sakila-csv").toFile();
        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertNotNull(dataSetFile);
    }

    @Test
    void xls(){
        DataSetFileDetection dataSetFileDetector = new DataSetFileDetection();
        File file = Paths.get(tmpDir.toString(), "tiny-sakila.xls").toFile();
        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertNotNull(dataSetFile);
    }

    @Test
    void flatXml(){
        DataSetFileDetection dataSetFileDetector = new DataSetFileDetection();
        File file = Paths.get(tmpDir.toString(), "tiny-sakila-flat.xml").toFile();
        DataSetFile dataSetFile = dataSetFileDetector.detect(file);

        assertNotNull(dataSetFile);
    }
}