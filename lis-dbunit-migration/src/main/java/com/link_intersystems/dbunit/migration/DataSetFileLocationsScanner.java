package com.link_intersystems.dbunit.migration;

import com.link_intersystems.io.FilePath;
import com.link_intersystems.io.FileScanner;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFileLocationsScanner implements DataSetFileLocations {

    private final FileScanner fileScanner;

    public DataSetFileLocationsScanner() {
        this(Paths.get(System.getProperty("user.dir")));
    }

    public DataSetFileLocationsScanner(File basedir) {
        this(basedir.toPath());
    }

    public DataSetFileLocationsScanner(Path basepath) {
        fileScanner = new FileScanner(basepath);
    }

    public void addDefaultFilePatterns() {
        addFilePatterns("**/*.xml", "*.xml", "**/*.xls", "*.xls", "**/*.csv", "*.csv");
    }


    public void addFilePatterns(String... globPattern) {
        fileScanner.addFilePattern(globPattern);
    }

    public void addDirectoryPatterns(String... globPattern) {
        fileScanner.addDirectoryPatterns(globPattern);
    }

    @Override
    public List<FilePath> getPaths() {
        return fileScanner.scan();
    }
}
