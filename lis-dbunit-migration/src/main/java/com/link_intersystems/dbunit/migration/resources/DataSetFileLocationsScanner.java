package com.link_intersystems.dbunit.migration.resources;

import com.link_intersystems.io.FileScanner;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFileLocationsScanner implements DataSetFileLocations {

    private final FileScanner fileScanner;
    private Path basepath;

    public DataSetFileLocationsScanner() {
        this(Paths.get(System.getProperty("user.dir")));
    }

    public DataSetFileLocationsScanner(Path basepath) {
        this(defaultFileScanner());
        this.basepath = requireNonNull(basepath);
    }

    public DataSetFileLocationsScanner(FileScanner fileScanner) {
        this.fileScanner = requireNonNull(fileScanner);
    }

    private static FileScanner defaultFileScanner() {
        FileScanner fileScanner = new FileScanner();
        fileScanner.addIncludeFilePattern("**/*.xml", "*.xml", "**/*.xls", "*.xls", "**/*.csv", "*.csv");
        return fileScanner;
    }

    @Override
    public List<File> getPaths() {
        return fileScanner.scan(basepath);
    }
}
