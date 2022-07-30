package com.link_intersystems.dbunit.migration.resources;

import com.link_intersystems.io.FilePath;
import com.link_intersystems.io.FileScanner;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class DataSetFileLocationsScanner implements DataSetFileLocations {

    private final FileScanner fileScanner;

    public DataSetFileLocationsScanner() {
        this(Paths.get(System.getProperty("user.dir")));
    }

    public DataSetFileLocationsScanner(Path basepath) {
        this(defaultFileScanner(basepath));
    }

    public DataSetFileLocationsScanner(FileScanner fileScanner) {
        this.fileScanner = requireNonNull(fileScanner);
    }

    @NotNull
    private static FileScanner defaultFileScanner(Path basepath) {
        FileScanner fileScanner = new FileScanner(basepath);
        fileScanner.addIncludeFilePattern("**/*.xml", "*.xml", "**/*.xls", "*.xls", "**/*.csv", "*.csv");
        return fileScanner;
    }

    @Override
    public List<FilePath> getPaths() {
        return fileScanner.scan();
    }
}
