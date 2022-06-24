package com.link_intersystems.dbunit.dataset.browser.persistence.file;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;
import com.link_intersystems.dbunit.dataset.browser.persistence.BrowserModelRepository;
import com.link_intersystems.dbunit.dataset.browser.persistence.ModelAlreadyExistsException;
import com.link_intersystems.dbunit.dataset.browser.persistence.ModelPersistenceException;

import java.io.*;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class FileBrowseModelRepository implements BrowserModelRepository {

    private File baseDir;
    private FileExtension fileExtension;
    private BrowseTableSerdes serdes;

    public FileBrowseModelRepository(File baseDir, BrowseTableSerdes serdes, FileExtension fileExtension) {
        this.baseDir = requireNonNull(baseDir);
        this.serdes = requireNonNull(serdes);
        this.fileExtension = requireNonNull(fileExtension);
    }

    @Override
    public void persistModel(BrowseTable browseTable, String name) throws ModelPersistenceException {
        File file = getFile(name);

        if (file.exists()) {
            throw new ModelAlreadyExistsException();
        }

        try (BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file))) {
            serdes.serialize(browseTable, outputStream);
        } catch (Exception e) {
            throw new ModelPersistenceException(e);
        }
    }

    @Override
    public BrowseTable loadModel(String name) throws ModelPersistenceException {
        File file = getFile(name);

        if (!file.exists()) {
            throw new ModelPersistenceException(file + " does not exist");
        }

        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(file))) {
            return serdes.deserialize(inputStream);
        } catch (Exception e) {
            throw new ModelPersistenceException(e);
        }
    }

    private File getFile(String name) {
        String filename = fileExtension.createFilename(name);
        return new File(baseDir, filename);
    }


}
