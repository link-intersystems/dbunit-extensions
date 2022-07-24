package com.link_intersystems.dbunit.stream.consumer;

import com.link_intersystems.dbunit.table.TableOrder;
import org.dbunit.dataset.stream.DefaultConsumer;
import org.dbunit.dataset.stream.IDataSetConsumer;

import static java.util.Objects.requireNonNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class ExternalSortTableCosumerTransformer implements DataSetTransormer {

    private TableOrder tableOrder;
    private ExternalSortTableConsumer externalSortTableConsumer;

    public ExternalSortTableCosumerTransformer(TableOrder tableOrder) {
        this.tableOrder = requireNonNull(tableOrder);
    }

    @Override
    public IDataSetConsumer getInputConsumer() {
        return externalSortTableConsumer == null ? new DefaultConsumer() : externalSortTableConsumer;
    }

    @Override
    public void setOutputConsumer(IDataSetConsumer dataSetConsumer) {
        externalSortTableConsumer = new ExternalSortTableConsumer(dataSetConsumer, tableOrder);
    }
}
