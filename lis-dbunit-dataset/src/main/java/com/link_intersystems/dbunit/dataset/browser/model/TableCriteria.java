package com.link_intersystems.dbunit.dataset.browser.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
public class TableCriteria {

    private List<TableCriterion> criterionList = new ArrayList<>();

    void addCriterion(TableCriterion tableCriterion) {
        criterionList.add(tableCriterion);
    }

    public List<TableCriterion> getCriterions() {
        return criterionList;
    }
}
