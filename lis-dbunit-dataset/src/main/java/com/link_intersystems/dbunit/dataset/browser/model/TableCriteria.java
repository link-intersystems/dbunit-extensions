package com.link_intersystems.dbunit.dataset.browser.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableCriteria criteria = (TableCriteria) o;
        return Objects.equals(criterionList, criteria.criterionList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(criterionList);
    }
}
