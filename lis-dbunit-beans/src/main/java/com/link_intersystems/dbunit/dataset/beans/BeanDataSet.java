package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.util.ReversedList;
import org.dbunit.dataset.AbstractDataSet;
import org.dbunit.dataset.ITableIterator;

import java.util.*;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * A {@link org.dbunit.dataset.IDataSet} perspective on a set of Java beans.
 *
 * @author - René Link {@literal <rene.link@link-intersystems.com>}
 */
public class BeanDataSet extends AbstractDataSet {

    private final List<BeanList<?>> beansDataSet;
    private final BeanTableMetaDataProvider beanTableMetaDataProvider;

    /**
     * returns a {@link BeanDataSet} based on a single collection of Java beans.
     *
     * @param beanList                  the list of Java beans that returned {@link org.dbunit.dataset.IDataSet}
     *                                  should contain.
     * @param beanTableMetaDataProvider the {@link BeanTableMetaDataProvider} used to resolve
     *                                  {@link org.dbunit.dataset.ITableMetaData} for the beans.
     */
    public static BeanDataSet singletonSet(BeanList<?> beanList, BeanTableMetaDataProvider beanTableMetaDataProvider) {
        List<BeanList<?>> beanLists = singletonList(beanList);
        return new BeanDataSet(beanLists, beanTableMetaDataProvider);
    }

    /**
     * Create a {@link BeanDataSet} with a {@link DefaultBeanTableMetaDataProvider} that registered all classes that
     * the passed BeanList's have.
     */
    public BeanDataSet(List<BeanList<?>> beansDataSet) {
        this(beansDataSet, createDefaultBeanTableMetaDataProvider(beansDataSet));
    }

    private static BeanTableMetaDataProvider createDefaultBeanTableMetaDataProvider(List<BeanList<?>> beansDataSet) {
        Class<?>[] beanClasses = beansDataSet.stream().map(BeanList::getBeanClass).distinct().toArray(Class<?>[]::new);
        return new DefaultBeanTableMetaDataProvider(beanClasses);
    }

    /**
     * Creates a {@link BeanDataSet} based on the {@link BeanList}s of Java beans and the {@link BeanTableMetaDataProvider}.
     * <p>
     * If you pass multiple {@link BeanList}s with the same {@link BeanList#getBeanClass()} the list will be joined.
     * The order of the joined beans in the result list is not defined. If you expect a specific order you should
     * ensure that you only pass one {@link BeanList} for each bean type.
     *
     * @param beansDataSet              a list of {@link BeanList} that this {@link BeanDataSet} should contain.
     * @param beanTableMetaDataProvider the {@link BeanTableMetaDataProvider} used to resolve
     *                                  {@link org.dbunit.dataset.ITableMetaData} for the beans.
     */
    public BeanDataSet(List<BeanList<?>> beansDataSet, BeanTableMetaDataProvider beanTableMetaDataProvider) {
        Map<Class<?>, List<BeanList<?>>> beanLists = new LinkedHashMap<>();

        for (BeanList<?> beanList : beansDataSet) {
            Class<?> beanClass = beanList.getBeanClass();
            List<BeanList<?>> allListsForClass = beanLists.computeIfAbsent(beanClass, (bc) -> new ArrayList<>());
            allListsForClass.add(beanList);
        }

        this.beansDataSet = beanLists.values().stream()
                .flatMap(this::joinBeanLists)
                .collect(toList());
        this.beanTableMetaDataProvider = beanTableMetaDataProvider;
    }

    private Stream<BeanList<?>> joinBeanLists(List<BeanList<?>> lists) {
        return Stream.of(lists.stream().reduce(BeanList::join).orElseThrow(IllegalStateException::new));
    }

    @Override
    protected ITableIterator createIterator(boolean reversed) {
        List<BeanList<?>> beanLists = Collections.unmodifiableList(beansDataSet);

        if (reversed) {
            beanLists = new ReversedList<>(beanLists);
        }

        return new BeanTableIterator(beanLists, beanTableMetaDataProvider);
    }

}
