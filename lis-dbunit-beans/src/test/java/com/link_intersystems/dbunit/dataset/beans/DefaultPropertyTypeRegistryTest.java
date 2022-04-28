package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.test.UnitTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@UnitTest
class DefaultPropertyTypeRegistryTest {

    private DefaultPropertyTypeRegistry propertyTypeRegistry;

    static Stream<Class<?>> supportedValueConverterTargetTypes() {
        List<Class<?>> targetTypes = new ArrayList<>();

        targetTypes.add(Integer.class);
        targetTypes.add(Long.class);
        targetTypes.add(Double.class);
        targetTypes.add(Float.class);

        targetTypes.add(Integer.TYPE);
        targetTypes.add(Long.TYPE);
        targetTypes.add(Double.TYPE);
        targetTypes.add(Float.TYPE);

        targetTypes.add(Boolean.class);
        targetTypes.add(Boolean.TYPE);

        targetTypes.add(BigInteger.class);
        targetTypes.add(BigDecimal.class);
        targetTypes.add(String.class);

        targetTypes.add(Time.class);
        targetTypes.add(Timestamp.class);
        targetTypes.add(Date.class);
        targetTypes.add(java.sql.Date.class);

        targetTypes.add(byte[].class);

        return targetTypes.stream();
    }

    @BeforeEach
    void setUp() {
        propertyTypeRegistry = new DefaultPropertyTypeRegistry();
    }

    @ParameterizedTest
    @DisplayName("ValueConverters")
    @MethodSource("supportedValueConverterTargetTypes")
    void noValueConversions(Class<?> supportedType) {
        PropertyType valueConverter = propertyTypeRegistry.getPropertyType(supportedType);
        assertNotEquals(DefaultPropertyTypeRegistry.UNKNOWN_VALUE_CONVERTER, valueConverter, "Should not be an unknown converter.");
        assertNotNull(valueConverter, supportedType::getSimpleName);
    }
}