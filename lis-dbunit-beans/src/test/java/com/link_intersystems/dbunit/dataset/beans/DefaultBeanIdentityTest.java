package com.link_intersystems.dbunit.dataset.beans;

import com.link_intersystems.beans.BeanClass;
import com.link_intersystems.beans.BeansFactory;
import com.link_intersystems.beans.PropertyDesc;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class DefaultBeanIdentityTest {

    private BeansFactory beansFactory;
    private DefaultBeanIdentity beanIdentity;

    @BeforeEach
    void setUp() {
        beansFactory = BeansFactory.getDefault();
        beanIdentity = new DefaultBeanIdentity();
    }

    @Test
    void setIdPropertyNames() {
        class BeanNoDefaultIdProperty {
            public String getIdentifierString() {
                return null;
            }
        }

        beanIdentity.setIdPropertyNames("identifierString");
        BeanClass<BeanNoDefaultIdProperty> noDefaultIdPropertyBeanClass = beansFactory.createBeanClass(BeanNoDefaultIdProperty.class);

        List<PropertyDesc> idProperties = beanIdentity.getIdProperties(noDefaultIdPropertyBeanClass);

        assertNotNull(idProperties);
        assertEquals(1, idProperties.size());

    }

    @Test
    void getDefaultIdPropertyNames() {
        List<String> idPropertyNames = beanIdentity.getIdPropertyNames();

        assertEquals(Arrays.asList("id", "uuid"), idPropertyNames);
    }

    @Test
    void getIdProperties() {
        class BeanWithIdProperty {
            public String getId() {
                return null;
            }
        }

        BeanClass<BeanWithIdProperty> idBeanClass = beansFactory.createBeanClass(BeanWithIdProperty.class);
        List<PropertyDesc> idProperties = beanIdentity.getIdProperties(idBeanClass);

        assertNotNull(idProperties);
        assertEquals(1, idProperties.size());
    }

    @Test
    void getIdPropertiesUuid() {
        class BeanWithUuidProperty {
            public String getUuid() {
                return null;
            }
        }

        BeanClass<BeanWithUuidProperty> uuidBeanClass = beansFactory.createBeanClass(BeanWithUuidProperty.class);
        List<PropertyDesc> idProperties = beanIdentity.getIdProperties(uuidBeanClass);

        assertNotNull(idProperties);
        assertEquals(1, idProperties.size());
    }

    @Test
    void getIdPropertiesNoId() {
        class BeanNoDefaultIdProperty {
            public String getIdentifierString() {
                return null;
            }
        }

        BeanClass<BeanNoDefaultIdProperty> noDefaultIdProperty = beansFactory.createBeanClass(BeanNoDefaultIdProperty.class);
        List<PropertyDesc> idProperties = beanIdentity.getIdProperties(noDefaultIdProperty);

        assertNotNull(idProperties);
        assertEquals(0, idProperties.size());
    }

    @Test
    void getCompositeIdPropertiesOrderShouldBeTheDefinedNamesOrder() {
        class BeanWithCompositeId {
            public String getId() {
                return null;
            }

            public String getUuid() {
                return null;
            }

            public String getIdentifierString() {
                return null;
            }
        }

        // "id", "identifierString", "uuid",
        beanIdentity.setIdPropertyNames( "identifierString", "id", "uuid");

        BeanClass<BeanWithCompositeId> compositeIdBeanClass = beansFactory.createBeanClass(BeanWithCompositeId.class);
        List<PropertyDesc> idProperties = beanIdentity.getIdProperties(compositeIdBeanClass);

        assertNotNull(idProperties);
        assertEquals(3, idProperties.size());

        assertEquals("uuid", idProperties.get(2).getName());
        assertEquals("identifierString", idProperties.get(0).getName());
        assertEquals("id", idProperties.get(1).getName());
    }
}