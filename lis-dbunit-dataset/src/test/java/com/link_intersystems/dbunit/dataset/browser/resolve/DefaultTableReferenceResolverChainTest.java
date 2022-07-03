package com.link_intersystems.dbunit.dataset.browser.resolve;

import com.link_intersystems.dbunit.dataset.browser.model.BrowseTable;
import com.link_intersystems.dbunit.dataset.browser.model.BrowseTableReference;
import com.link_intersystems.jdbc.ConnectionMetaData;
import com.link_intersystems.jdbc.TableReferenceException;
import com.link_intersystems.jdbc.test.db.sakila.SakilaSlimTestDBExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
@ExtendWith(SakilaSlimTestDBExtension.class)
class DefaultTableReferenceResolverChainTest {

    private DefaultTableReferenceResolverChain resolverChain;
    private Connection connection;
    private ConnectionMetaData connectionMetaData;

    @BeforeEach
    void setUp(Connection sakilaConnection) {
        connection = sakilaConnection;
        connectionMetaData = new ConnectionMetaData(connection);
        resolverChain = new DefaultTableReferenceResolverChain(connectionMetaData);
    }

    @Test
    void handleNoReferenceFound() {
        BrowseTable actor = new BrowseTable("actor");
        actor.browse("language");
        BrowseTableReference languageReference = actor.getReferences().get(0);

        assertThrows(TableReferenceException.class, () -> resolverChain.getTableReference("actor", languageReference));
    }

    @Test
    void exceptionOnHandleNoReferenceFound() {
        BrowseTable actor = new BrowseTable("actor");
        actor.browse("language");
        BrowseTableReference languageReference = actor.getReferences().get(0);
        SQLException sqlException = new SQLException();


        resolverChain = new DefaultTableReferenceResolverChain(connectionMetaData){
            @Override
            protected void tryHandleNoReferenceFound(String sourceTableName, String targetTableName) throws SQLException {
                throw sqlException;
            }
        };

        assertThrows(TableReferenceException.class, () -> resolverChain.getTableReference("actor", languageReference));
    }
}