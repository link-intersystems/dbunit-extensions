package com.link_intersystems.dbunit.testcontainers.consumer;

import org.junit.jupiter.api.Test;

import java.security.NoSuchAlgorithmException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class HashedCredentialsTest {

    @Test
    void toStringTest() throws NoSuchAlgorithmException {
        HashedCredentials hashedCredentials = new HashedCredentials("user1", "pass1");
        assertEquals("7063CDD951FA1311939D12698440EE25816E1264E107E2E865200E1F28170D33", hashedCredentials.toString());
    }

    @Test
    void equals() throws NoSuchAlgorithmException {
        HashedCredentials hashedCredentials1 = new HashedCredentials("user1", "pass1");
        HashedCredentials hashedCredentials2 = new HashedCredentials("user1", "pass1");
        HashedCredentials hashedCredentials3 = new HashedCredentials("user2", "pass2");

        assertEquals(hashedCredentials1, hashedCredentials2);
        assertNotEquals(hashedCredentials1, hashedCredentials3);
    }

    @Test
    void nullUserName() throws NoSuchAlgorithmException {
        new HashedCredentials(null, "pass1");
    }

    @Test
    void nullPassword() throws NoSuchAlgorithmException {
        new HashedCredentials("user1", null);
    }
}