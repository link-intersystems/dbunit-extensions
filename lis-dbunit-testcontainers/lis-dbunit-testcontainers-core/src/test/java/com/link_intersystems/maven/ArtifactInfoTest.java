package com.link_intersystems.maven;

import com.link_intersystems.dbunit.testcontainers.DatabaseContainerSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author René Link {@literal <rene.link@link-intersystems.com>}
 */
class ArtifactInfoTest {

    @Test
    void artifactInfo() {
        ArtifactInfo artifactInfo = new ArtifactInfo(DatabaseContainerSupport.class);

        String groupId = artifactInfo.getGroupId();

        assertEquals("com.link-intersystems.dbunit.testcontainers", groupId);
    }

}