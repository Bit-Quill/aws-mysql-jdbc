package testsuite.integration.container;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

// Tests will run in order of top to bottom.
// To add additional tests, append it inside SelectClasses, comma-separated
@Suite
@SelectClasses({
    AuroraMysqlIntegrationTest.class,
    AuroraMysqlFailoverIntegrationTest.class,
    AuroraMysqlHardFailureIntegrationTest.class,
    ReplicationFailoverIntegrationTest.class
})
public class IntegrationTestSuite {}
