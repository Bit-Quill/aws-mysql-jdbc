package testsuite.integration.container;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({
    AuroraMysqlIntegrationTest.class,
    AuroraMysqlFailoverIntegrationTest.class,
    AuroraMysqlHardFailureIntegrationTest.class,
    ReplicationFailoverIntegrationTest.class
})
public class IntegrationTestSuite {}
