package testsuite.integration.utility;

import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.services.rds.AmazonRDS;
import com.amazonaws.services.rds.AmazonRDSClientBuilder;
import com.amazonaws.services.rds.model.CreateDBClusterRequest;
import com.amazonaws.services.rds.model.CreateDBInstanceRequest;
import com.amazonaws.services.rds.model.DeleteDBClusterRequest;
import com.amazonaws.services.rds.model.DeleteDBInstanceRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesRequest;
import com.amazonaws.services.rds.model.DescribeDBInstancesResult;
import com.amazonaws.services.rds.model.Filter;
import com.amazonaws.services.rds.waiters.AmazonRDSWaiters;
import com.amazonaws.waiters.NoOpWaiterHandler;
import com.amazonaws.waiters.Waiter;
import com.amazonaws.waiters.WaiterHandler;
import com.amazonaws.waiters.WaiterParameters;
import com.amazonaws.waiters.WaiterTimedOutException;
import com.amazonaws.waiters.WaiterUnrecoverableException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Creates and destroys AWS RDS Cluster and Instances
 * By default, AWS Credentials will be loaded in from environment variables
 * To load credentials with other methods, use AuroraTestUtility(String region, AWSCredentialsProvider credentials)
 *
 * Environment variables to include
 *     Required
 *     - AWS_ACCESS_KEY
 *     - AWS_SECRET_KEY
 */
public class AuroraTestUtility {
    // Default values
    private static String dbUsername = "my_test_username";
    private static String dbPassword = "my_test_password";
    private static String dbIdentifier = "my-identifier-3";
    private static String dbEngine = "aurora-mysql";
    private static String dbInstanceClass = "db.r5.large";
    private static String dbRegion = "us-east-1";
    private static int numOfInstances = 5;

    private static AmazonRDS rdsClient = null;

    /**
     * Creates an AmazonRDS Client using AWS Credentials from environment variables
     * Required Environment Variables
     *  - AWS_ACCESS_KEY
     *  - AWS_SECRET_KEY
     */
    public AuroraTestUtility() {
        rdsClient = AmazonRDSClientBuilder
            .standard()
            .withRegion(dbRegion)
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .build();
    }

    public AuroraTestUtility(String region) {
        dbRegion = region;

        rdsClient = AmazonRDSClientBuilder
            .standard()
            .withRegion(dbRegion)
            .withCredentials(new EnvironmentVariableCredentialsProvider())
            .build();
    }

    public AuroraTestUtility(String region, AWSCredentialsProvider credentials) {
        dbRegion = region;

        rdsClient = AmazonRDSClientBuilder
            .standard()
            .withRegion(dbRegion)
            .withCredentials(credentials)
            .build();
    }

    public String initCluster(String username, String password, String identifier, String engine, String instanceClass, String region, int instances)
        throws InterruptedException {
        dbUsername = username;
        dbPassword = password;
        dbIdentifier = identifier;
        dbEngine = engine;
        dbInstanceClass = instanceClass;
        dbRegion = region;
        numOfInstances = instances;
        return initCluster();
    }

    public String initCluster(String username, String password, String identifier)
        throws InterruptedException {
        dbUsername = username;
        dbPassword = password;
        dbIdentifier = identifier;
        return initCluster();
    }

    public String initCluster() throws InterruptedException {
        // Create Cluster
        CreateDBClusterRequest dbClusterRequest = new CreateDBClusterRequest()
            .withDBClusterIdentifier(dbIdentifier)
            .withMasterUsername(dbUsername)
            .withMasterUserPassword(dbPassword)
            .withSourceRegion(dbRegion)
            .withEnableIAMDatabaseAuthentication(true)
            .withEngine(dbEngine)
            .withStorageEncrypted(true);

        rdsClient.createDBCluster(dbClusterRequest);

        // Create Instances
        CreateDBInstanceRequest dbInstanceRequest = new CreateDBInstanceRequest()
            .withDBClusterIdentifier(dbIdentifier)
            .withDBInstanceClass(dbInstanceClass)
            .withEngine(dbEngine)
            .withPubliclyAccessible(true);

        for (int i = 1; i <= numOfInstances; i++) {
            rdsClient.createDBInstance(dbInstanceRequest.withDBInstanceIdentifier(dbIdentifier + "-" + i));
        }

        // Wait for all instances to be up
        AmazonRDSWaiters waiter = new AmazonRDSWaiters(rdsClient);
        DescribeDBInstancesRequest dbInstancesRequest = new DescribeDBInstancesRequest()
            .withFilters(new Filter().withName("db-cluster-id").withValues(dbIdentifier));
        Waiter<DescribeDBInstancesRequest> instancesRequestWaiter = waiter
            .dBInstanceAvailable();
        Future<Void> future = instancesRequestWaiter.runAsync(new WaiterParameters<>(dbInstancesRequest), new NoOpWaiterHandler());
        try {
            future.get(30, TimeUnit.MINUTES);
        } catch (WaiterUnrecoverableException | WaiterTimedOutException | TimeoutException | ExecutionException exception) {
            teardown();
            throw new InterruptedException("Unable to start AWS RDS Cluster & Instances after waiting for 30 minutes");
        }

        DescribeDBInstancesResult dbInstancesResult = rdsClient.describeDBInstances(dbInstancesRequest);
        String suffix = dbInstancesResult.getDBInstances().get(0).getEndpoint().getAddress();
        suffix = suffix.substring(suffix.indexOf('.'));
        return suffix;
    }

    public void teardown() {
        if (rdsClient == null) return;

        // Tear down instances
        DeleteDBInstanceRequest dbDeleteInstanceRequest = new DeleteDBInstanceRequest()
            .withSkipFinalSnapshot(true);

        for (int i = 1; i <= numOfInstances; i++) {
            rdsClient.deleteDBInstance(dbDeleteInstanceRequest.withDBInstanceIdentifier(dbIdentifier + "-" + i));
        }

        // Tear down cluster
        DeleteDBClusterRequest dbDeleteClusterRequest = new DeleteDBClusterRequest()
            .withSkipFinalSnapshot(true)
            .withDBClusterIdentifier(dbIdentifier);

        rdsClient.deleteDBCluster(dbDeleteClusterRequest);
    }
}
