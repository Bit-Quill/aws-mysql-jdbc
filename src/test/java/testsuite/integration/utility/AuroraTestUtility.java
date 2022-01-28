/*
 * AWS JDBC Driver for MySQL
 * Copyright Amazon.com Inc. or affiliates.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package testsuite.integration.utility;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AmazonEC2Exception;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
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
import com.amazonaws.waiters.WaiterParameters;
import com.amazonaws.waiters.WaiterTimedOutException;
import com.amazonaws.waiters.WaiterUnrecoverableException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Creates and destroys AWS RDS Cluster and Instances
 * AWS Credentials is loaded using DefaultAWSCredentialsProviderChain
 *      Environment Variable > System Properties > Web Identity Token > Profile Credentials > EC2 Container
 * To specify which to credential provider, use AuroraTestUtility(String region, AWSCredentialsProvider credentials) *
 *
 * If using environment variables for credential provider
 *     Required
 *     - AWS_ACCESS_KEY
 *     - AWS_SECRET_KEY
 */
public class AuroraTestUtility {
    // Default values
    private static String dbUsername = "my_test_username";
    private static String dbPassword = "my_test_password";
    private static String dbIdentifier = "test-identifier";
    private static String dbEngine = "aurora-mysql";
    private static String dbInstanceClass = "db.r5.large";
    private static String dbRegion = "us-east-1";
    private static String dbSecGroup = "default";
    private static int numOfInstances = 5;

    private static AmazonRDS rdsClient = null;
    private static AmazonEC2 ec2Client = null;
    private static String runnerIP = "";

    private static final String DUPLICATE_IP_ERROR_CODE = "InvalidPermission.Duplicate";

    /**
     * Initializes an AmazonRDS & AmazonEC2 client.
     * RDS client used to create/destroy clusters & instances.
     * EC2 client used to add/remove IP from security group.
     */
    public AuroraTestUtility() {
        this(dbRegion, new DefaultAWSCredentialsProviderChain());
    }

    /**
     * Initializes an AmazonRDS & AmazonEC2 client.
     * @param region define AWS Regions, refer to https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
     */
    public AuroraTestUtility(String region) {
        this(region, new DefaultAWSCredentialsProviderChain());
    }

    /**
     * Initializes an AmazonRDS & AmazonEC2 client.
     * @param region define AWS Regions, refer to https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html
     * @param credentials Specific AWS credential provider
     */
    public AuroraTestUtility(String region, AWSCredentialsProvider credentials) {
        dbRegion = region;

        rdsClient = AmazonRDSClientBuilder
            .standard()
            .withRegion(dbRegion)
            .withCredentials(credentials)
            .build();

         ec2Client = AmazonEC2ClientBuilder
            .standard()
            .withRegion(dbRegion)
            .withCredentials(credentials)
            .build();
    }

    /**
     * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
     * @param username Master username for access to database
     * @param password Master password for access to database
     * @param identifier Database cluster identifier
     * @param engine Database engine to use, refer to https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Welcome.html#:~:text=AWS%20Management%20Console.-,DB%20engines,-A%20DB%20engine
     * @param instanceClass instance class, refer to https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.DBInstanceClass.html
     * @param instances number of instances to spin up
     * @return An endpoint for one of the instances
     * @throws InterruptedException when clusters have not started after 30 minutes
     */
    public String initCluster(String username, String password, String identifier, String engine, String instanceClass, int instances)
        throws InterruptedException {
        dbUsername = username;
        dbPassword = password;
        dbIdentifier = identifier;
        dbEngine = engine;
        dbInstanceClass = instanceClass;
        numOfInstances = instances;
        return initCluster();
    }

    /**
     * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
     * @param username Master username for access to database
     * @param password Master password for access to database
     * @param identifier Database identifier,
     * @return An endpoint for one of the instances
     * @throws InterruptedException when clusters have not started after 30 minutes
     */
    public String initCluster(String username, String password, String identifier)
        throws InterruptedException {
        dbUsername = username;
        dbPassword = password;
        dbIdentifier = identifier;
        return initCluster();
    }

    /**
     * Creates RDS Cluster/Instances and waits until they are up, and proper IP whitelisting for databases.
     * @return An endpoint for one of the instances
     * @throws InterruptedException when clusters have not started after 30 minutes
     */
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
            addRunnerIP();
        } catch (WaiterUnrecoverableException | WaiterTimedOutException | TimeoutException | ExecutionException exception) {
            teardown();
            throw new InterruptedException("Unable to start AWS RDS Cluster & Instances after waiting for 30 minutes");
        } catch (UnknownHostException exception) {
            throw new InterruptedException("Unable to authorize runner IP");
        }

        DescribeDBInstancesResult dbInstancesResult = rdsClient.describeDBInstances(dbInstancesRequest);
        return dbInstancesResult.getDBInstances().get(0).getEndpoint().getAddress();
    }

    /**
     * Adds current runner's public IP to EC2 Security groups for RDS access.
     * @throws UnknownHostException
     */
    private void addRunnerIP() throws UnknownHostException {
        try {
            URL ipChecker = new URL("http://checkip.amazonaws.com");
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                ipChecker.openStream()));
            runnerIP = reader.readLine();
        } catch (Exception e) {
            throw new UnknownHostException("Unable to get IP");
        }

        AuthorizeSecurityGroupIngressRequest authRequest = new AuthorizeSecurityGroupIngressRequest()
            .withGroupName(dbSecGroup)
            .withCidrIp(runnerIP + "/32")
            .withIpProtocol("-1") // All protocols
            .withFromPort(0) // For all ports
            .withToPort(65535);

        try {
            ec2Client.authorizeSecurityGroupIngress(authRequest);
        } catch (AmazonEC2Exception exception) {
            if (!DUPLICATE_IP_ERROR_CODE.equalsIgnoreCase(exception.getErrorCode())) {
                throw exception;
            }
        }
    }

    /**
     * Removes current runner's public IP from EC2 Security groups.
     * @throws UnknownHostException
     */
    private void removeRunnerIP() {
        RevokeSecurityGroupIngressRequest revokeRequest = new RevokeSecurityGroupIngressRequest()
            .withGroupName(dbSecGroup)
            .withCidrIp(runnerIP + "/32")
            .withIpProtocol("-1") // All protocols
            .withFromPort(0) // For all ports
            .withToPort(65535);

        try {
            ec2Client.revokeSecurityGroupIngress(revokeRequest);
        } catch (AmazonEC2Exception exception) {
            // Ignore
        }
    }

    /**
     * Destroys all instances and clusters. Removes IP from EC2 whitelist.
     * @param identifier
     */
    public void teardown(String identifier) {
        dbIdentifier = identifier;
        teardown();
    }

    /**
     * Destroys all instances and clusters. Removes IP from EC2 whitelist.
     */
    public void teardown() {
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
        removeRunnerIP();
    }
}
