package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import software.aws.rds.jdbc.mysql.Driver;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

@TestMethodOrder(MethodOrderer.Alphanumeric.class)
public class ConnectionPluginManagerTest {
    @Mock IConnectionPlugin plugin;
    @Mock ClusterAwareConnectionProxy proxy;
    @Mock PropertySet propertySet;

    private AutoCloseable openMocks;
    private Log logger = new NullLogger(this.getClass().getName());

    private static final int NUM_OF_MANAGERS = 5;

    @BeforeEach
    void setUp() {
        openMocks = MockitoAnnotations.openMocks(this);
        Mockito.doNothing()
            .when(plugin).releaseResources();
    }

    @Test
    void test_1_ReleasingAllWithEmptyList() {
        ConnectionPluginManager.releaseAllResources();
        Assert.assertEquals(0, ConnectionPluginManager.instances.size());
    }

    @Test
    void test_2_ReleasingAllWithEmptyListUsingDriver() {
        Driver.releasePluginManager();
        Assert.assertEquals(0, ConnectionPluginManager.instances.size());
    }

    @Test
    void test_3_ReleasingAllWithInstanceInList() {
        for (int i = 0; i < NUM_OF_MANAGERS; i++) {
            createPluginManager();
        }

        Assert.assertEquals(NUM_OF_MANAGERS, ConnectionPluginManager.instances.size());
        ConnectionPluginManager.releaseAllResources();
        Assert.assertEquals(0, ConnectionPluginManager.instances.size());
    }

    @Test
    void test_4_ReleasingAllWithInstanceInListUsingDriver() {
        for (int i = 0; i < NUM_OF_MANAGERS; i++) {
            createPluginManager();
        }

        Assert.assertEquals(NUM_OF_MANAGERS, ConnectionPluginManager.instances.size());
        Driver.releasePluginManager();
        Assert.assertEquals(0, ConnectionPluginManager.instances.size());
    }

    private void createPluginManager() {
        ConnectionPluginManager pluginManager = spy(new ConnectionPluginManager(logger));
        doAnswer(invocation -> {
            pluginManager.headPlugin = plugin;
            pluginManager.instances.add(pluginManager);
            return null;
        }).when(pluginManager).init(proxy, propertySet);

        pluginManager.init(proxy, propertySet);
    }
}
