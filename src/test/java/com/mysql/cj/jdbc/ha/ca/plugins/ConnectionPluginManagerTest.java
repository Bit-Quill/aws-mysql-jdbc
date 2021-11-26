package com.mysql.cj.jdbc.ha.ca.plugins;

import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.jdbc.ha.ca.ClusterAwareConnectionProxy;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.NullLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.concurrent.CopyOnWriteArrayList;

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

    @BeforeEach
    void setUp() {
        openMocks = MockitoAnnotations.openMocks(this);
        Mockito.doNothing()
            .when(plugin).releaseResources();
    }

    @Test
    void testReleasingAllWithEmptyList() {
        ConnectionPluginManager.releaseAllResources();
    }

    @Test
    void testReleasingAllWithEmptyListUsingDriver() {
        Driver.releasePluginManager();
    }

    @Test
    void testReleasingAllWithInstanceInList() {
        for (int i = 0; i < 5; i++) {
            createPluginManager();
        }

        ConnectionPluginManager.releaseAllResources();
    }

    @Test
    void testReleasingAllWithInstanceInListUsingDriver() {
        for (int i = 0; i < 5; i++) {
            createPluginManager();
        }

        Driver.releasePluginManager();
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
