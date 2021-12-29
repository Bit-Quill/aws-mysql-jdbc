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

package com.mysql.cj.jdbc.ha;

/**
 * ClusterAwareConnectionProxyTest class.
 */
public class ConnectionProxyTest {
  // @Mock private ConnectionImpl mockConnection;
  // @Mock private TopologyService mockTopologyService;
  // @Mock private ConnectionProvider mockConnectionProvider;
  // @Mock private JdbcPropertySet mockPropertySet;
  // @Mock private NativeSession mockSession;
  // @Mock private Log mockLogger;
  // @Mock private ConnectionPluginManager mockPluginManager;
  // @Mock private WriterFailoverHandler mockWriterFailoverHandler;
  // @Mock private ReaderFailoverHandler mockReaderFailoverHandler;
  // @Mock private RuntimeProperty<Boolean> mockLocalSessionState;
  // @Mock private RuntimeProperty<Integer> mockConnectTimeout;
  // @Mock private RuntimeProperty<Integer> mockSocketTimeout;
  //
  // private final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer", "test");
  // private final HostInfo readerHost = ClusterAwareTestUtils.createBasicHostInfo("reader", "test");
  // private final List<HostInfo> mockTopology = new ArrayList<>(Arrays.asList(writerHost, readerHost));
  // private AutoCloseable closeable;
  //
  // @BeforeEach
  // void init() throws SQLException {
  //   closeable = MockitoAnnotations.openMocks(this);
  //
  //   when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class))).thenReturn(mockTopology);
  //   when(mockTopologyService.getHostByName(mockConnection)).thenReturn(writerHost);
  //
  //   when(mockConnectionProvider.connect(any(HostInfo.class))).thenReturn(mockConnection);
  //   when(mockConnection.getSession()).thenReturn(mockSession);
  //   when(mockSession.getLog()).thenReturn(mockLogger);
  //   when(mockConnection.getPropertySet()).thenReturn(mockPropertySet);
  //
  //   when(mockLocalSessionState.getValue()).thenReturn(false);
  //   when(mockConnectTimeout.getValue()).thenReturn(0);
  //   when(mockPropertySet.getBooleanProperty(PropertyKey.useLocalSessionState)).thenReturn(mockLocalSessionState);
  //   when(mockPropertySet.getIntegerProperty(PropertyKey.connectTimeout)).thenReturn(mockConnectTimeout);
  //   when(mockPropertySet.getIntegerProperty(PropertyKey.socketTimeout)).thenReturn(mockSocketTimeout);
  // }
  //
  // @AfterEach
  // void cleanUp() throws Exception {
  //   closeable.close();
  // }
  //
  // /**
  //  * Tests {@link ClusterAwareConnectionProxy} return original connection if failover is not
  //  * enabled.
  //  */
  // @Test
  // public void testFailoverDisabled() throws SQLException {
  //   final String url =
  //       "jdbc:mysql:aws://somehost:1234/test?"
  //           + PropertyKey.enableClusterAwareFailover.getKeyName()
  //           + "=false";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertFalse(proxy.enableFailoverSetting);
  //   assertSame(mockConnection, proxy.getConnection());
  // }
  //
  // @Test
  // public void testIfClusterTopologyAvailable() throws SQLException {
  //   final String url =
  //       "jdbc:mysql:aws://somehost:1234/test?"
  //           + PropertyKey.clusterInstanceHostPattern.getKeyName()
  //           + "=?.somehost";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertFalse(proxy.isRds());
  //   assertFalse(proxy.isRdsProxy());
  //   assertTrue(proxy.isClusterTopologyAvailable());
  //   assertTrue(proxy.isFailoverEnabled());
  //   verify(mockTopologyService, never()).setClusterId(any());
  //   verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  // }
  //
  // @Test
  // public void testIfClusterTopologyNotAvailable() throws SQLException {
  //   final String url = "jdbc:mysql:aws://somehost:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final List<HostInfo> emptyTopology = new ArrayList<>();
  //   when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class)))
  //       .thenReturn(emptyTopology);
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertFalse(proxy.isRds());
  //   assertFalse(proxy.isRdsProxy());
  //   assertFalse(proxy.isClusterTopologyAvailable());
  //   assertFalse(proxy.isFailoverEnabled());
  // }
  //
  // @Test
  // public void testIfClusterTopologyAvailableAndDnsPatternRequired() {
  //   final String url = "jdbc:mysql:aws://somehost:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   assertThrows(
  //       SQLException.class,
  //       () -> getClusterAwareConnectionProxy(conStr));
  // }
  //
  // @Test
  // public void testRdsCluster() throws SQLException {
  //   final JdbcPropertySet propertySet = new JdbcPropertySetImpl();
  //
  //   when(mockConnection.getSession()).thenReturn(mockSession);
  //   when(mockSession.getLog()).thenReturn(mockLogger);
  //   when(mockConnection.getPropertySet()).thenReturn(propertySet);
  //
  //   final String url =
  //       "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertTrue(proxy.isRds());
  //   assertFalse(proxy.isRdsProxy());
  //   assertTrue(proxy.isClusterTopologyAvailable());
  //   assertTrue(proxy.isFailoverEnabled());
  //   verify(mockTopologyService, atLeastOnce())
  //       .setClusterId("my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234");
  //   verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  // }
  //
  // @Test
  // public void testRdsReaderCluster() throws SQLException {
  //   final String url =
  //       "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertTrue(proxy.isRds());
  //   assertFalse(proxy.isRdsProxy());
  //   assertTrue(proxy.isClusterTopologyAvailable());
  //   assertTrue(proxy.isFailoverEnabled());
  //   verify(mockTopologyService, atLeastOnce())
  //       .setClusterId("my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234");
  //   verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  // }
  //
  // @Test
  // public void testRdsCustomCluster() throws SQLException {
  //   final String url =
  //       "jdbc:mysql:aws://my-custom-cluster-name.cluster-custom-XYZ.us-east-2.rds.amazonaws.com:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertTrue(proxy.isRds());
  //   assertFalse(proxy.isRdsProxy());
  //   assertTrue(proxy.isClusterTopologyAvailable());
  //   assertTrue(proxy.isFailoverEnabled());
  //   verify(mockTopologyService, never()).setClusterId(any());
  //   verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  // }
  //
  // @Test
  // public void testRdsInstance() throws SQLException {
  //   final String url = "jdbc:mysql:aws://my-instance-name.XYZ.us-east-2.rds.amazonaws.com:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertTrue(proxy.isRds());
  //   assertFalse(proxy.isRdsProxy());
  //   assertTrue(proxy.isClusterTopologyAvailable());
  //   assertTrue(proxy.isFailoverEnabled());
  //   verify(mockTopologyService, never()).setClusterId(any());
  //   verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  // }
  //
  // @Test
  // public void testRdsProxy() throws SQLException {
  //   final String url = "jdbc:mysql:aws://test-proxy.proxy-XYZ.us-east-2.rds.amazonaws.com:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertTrue(proxy.isRds());
  //   assertTrue(proxy.isRdsProxy());
  //   assertTrue(proxy.isClusterTopologyAvailable());
  //   assertFalse(proxy.isFailoverEnabled());
  //   verify(mockTopologyService, atLeastOnce())
  //       .setClusterId("test-proxy.proxy-XYZ.us-east-2.rds.amazonaws.com:1234");
  //   verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  // }
  //
  // @Test
  // public void testCustomDomainCluster() throws SQLException {
  //   final String url =
  //       "jdbc:mysql:aws://my-custom-domain.com:1234/test?"
  //           + PropertyKey.clusterInstanceHostPattern.getKeyName()
  //           + "=?.my-custom-domain.com:9999";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertFalse(proxy.isRds());
  //   assertFalse(proxy.isRdsProxy());
  //   assertTrue(proxy.isClusterTopologyAvailable());
  //   assertTrue(proxy.isFailoverEnabled());
  //   verify(mockTopologyService, never()).setClusterId(any());
  //   verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  // }
  //
  // @Test
  // public void testIpAddressCluster() throws SQLException {
  //   final String url =
  //       "jdbc:mysql:aws://10.10.10.10:1234/test?"
  //           + PropertyKey.clusterInstanceHostPattern.getKeyName()
  //           + "=?.my-custom-domain.com:9999";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertFalse(proxy.isRds());
  //   assertFalse(proxy.isRdsProxy());
  //   assertTrue(proxy.isClusterTopologyAvailable());
  //   assertTrue(proxy.isFailoverEnabled());
  //   verify(mockTopologyService, never()).setClusterId(any());
  //   verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  // }
  //
  // @Test
  // public void testIpAddressClusterWithClusterId() throws SQLException {
  //   final String url =
  //       "jdbc:mysql:aws://10.10.10.10:1234/test?"
  //           + PropertyKey.clusterInstanceHostPattern.getKeyName()
  //           + "=?.my-custom-domain.com:9999&"
  //           + PropertyKey.clusterId.getKeyName()
  //           + "=test-cluster-id";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertFalse(proxy.isRds());
  //   assertFalse(proxy.isRdsProxy());
  //   assertTrue(proxy.isClusterTopologyAvailable());
  //   assertTrue(proxy.isFailoverEnabled());
  //   verify(mockTopologyService, atLeastOnce()).setClusterId("test-cluster-id");
  //   verify(mockTopologyService, atLeastOnce()).setClusterInstanceTemplate(any());
  // }
  //
  // @Test
  // public void testIpAddressAndTopologyAvailableAndDnsPatternRequired() {
  //   final String url = "jdbc:mysql:aws://10.10.10.10:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   assertThrows(
  //       SQLException.class,
  //       () -> getClusterAwareConnectionProxy(conStr));
  // }
  //
  // @Test
  // public void testIpAddressAndTopologyNotAvailable() throws SQLException {
  //   final String url = "jdbc:mysql:aws://10.10.10.10:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final List<HostInfo> emptyTopology = new ArrayList<>();
  //   when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class)))
  //       .thenReturn(emptyTopology);
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertFalse(proxy.isRds());
  //   assertFalse(proxy.isRdsProxy());
  //   assertFalse(proxy.isClusterTopologyAvailable());
  //   assertFalse(proxy.isFailoverEnabled());
  // }
  //
  // @Test
  // public void testReadOnlyFalseWhenWriterCluster() throws SQLException {
  //   final String url =
  //       "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
  //   final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
  //   final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
  //   final List<HostInfo> topology = new ArrayList<>();
  //   topology.add(writerHost);
  //   topology.add(readerA_Host);
  //   topology.add(readerB_Host);
  //
  //   when(mockTopologyService.getCachedTopology()).thenReturn(topology);
  //   when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class))).thenReturn(topology);
  //   when(mockTopologyService.getHostByName(mockConnection)).thenReturn(writerHost);
  //   when(mockConnectionProvider.connect(refEq(writerHost))).thenReturn(mockConnection);
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertTrue(proxy.isCurrentConnectionWriter());
  //   assertFalse(proxy.explicitlyReadOnly);
  //   assertFalse(proxy.isCurrentConnectionReadOnly());
  // }
  //
  // @Test
  // public void testReadOnlyTrueWhenReaderCluster() throws SQLException {
  //   final String url =
  //       "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
  //
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final int connectionHostIndex = 1;
  //
  //   final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
  //   final HostInfo readerAHost = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
  //   final List<HostInfo> topology = new ArrayList<>();
  //   topology.add(writerHost);
  //   topology.add(readerAHost);
  //
  //   when(mockConnectionProvider.connect(refEq(readerAHost))).thenReturn(mockConnection);
  //   when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class))).thenReturn(topology);
  //   when(mockTopologyService.getCachedTopology()).thenReturn(topology);
  //   when(mockTopologyService.getHostByName(mockConnection)).thenReturn(readerAHost);
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertEquals(connectionHostIndex, proxy.currentHostIndex);
  //   assertTrue(proxy.explicitlyReadOnly);
  //   assertTrue(proxy.isCurrentConnectionReadOnly());
  // }
  //
  // @Test
  // public void testLastUsedReaderAvailable() throws SQLException {
  //   final String url =
  //       "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
  //
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final int newConnectionHostIndex = 1;
  //
  //   final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
  //   final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
  //   final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
  //   final List<HostInfo> topology = new ArrayList<>();
  //   topology.add(writerHost);
  //   topology.add(readerA_Host);
  //   topology.add(readerB_Host);
  //
  //   when(mockConnectionProvider.connect(refEq(readerA_Host))).thenReturn(mockConnection);
  //
  //   when(mockTopologyService.getCachedTopology()).thenReturn(topology);
  //   when(mockTopologyService.getLastUsedReaderHost()).thenReturn(readerA_Host);
  //   when(mockTopologyService.getTopology(eq(mockConnection), any(Boolean.class))).thenReturn(topology);
  //   when(mockTopologyService.getHostByName(mockConnection)).thenReturn(readerA_Host);
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertEquals(newConnectionHostIndex, proxy.currentHostIndex);
  //   assertTrue(proxy.explicitlyReadOnly);
  //   assertTrue(proxy.isCurrentConnectionReadOnly());
  // }
  //
  // @Test
  // public void testForWriterReconnectWhenInvalidInitialWriterConnection() throws SQLException {
  //   final ConnectionImpl mockCachedWriterConn = mockConnection;
  //   final ConnectionImpl mockActualWriterConn = mockConnection;
  //
  //   final String url =
  //       "jdbc:mysql:aws://my-cluster-name.cluster-XYZ.us-east-2.rds.amazonaws.com:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final HostInfo cachedWriterHost = ClusterAwareTestUtils.createBasicHostInfo("cached-writer-host", "test");
  //   final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
  //   final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
  //   final List<HostInfo> cachedTopology = new ArrayList<>();
  //   cachedTopology.add(cachedWriterHost);
  //   cachedTopology.add(readerA_Host);
  //   cachedTopology.add(readerB_Host);
  //
  //   final HostInfo actualWriterHost = ClusterAwareTestUtils.createBasicHostInfo("actual-writer-host", "test");
  //   final HostInfo obsoleteWriterHost = ClusterAwareTestUtils.createBasicHostInfo("obsolete-writer-host", "test");
  //   final List<HostInfo> actualTopology = new ArrayList<>();
  //   actualTopology.add(actualWriterHost);
  //   actualTopology.add(readerA_Host);
  //   actualTopology.add(obsoleteWriterHost);
  //
  //   when(mockTopologyService.getCachedTopology()).thenReturn(cachedTopology);
  //   when(mockTopologyService.getTopology(eq(mockCachedWriterConn), any(Boolean.class)))
  //       .thenReturn(actualTopology);
  //   when(mockTopologyService.getHostByName(mockCachedWriterConn)).thenReturn(obsoleteWriterHost);
  //
  //   ConnectionProvider mockConnectionProvider = Mockito.mock(ConnectionProvider.class);
  //   when(mockConnectionProvider.connect(refEq(cachedWriterHost))).thenReturn(mockCachedWriterConn);
  //   when(mockConnectionProvider.connect(refEq(actualWriterHost))).thenReturn(mockActualWriterConn);
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertEquals(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX, proxy.currentHostIndex);
  //   assertEquals(actualWriterHost, proxy.hosts.get(proxy.currentHostIndex));
  //   assertFalse(proxy.explicitlyReadOnly);
  //   assertFalse(proxy.isCurrentConnectionReadOnly());
  // }
  //
  // @Test
  // public void testForWriterReconnectWhenDirectReaderConnectionFails() throws SQLException {
  //   // Although the user specified an instance that happened to be a reader, they have not
  //   // explicitly specified that they want a reader.
  //   // It is possible that they don't know the reader/writer status of this instance, so we cannot
  //   // assume they want a read-only connection.
  //   // As a result, if this direct connection fails, we should reconnect to the writer.
  //   final ConnectionImpl mockDirectReaderConn = mockConnection;
  //   final ConnectionImpl mockWriterConn = mockConnection;
  //
  //   final String url = "jdbc:mysql:aws://reader-b-host.XYZ.us-east-2.rds.amazonaws.com";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", null);
  //   final HostInfo readerAHost = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", null);
  //   final HostInfo readerBHost = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", null);
  //   final List<HostInfo> topology = new ArrayList<>();
  //   topology.add(writerHost);
  //   topology.add(readerAHost);
  //   topology.add(readerBHost);
  //
  //   when(mockTopologyService.getTopology(eq(mockDirectReaderConn), any(Boolean.class)))
  //       .thenReturn(topology);
  //   when(mockTopologyService.getHostByName(mockDirectReaderConn))
  //       .thenReturn(null);
  //
  //   when(mockConnectionProvider.connect(conStr.getMainHost())).thenReturn(mockDirectReaderConn);
  //   when(mockConnectionProvider.connect(refEq(writerHost))).thenReturn(mockWriterConn);
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //
  //   assertEquals(FailoverConnectionPlugin.WRITER_CONNECTION_INDEX, proxy.currentHostIndex);
  //   assertNull(proxy.explicitlyReadOnly);
  //   assertFalse(proxy.isCurrentConnectionReadOnly());
  //   assertTrue(proxy.isFailoverEnabled());
  // }
  //
  // @Test
  // public void testConnectToWriterFromReaderOnSetReadOnlyFalse() throws SQLException {
  //   final ConnectionImpl mockReaderConnection = mockConnection;
  //   final ConnectionImpl mockWriterConnection = mockConnection;
  //
  //   final String url =
  //       "jdbc:mysql:aws://my-cluster-name.cluster-ro-XYZ.us-east-2.rds.amazonaws.com:1234/test";
  //   final ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, new Properties());
  //
  //   final HostInfo writerHost = ClusterAwareTestUtils.createBasicHostInfo("writer-host", "test");
  //   final HostInfo readerA_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-a-host", "test");
  //   final HostInfo readerB_Host = ClusterAwareTestUtils.createBasicHostInfo("reader-b-host", "test");
  //   final List<HostInfo> topology = new ArrayList<>();
  //   topology.add(writerHost);
  //   topology.add(readerA_Host);
  //   topology.add(readerB_Host);
  //
  //   when(mockTopologyService.getTopology(eq(mockReaderConnection), any(Boolean.class)))
  //       .thenReturn(topology);
  //   when(mockTopologyService.getHostByName(mockReaderConnection)).thenReturn(readerB_Host);
  //
  //   when(mockConnectionProvider.connect(conStr.getMainHost())).thenReturn(mockReaderConnection);
  //   when(mockConnectionProvider.connect(refEq(writerHost))).thenReturn(mockWriterConnection);
  //
  //   final ClusterAwareConnectionProxy proxy = getClusterAwareConnectionProxy(conStr);
  //   assertTrue(proxy.isCurrentConnectionReadOnly());
  //   assertTrue(proxy.explicitlyReadOnly);
  //
  //   final JdbcConnection connectionProxy =
  //       (JdbcConnection)
  //           java.lang.reflect.Proxy.newProxyInstance(
  //               JdbcConnection.class.getClassLoader(),
  //               new Class<?>[]{JdbcConnection.class},
  //               proxy);
  //   connectionProxy.setReadOnly(false);
  //
  //   assertFalse(proxy.explicitlyReadOnly);
  //   assertTrue(proxy.isCurrentConnectionWriter());
  // }
  //
  // private ClusterAwareConnectionProxy getClusterAwareConnectionProxy(ConnectionUrl connectionUrl) throws SQLException {
  //   return new ClusterAwareConnectionProxy(
  //       connectionUrl,
  //       mockConnectionProvider,
  //       mockTopologyService,
  //       mockWriterFailoverHandler,
  //       mockReaderFailoverHandler,
  //       (log) -> mockPluginManager);
  // }
}
