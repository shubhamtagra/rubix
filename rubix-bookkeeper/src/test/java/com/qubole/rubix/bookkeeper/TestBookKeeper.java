/**
 * Copyright (c) 2019. Qubole Inc
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.qubole.rubix.bookkeeper;

import com.codahale.metrics.MetricRegistry;
import com.google.common.testing.FakeTicker;
import com.qubole.rubix.bookkeeper.utils.DiskUtils;
import com.qubole.rubix.common.metrics.BookKeeperMetrics;
import com.qubole.rubix.common.utils.DataGen;
import com.qubole.rubix.common.utils.TestUtil;
import com.qubole.rubix.core.CachedReadRequestChain;
import com.qubole.rubix.core.ClusterManagerInitilizationException;
import com.qubole.rubix.core.MockCachingFileSystem;
import com.qubole.rubix.core.ReadRequest;
import com.qubole.rubix.core.utils.DummyClusterManager;
import com.qubole.rubix.hadoop2.Hadoop2ClusterManager;
import com.qubole.rubix.presto.PrestoClusterManager;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.ClusterManager;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.CacheStatusResponse;
import com.qubole.rubix.spi.thrift.FileInfo;
import com.qubole.rubix.spi.thrift.Location;
import com.qubole.rubix.spi.thrift.ReadResponse;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.thrift.shaded.TException;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.FileAssert.fail;

/**
 * Created by Abhishek on 6/15/18.
 */
public class TestBookKeeper
{
  private static final Log log = LogFactory.getLog(TestBookKeeper.class);

  private static final String TEST_CACHE_DIR_PREFIX = TestUtil.getTestCacheDirPrefix("TestBookKeeper");
  private static final String TEST_DNE_CLUSTER_MANAGER = "com.qubole.rubix.core.DoesNotExistClusterManager";
  private static final int TEST_MAX_DISKS = 1;
  private static final String BACKEND_FILE_NAME = "backendFile";
  private static final String TEST_REMOTE_PATH = "/tmp/testPath";
  private static final int TEST_BLOCK_SIZE = 100;
  private static final long TEST_LAST_MODIFIED = 1514764800; // 2018-01-01T00:00:00
  private static final long TEST_FILE_LENGTH = 5000;
  private static final long TEST_START_BLOCK = 20;
  private static final long TEST_END_BLOCK = 23;

  private final Configuration conf = new Configuration();
  private MetricRegistry metrics;
  private BookKeeperMetrics bookKeeperMetrics;

  private BookKeeper bookKeeper;

  @BeforeMethod
  public void setUp() throws IOException
  {
    CacheConfig.setCacheDataDirPrefix(conf, TEST_CACHE_DIR_PREFIX);
    CacheConfig.setBlockSize(conf, TEST_BLOCK_SIZE);

    TestUtil.createCacheParentDirectories(conf, TEST_MAX_DISKS);

    metrics = new MetricRegistry();
    bookKeeperMetrics = new BookKeeperMetrics(conf, metrics);
    bookKeeper = new CoordinatorBookKeeper(conf, bookKeeperMetrics);
    bookKeeper.clusterManager = null;
  }

  @AfterMethod
  public void tearDown() throws Exception
  {
    TestUtil.removeCacheParentDirectories(conf, TEST_MAX_DISKS);
    bookKeeperMetrics.close();
    conf.clear();
  }

  @Test
  public void testGetDummyClusterManagerValidInstance() throws Exception
  {
    ClusterType type = ClusterType.TEST_CLUSTER_MANAGER;
    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);

    assertTrue(manager instanceof DummyClusterManager, " Didn't initialize the correct cluster manager class." +
        " Expected : " + DummyClusterManager.class + " Got : " + manager.getClass());
  }

  @Test
  private void testGetCacheDirSize_WithNoGapInMiddle() throws IOException, TException
  {
    CacheConfig.setBlockSize(conf, 1024 * 1024);
    int downloadSize = 3145728; //3MB
    cacheDirSizeHelper(2000000, downloadSize, false);
  }

  @Test(enabled = false)
  private void testGetCacheDirSize_WithGapInMiddle() throws IOException, TException
  {
    CacheConfig.setBlockSize(conf, 1024 * 1024);
    int downloadSize = 3145728; //3MB
    cacheDirSizeHelper(2000000, downloadSize, true);
  }

  private void cacheDirSizeHelper(long sizeMultiplier, long downloadSize, boolean hasHole) throws IOException, TException
  {
    String testDirectory = CacheConfig.getCacheDirPrefixList(conf) + "0" + CacheConfig.getCacheDataDirSuffix(conf);
    String backendFileName = testDirectory + "testBackendFile";
    long fileSize = DataGen.populateFile(backendFileName, 1, (int) sizeMultiplier);
    final String remotePathWithScheme = "file://" + backendFileName;

    // Read from 30th block to 33rd block
    int offset = 31457280; //30MB
    int holeSize = 5242880;
    int expectedSparseFileSize;
    ReadResponse response = bookKeeper.readData(remotePathWithScheme, offset, (int) downloadSize, fileSize, TEST_LAST_MODIFIED, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    verifyDownloadedData(backendFileName, offset, downloadSize, response.getGenerationNumber());
    expectedSparseFileSize = (int) DiskUtils.bytesToMB(downloadSize);

    if (hasHole) {
      // Create a hole of 5 mb at 34th block
      // Read from 38th block to 40th block

      offset = offset + (int) downloadSize + holeSize; //36MB
      response = bookKeeper.readData(remotePathWithScheme, offset, (int) downloadSize, fileSize, TEST_LAST_MODIFIED, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
      verifyDownloadedData(backendFileName, offset, downloadSize, response.getGenerationNumber());
      expectedSparseFileSize = (int) DiskUtils.bytesToMB(2 * downloadSize);
    }
    long sparseFileSize = DiskUtils.getDirectorySizeInMB(new File(CacheUtil.getLocalPath(remotePathWithScheme, conf, response.getGenerationNumber())));
    assertTrue(sparseFileSize == expectedSparseFileSize, "getDirectorySizeInMB is reporting wrong file Size : " + sparseFileSize);
  }

  private void verifyDownloadedData(String backendFileName, int offset, long downloadSize, int generationNumber) throws IOException
  {
    final String remotePathWithScheme = "file://" + backendFileName;

    int bufferSize = (int) (offset + downloadSize);
    byte[] buffer = new byte[bufferSize];
    FileInputStream localFileInputStream = new FileInputStream(new File(CacheUtil.getLocalPath(remotePathWithScheme, conf, generationNumber)));
    localFileInputStream.read(buffer, 0, bufferSize);

    byte[] backendBuffer = new byte[bufferSize];
    FileInputStream backendFileInputStream = new FileInputStream(new File(backendFileName));
    backendFileInputStream.read(backendBuffer, 0, bufferSize);

    for (int i = offset; i <= downloadSize; i++) {
      assertTrue(buffer[i] == backendBuffer[i], "Got " + buffer[i] + " at " + i + "instead of " + backendBuffer[i]);
    }

    localFileInputStream.close();
    backendFileInputStream.close();
  }

  @Test(expectedExceptions = ClusterManagerInitilizationException.class)
  public void testGetDummyClusterManagerInValidInstance() throws Exception
  {
    ClusterType type = ClusterType.TEST_CLUSTER_MANAGER;
    CacheConfig.setDummyClusterManager(conf, TEST_DNE_CLUSTER_MANAGER);

    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);
  }

  @Test
  public void testGetHadoop2ClusterManagerValidInstance() throws Exception
  {
    ClusterType type = ClusterType.HADOOP2_CLUSTER_MANAGER;
    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);

    assertTrue(manager instanceof Hadoop2ClusterManager, " Didn't initialize the correct cluster manager class." +
        " Expected : " + Hadoop2ClusterManager.class + " Got : " + manager.getClass());
  }

  @Test(expectedExceptions = ClusterManagerInitilizationException.class)
  public void testGetHadoop2ClusterManagerInValidInstance() throws Exception
  {
    ClusterType type = ClusterType.HADOOP2_CLUSTER_MANAGER;
    CacheConfig.setHadoopClusterManager(conf, TEST_DNE_CLUSTER_MANAGER);

    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);
  }

  @Test
  public void testGetPrestoClusterManagerValidInstance() throws Exception
  {
    ClusterType type = ClusterType.PRESTO_CLUSTER_MANAGER;
    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);

    assertTrue(manager instanceof PrestoClusterManager, " Didn't initialize the correct cluster manager class." +
        " Expected : " + PrestoClusterManager.class + " Got : " + manager.getClass());
  }

  @Test(expectedExceptions = ClusterManagerInitilizationException.class)
  public void testGetPrestoClusterManagerInValidInstance() throws Exception
  {
    ClusterType type = ClusterType.PRESTO_CLUSTER_MANAGER;
    CacheConfig.setPrestoClusterManager(conf, TEST_DNE_CLUSTER_MANAGER);

    ClusterManager manager = bookKeeper.getClusterManagerInstance(type, conf);
  }

  @Test
  public void testGetFileInfoWithInvalidationDisabled() throws Exception
  {
    Path backendFilePath = new Path(TestUtil.getDefaultTestDirectoryPath(conf), BACKEND_FILE_NAME);
    DataGen.populateFile(backendFilePath.toString());
    int expectedFileSize = DataGen.generateContent(1).length();

    CacheConfig.setFileStalenessCheck(conf, false);

    FileInfo info = bookKeeper.getFileInfo(backendFilePath.toString());

    assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
        " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);

    //Rewrite the file with half the data
    DataGen.populateFile(backendFilePath.toString(), 2);

    info = bookKeeper.getFileInfo(backendFilePath.toString());
    assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
        " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);
  }

  @Test
  public void testGetFileInfoWithInvalidationDisabledWithCacheExpired() throws Exception
  {
    Path backendFilePath = new Path(TestUtil.getDefaultTestDirectoryPath(conf), BACKEND_FILE_NAME);
    DataGen.populateFile(backendFilePath.toString());
    int expectedFileSize = DataGen.generateContent(1).length();

    CacheConfig.setFileStalenessCheck(conf, false);
    CacheConfig.setStaleFileInfoExpiryPeriod(conf, 5);

    FakeTicker ticker = new FakeTicker();

    // Close metrics created in setUp(); we want a new one with the above configuration.
    bookKeeperMetrics.close();
    try (BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, new MetricRegistry())) {
      bookKeeper = new CoordinatorBookKeeper(conf, bookKeeperMetrics, ticker);
      FileInfo info = bookKeeper.getFileInfo(backendFilePath.toString());

      assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
          " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);

      //Rewrite the file with half the data
      DataGen.populateFile(backendFilePath.toString(), 2);

      info = bookKeeper.getFileInfo(backendFilePath.toString());
      assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
          " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);

      // Advance the ticker to 5 sec
      ticker.advance(5, TimeUnit.SECONDS);

      expectedFileSize = DataGen.generateContent(2).length();
      info = bookKeeper.getFileInfo(backendFilePath.toString());
      assertTrue(info.getFileSize() == expectedFileSize, "FileSize was not equal to the expected value." +
          " Got FileSize: " + info.getFileSize() + " Expected Value : " + expectedFileSize);
    }
  }

  /**
   * Verify that the metric representing total requests is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyTotalRequestMetricIsReported() throws TException
  {
    final long totalRequests = TEST_END_BLOCK - TEST_START_BLOCK;

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.TOTAL_REQUEST_COUNT.getMetricName()).getCount(), 0);

    CacheStatusRequest request = new CacheStatusRequest(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK)
            .setClusterType(ClusterType.TEST_CLUSTER_MANAGER.ordinal())
            .setIncrMetrics(true);

    bookKeeper.getCacheStatus(request);

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.TOTAL_REQUEST_COUNT.getMetricName()).getCount(), totalRequests);
  }

  @Test
  public void verifyTotalRequestMetricIsNotReportedWhenMetricsAreNotIncremented() throws TException
  {
    final long totalRequests = TEST_END_BLOCK - TEST_START_BLOCK;

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.TOTAL_REQUEST_COUNT.getMetricName()).getCount(), 0);

    CacheStatusRequest request = new CacheStatusRequest(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK).setClusterType(ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    bookKeeper.getCacheStatus(request);

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.TOTAL_REQUEST_COUNT.getMetricName()).getCount(), 0);
  }

  /**
   * Verify that the metric representing total remote requests is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyRemoteRequestMetricIsReported() throws TException
  {
    final long totalRequests = TEST_END_BLOCK - TEST_START_BLOCK;

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.REMOTE_REQUEST_COUNT.getMetricName()).getCount(), 0);

    CacheStatusRequest request = new CacheStatusRequest(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK)
            .setClusterType(ClusterType.TEST_CLUSTER_MANAGER.ordinal())
            .setIncrMetrics(true);

    bookKeeper.getCacheStatus(request);

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.REMOTE_REQUEST_COUNT.getMetricName()).getCount(), totalRequests);
  }

  /**
   * Verify that the metric representing total local requests is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyLocalRequestMetricIsReported() throws TException
  {
    final long totalRequests = TEST_END_BLOCK - TEST_START_BLOCK;

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.CACHE_REQUEST_COUNT.getMetricName()).getCount(), 0);

    CacheStatusRequest request = new CacheStatusRequest(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK)
            .setClusterType(ClusterType.TEST_CLUSTER_MANAGER.ordinal())
            .setIncrMetrics(true);

    CacheStatusResponse response = bookKeeper.getCacheStatus(request);
    bookKeeper.setAllCached(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK, response.getGenerationNumber());
    bookKeeper.getCacheStatus(request);

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.CACHE_REQUEST_COUNT.getMetricName()).getCount(), totalRequests);
  }

  /**
   * Verify that the metric representing total non-local requests is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyNonlocalRequestMetricIsReported() throws TException
  {
    final long totalRequests = TEST_END_BLOCK - TEST_START_BLOCK;

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.NONLOCAL_REQUEST_COUNT.getMetricName()).getCount(), 0);
    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.CACHE_REQUEST_COUNT.getMetricName()).getCount(), 0);

    CacheStatusRequest request = new CacheStatusRequest(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK)
            .setClusterType(ClusterType.TEST_CLUSTER_MANAGER_MULTINODE.ordinal())
            .setIncrMetrics(true);

    bookKeeper.getCacheStatus(request);

    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.NONLOCAL_REQUEST_COUNT.getMetricName()).getCount(), totalRequests);
    assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.CACHE_REQUEST_COUNT.getMetricName()).getCount(), 0);
  }

  /**
   * Verify that the metric representing the current cache size is correctly registered & reports expected values.
   *
   * @throws IOException if an I/O error occurs when interacting with the cache.
   */
  @Test
  public void verifyCacheSizeMetricIsReported() throws IOException, TException
  {
    final String remotePathWithScheme = "file://" + TEST_REMOTE_PATH;
    final int readOffset = 0;
    final int readLength = 100;

    // Since the value returned from a gauge metric is an object rather than a primitive, boxing is required here to properly compare the values.
    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.CACHE_SIZE_GAUGE.getMetricName()).getValue(), 0);

    DataGen.populateFile(TEST_REMOTE_PATH);
    ReadResponse response = bookKeeper.readData(remotePathWithScheme, readOffset, readLength, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, ClusterType.TEST_CLUSTER_MANAGER.ordinal());

    final long mdSize = FileUtils.sizeOf(new File(CacheUtil.getMetadataFilePath(TEST_REMOTE_PATH, conf, response.getGenerationNumber())));
    final int totalCacheSize = (int) DiskUtils.bytesToMB(readLength + mdSize);
    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.CACHE_SIZE_GAUGE.getMetricName()).getValue(), totalCacheSize);
  }

  /**
   * Verify that the metric representing total cache evictions is correctly registered & incremented.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   * @throws FileNotFoundException when cache directories cannot be created.
   */
  @Test
  public void verifyCacheExpiryMetricIsReported() throws TException, IOException
  {
    final FakeTicker ticker = new FakeTicker();
    CacheConfig.setCacheDataExpirationAfterWrite(conf, 1000);
    metrics = new MetricRegistry();

    // Close metrics created in setUp(); we want a new one with the above configuration.
    bookKeeperMetrics.close();
    try (BookKeeperMetrics bookKeeperMetrics = new BookKeeperMetrics(conf, metrics)) {
      bookKeeper = new CoordinatorBookKeeper(conf, bookKeeperMetrics, ticker);

      assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.CACHE_EXPIRY_COUNT.getMetricName()).getCount(), 0);

      CacheStatusRequest request = new CacheStatusRequest(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
          TEST_START_BLOCK, TEST_END_BLOCK)
              .setClusterType(ClusterType.TEST_CLUSTER_MANAGER.ordinal())
              .setIncrMetrics(true);
      bookKeeper.getCacheStatus(request);

      ticker.advance(30000, TimeUnit.MILLISECONDS);
      bookKeeper.fileMetadataCache.cleanUp();

      assertEquals(metrics.getCounters().get(BookKeeperMetrics.CacheMetric.CACHE_EXPIRY_COUNT.getMetricName()).getCount(), 1);
    }
  }

  /**
   * Verify that the metrics representing cache hits & misses are correctly registered and report expected values.
   *
   * @throws TException when file metadata cannot be fetched or refreshed.
   */
  @Test
  public void verifyCacheHitAndMissMetricsAreReported() throws TException
  {
    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.CACHE_HIT_RATE_GAUGE.getMetricName()).getValue(), Double.NaN);
    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.CACHE_MISS_RATE_GAUGE.getMetricName()).getValue(), Double.NaN);

    CacheStatusRequest request = new CacheStatusRequest(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        TEST_START_BLOCK, TEST_END_BLOCK)
            .setClusterType(ClusterType.TEST_CLUSTER_MANAGER.ordinal())
            .setIncrMetrics(true);
    CacheStatusResponse response = bookKeeper.getCacheStatus(request);

    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.CACHE_HIT_RATE_GAUGE.getMetricName()).getValue(), 0.0);
    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.CACHE_MISS_RATE_GAUGE.getMetricName()).getValue(), 1.0);

    bookKeeper.setAllCached(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK, response.getGenerationNumber());
    bookKeeper.getCacheStatus(request);

    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.CACHE_HIT_RATE_GAUGE.getMetricName()).getValue(), 0.5);
    assertEquals(metrics.getGauges().get(BookKeeperMetrics.CacheMetric.CACHE_MISS_RATE_GAUGE.getMetricName()).getValue(), 0.5);
  }

  /**
   * Test that calling setAllCached with blocks which are in cache does not increase file size
   */
  @Test
  public void testSetAllCacheIngoresCachedBlocks() throws Exception
  {
    CacheStatusRequest request = new CacheStatusRequest(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
            TEST_START_BLOCK, TEST_END_BLOCK)
            .setClusterType(ClusterType.TEST_CLUSTER_MANAGER.ordinal())
            .setIncrMetrics(true);
    CacheStatusResponse response = bookKeeper.getCacheStatus(request);
    int genNumber = response.getGenerationNumber();

    long size = bookKeeper.getFileMetadata(TEST_REMOTE_PATH).getCurrentFileSize();
    assertTrue(size == 0, "Non zero size before any data cached: " + size);
    bookKeeper.setAllCached(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK, TEST_END_BLOCK, genNumber);
    size = bookKeeper.getFileMetadata(TEST_REMOTE_PATH).getCurrentFileSize();
    assertTrue(size == (TEST_END_BLOCK - TEST_START_BLOCK) * CacheConfig.getBlockSize(conf),
            String.format("Expected size: %s but found %s", (TEST_END_BLOCK - TEST_START_BLOCK) * CacheConfig.getBlockSize(conf), size));

    // Now a overlapping request of 10 new blocks, new size of FileMetadata should only increase for these 10 new blocks
    bookKeeper.setAllCached(TEST_REMOTE_PATH, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, TEST_START_BLOCK - 5, TEST_END_BLOCK + 5, genNumber);
    long newSize = bookKeeper.getFileMetadata(TEST_REMOTE_PATH).getCurrentFileSize();
    assertTrue(newSize == size + 10 * CacheConfig.getBlockSize(conf),
            String.format("Expected size: %s but found %s", (size + 10) * CacheConfig.getBlockSize(conf), newSize));
  }

  @Test
  void testCachedReadWithInvalidation() throws IOException, TException, InterruptedException {
    final String remotePathWithScheme = "file://" + TEST_REMOTE_PATH;
    final int readOffset = 0;
    final int readLength = 2000;
    DataGen.populateFile(TEST_REMOTE_PATH);
    CacheStatusRequest request = new CacheStatusRequest(remotePathWithScheme, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
            0, 20).setClusterType(ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    bookKeeper.readData(remotePathWithScheme, readOffset, readLength, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    List<BlockLocation> blockLocations = bookKeeper.getCacheStatus(request).getBlocks();
    for (BlockLocation location : blockLocations) {
      if (location.getLocation() != Location.CACHED) {
        fail();
      }
    }
    Thread invalidateRequest = new Thread(new Runnable() {
      @Override
      public void run() {
        bookKeeper.invalidateFileMetadata(remotePathWithScheme);
      }
    });
    Thread readrequest = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          List<BlockLocation> blockLocations = bookKeeper.getCacheStatus(request).getBlocks();
          for (BlockLocation location : blockLocations) {
            // once invalidated, should never be cached
            if (location.getLocation() == Location.CACHED) {
              byte[] buffer = new byte[1000];
              CachedReadRequestChain cachedReadRequestChain = getCachedReadRequestChain(buffer);
              cachedReadRequestChain.lock();
              int readSize = Math.toIntExact(cachedReadRequestChain.call());
              // it will read correct data if it does not find the file (from remote) or has opened the fd
              // next read request will generate local file with new generation number for the corresponding remote path
              assertEquals(1000, readSize);
            }
          }
        }
        catch (Exception e)
        {
          log.info("failed with exception ", e);
          fail();
        }
      }
    });
    invalidateRequest.start();
    readrequest.start();
    invalidateRequest.join();
    readrequest.join();
  }

  private CachedReadRequestChain getCachedReadRequestChain(byte[] buffer) throws IOException
  {
    MockCachingFileSystem fs = new MockCachingFileSystem();
    final String remotePathWithScheme = "file://" + TEST_REMOTE_PATH;
    Path backendFilePath = new Path(remotePathWithScheme);
    fs.initialize(backendFilePath.toUri(), conf);
    ReadRequest[] readRequests = getReadRequests(buffer);
    BookKeeperFactory factory;
    factory = new BookKeeperFactory();
    CachedReadRequestChain cachedReadRequestChain = new CachedReadRequestChain(fs, remotePathWithScheme, conf, factory, 0);
    for (ReadRequest rr : readRequests) {
      cachedReadRequestChain.addReadRequest(rr);
    }
    return cachedReadRequestChain;
  }

  private ReadRequest[] getReadRequests(byte[] buffer)
  {
    int fileSize = 2000;
    return new ReadRequest[]{
            new ReadRequest(0, 100, 0, 100, buffer, 0, fileSize),
            new ReadRequest(200, 300, 200, 300, buffer, 100, fileSize),
            new ReadRequest(400, 500, 400, 500, buffer, 200, fileSize),
            new ReadRequest(600, 700, 600, 700, buffer, 300, fileSize),
            new ReadRequest(800, 900, 800, 900, buffer, 400, fileSize),
            new ReadRequest(1000, 1100, 1000, 1100, buffer, 500, fileSize),
            new ReadRequest(1200, 1300, 1200, 1300, buffer, 600, fileSize),
            new ReadRequest(1400, 1500, 1400, 1500, buffer, 700, fileSize),
            new ReadRequest(1600, 1700, 1600, 1700, buffer, 800, fileSize),
            new ReadRequest(1800, 1900, 1800, 1900, buffer, 900, fileSize),
    };
  }

  @Test
  void testgenerationNumber() throws TException, IOException {
    System.out.println("running test testgenerationNumber ");
    final String remotePathWithScheme = "file://" + TEST_REMOTE_PATH;
    final int readOffset = 0;
    final int readLength = 2000;
    DataGen.populateFile(TEST_REMOTE_PATH);
    CacheStatusRequest request = new CacheStatusRequest(remotePathWithScheme, TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
            0, 20).setClusterType(ClusterType.TEST_CLUSTER_MANAGER.ordinal());
     bookKeeper.readData(remotePathWithScheme, readOffset, readLength, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
     bookKeeper.invalidateFileMetadata(remotePathWithScheme);
     assertEquals( bookKeeper.getCacheStatus(request).getGenerationNumber(), 1);
  }
}
