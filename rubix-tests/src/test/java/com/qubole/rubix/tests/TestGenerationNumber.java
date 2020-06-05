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
package com.qubole.rubix.tests;

import com.codahale.metrics.MetricRegistry;
import com.qubole.rubix.bookkeeper.BookKeeperServer;
import com.qubole.rubix.common.utils.DataGen;
import com.qubole.rubix.common.utils.DeleteFileVisitor;
import com.qubole.rubix.core.CachingFileSystemStats;
import com.qubole.rubix.core.CachingInputStream;
import com.qubole.rubix.spi.BookKeeperFactory;
import com.qubole.rubix.spi.CacheConfig;
import com.qubole.rubix.spi.CacheUtil;
import com.qubole.rubix.spi.ClusterType;
import com.qubole.rubix.spi.RetryingPooledBookkeeperClient;
import com.qubole.rubix.spi.thrift.BlockLocation;
import com.qubole.rubix.spi.thrift.CacheStatusRequest;
import com.qubole.rubix.spi.thrift.CacheStatusResponse;
import com.qubole.rubix.spi.thrift.FileInfo;
import com.qubole.rubix.spi.thrift.Location;
import com.qubole.rubix.spi.thrift.ReadResponse;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.thrift.shaded.TException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.testng.Assert.assertEquals;

import static com.qubole.rubix.spi.CacheUtil.UNKONWN_GENERATION_NUMBER;
import static org.testng.Assert.assertTrue;

public class TestGenerationNumber
{
  int blockSize = 100;
  String backendFileName = testDirectoryPrefix + "backendFile";
  Path backendPath = new Path("file:///" + backendFileName.substring(1));

  private BookKeeperServer bookKeeperServer;
  private BookKeeperFactory bookKeeperFactory;

  CachingInputStream inputStream;
  final Configuration conf = new Configuration();

  private static final String testDirectoryPrefix = System.getProperty("java.io.tmpdir") + "/tmp/testPath";
  private static final long TEST_LAST_MODIFIED = 1514764800; // 2018-01-01T00:00:00
  private static final long TEST_FILE_LENGTH = 5000;
  private static final String testDirectory = testDirectoryPrefix + "dir0";

  private static final Log log = LogFactory.getLog(TestGenerationNumber.class);

  @BeforeClass
  public static void setupClass() throws IOException
  {
    log.info(testDirectory);
    Files.createDirectories(Paths.get(testDirectory));
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    Files.walkFileTree(Paths.get(testDirectory), new DeleteFileVisitor());
    Files.deleteIfExists(Paths.get(testDirectory));
  }

  @BeforeMethod
  public void setup() throws Exception
  {
    CacheConfig.setOnMaster(conf, true);
    CacheConfig.setIsStrictMode(conf, true);
    CacheConfig.setBookKeeperServerPort(conf, 3456);
    CacheConfig.setDataTransferServerPort(conf, 2222);
    CacheConfig.setBlockSize(conf, blockSize);
    CacheConfig.setCacheDataDirPrefix(conf, testDirectoryPrefix + "dir");
    CacheConfig.setMaxDisks(conf, 1);
    CacheConfig.setIsParallelWarmupEnabled(conf, false);
    CacheConfig.setCleanupFilesDuringStart(conf, false);
    bookKeeperFactory = new BookKeeperFactory();
    startServer();
    // Populate File
    DataGen.populateFile(backendFileName);
    inputStream = createCachingStream(conf);
    log.info("BackendPath: " + backendPath);
  }

  private CachingInputStream createCachingStream(Configuration conf)
      throws IOException
  {
    FileSystem localFileSystem = new RawLocalFileSystem();
    Path backendFilePath = new Path(backendFileName);
    localFileSystem.initialize(backendFilePath.toUri(), new Configuration());
    CacheConfig.setBlockSize(conf, blockSize);

    // This should be after server comes up else client could not be created
    return new CachingInputStream(backendPath, conf,
        new CachingFileSystemStats(), ClusterType.TEST_CLUSTER_MANAGER,
        new BookKeeperFactory(), localFileSystem,
        CacheConfig.getBlockSize(conf), null);
  }

  @Test
  void testReadWithInvalidations() throws Exception {
    testReadWithInvalidationHelper();
    CacheConfig.setIsParallelWarmupEnabled(conf, true);
    restartServer();
    testReadWithInvalidationHelper();
  }

  void testReadWithInvalidationHelper() throws TException, InterruptedException {
    final int readOffset = 0, readLength = 2000;
    RetryingPooledBookkeeperClient client = getBookKeeperClient();
    Thread invalidateRequest = new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 1; i < 15; i++)
        {
          try {
            client.invalidateFileMetadata(backendPath.toString());
            Thread.sleep(1000);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    });
    Thread readrequest = new Thread(new Runnable() {
      @Override
      public void run() {
        for (int i = 1; i < 200; i++)
        {
          try {
            byte[] buffer = new byte[readLength];
            inputStream.seek(0);
            int readSize = inputStream.read(buffer, 0, readLength);
            assertTrue(readSize == readLength, "Wrong amount of data read " + readSize + " was expecting " + readLength);
            Thread.sleep(100);
          }
          catch (Exception e)
          {
            throw new RuntimeException(e);
          }
        }
      }
    });
    invalidateRequest.start();
    readrequest.start();
    readrequest.join();
    invalidateRequest.join();
  }

  @Test
  void testGenerationNumberIncrement() throws TException, IOException
  {
    RetryingPooledBookkeeperClient client = getBookKeeperClient();
    final int readOffset = 0, readLength = 2000;
    client.readData(backendPath.toString(), readOffset, readLength, TEST_FILE_LENGTH, TEST_LAST_MODIFIED, ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    client.invalidateFileMetadata(backendPath.toString());
    CacheStatusRequest request = new CacheStatusRequest(backendPath.toString(), TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        0, 20).setClusterType(ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    int generationNumber = client.getCacheStatus(request).getGenerationNumber();
    assertTrue(generationNumber == UNKONWN_GENERATION_NUMBER + 2,
        "generation number is reported: " + generationNumber +
        "but expected: " + UNKONWN_GENERATION_NUMBER + 2);
  }

  @Test
  void testGenerationNumberDuringStartUp() throws Exception
  {
    int generationNumber = 5;
    // create files till generation Number = 5
    creatLocalFilesOnCache(generationNumber);
    boolean missingFile = false;
    testGenerationNumberDuringStartUpHelper(generationNumber, missingFile);
    missingFile = true;
    restartServer();
    creatLocalFilesOnCache(generationNumber);
    new File(CacheUtil.getMetadataFilePath(backendPath.toString(), conf, generationNumber)).delete();
    testGenerationNumberDuringStartUpHelper(generationNumber, missingFile);
  }

  void testGenerationNumberDuringStartUpHelper(int generationNumber, boolean misssingFile) throws TException
  {
    RetryingPooledBookkeeperClient client = getBookKeeperClient();
    CacheStatusRequest request = new CacheStatusRequest(backendPath.toString(), TEST_FILE_LENGTH, TEST_LAST_MODIFIED,
        0, 20).setClusterType(ClusterType.TEST_CLUSTER_MANAGER.ordinal());
    CacheStatusResponse response = client.getCacheStatus(request);
    if (!misssingFile)
    {
      // Both datafile and mdfile exists with generationNumber on disk so we should get generationNumber in response
      assertTrue(response.getGenerationNumber() == generationNumber,
          "generation Number is reported:" + response.getGenerationNumber() +
              "but expected: " + generationNumber);
    }
    else
    {
      // datafile exists with generationNumber on disk but without mdFile so we should get generationNumber + 1 in response
      assertTrue(response.getGenerationNumber() == generationNumber + 1,
          "generation number is reported: " + response.getGenerationNumber() +
              " but expected: " + (generationNumber + 1) );
    }
  }

  void creatLocalFilesOnCache(int generationNumber) throws IOException {
    File mdFile = null, localFile = null;
    for(int genNumber = 1; genNumber <= generationNumber; genNumber++)
    {
      mdFile = new File(CacheUtil.getMetadataFilePath(backendPath.toString(), conf, genNumber));
      mdFile.createNewFile();
      localFile = new File(CacheUtil.getLocalPath(backendPath.toString(), conf, genNumber));
      localFile.createNewFile();
    }
  }

  RetryingPooledBookkeeperClient getBookKeeperClient() throws TException
  {
    RetryingPooledBookkeeperClient client = bookKeeperFactory.createBookKeeperClient("localhost", conf);
    return client;
  }

  @AfterMethod
  public void cleanup()
      throws IOException
  {
    stopServer();
    conf.clear();
  }

  private void stopServer()
  {
    if (bookKeeperServer != null) {
      bookKeeperServer.stopServer();
    }
  }

  private void startServer() throws Exception
  {
    bookKeeperServer = new BookKeeperServer();
    Thread thread = new Thread()
    {
      public void run()
      {
        bookKeeperServer.startServer(conf, new MetricRegistry());
      }
    };
    thread.start();
    while (!bookKeeperServer.isServerUp()) {
      Thread.sleep(200);
      log.info("Waiting for BookKeeper Server to come up");
    }
  }

  private void restartServer() throws Exception {
    stopServer();
    startServer();
  }
}
