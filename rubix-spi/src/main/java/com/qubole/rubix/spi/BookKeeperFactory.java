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
package com.qubole.rubix.spi;

import com.qubole.rubix.spi.thrift.BookKeeperService;
import org.apache.hadoop.conf.Configuration;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * Created by sakshia on 5/10/16.
 */
public class BookKeeperFactory
{
  BookKeeperService.Iface bookKeeper;

  public BookKeeperFactory()
  {
  }

  public BookKeeperFactory(BookKeeperService.Iface bookKeeper)
  {
    if (bookKeeper != null) {
      this.bookKeeper = bookKeeper;
    }
  }

  public RetryingBookkeeperClient createBookKeeperClient(String host, Configuration conf) throws TTransportException
  {
    if (bookKeeper != null) {
      return new LocalBookKeeperClient(null, bookKeeper);
    }
    else {
      final int socketTimeout = CacheConfig.getServerSocketTimeout(conf);
      final int connectTimeout = CacheConfig.getServerConnectTimeout(conf);

      TTransport transport = new TSocket(host, CacheConfig.getBookKeeperServerPort(conf), socketTimeout, connectTimeout);
      transport.open();
      RetryingBookkeeperClient retryingBookkeeperClient = new RetryingBookkeeperClient(transport, CacheConfig.getMaxRetries(conf));
      return retryingBookkeeperClient;
    }
  }

  public RetryingBookkeeperClient createBookKeeperClient(Configuration conf) throws TTransportException
  {
    return createBookKeeperClient("localhost", conf);
  }

  public boolean isBookKeeperInitialized()
  {
    return bookKeeper != null;
  }
}
