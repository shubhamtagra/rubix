/**
 * Copyright (c) 2016. Qubole Inc
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
package com.qubole.rubix.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class ReadRequestPool
{
    final Queue<WeakReference<ReadRequest>> queue = new ConcurrentLinkedQueue();

    private AtomicLong allocations = new AtomicLong(0);
    private AtomicLong freed = new AtomicLong(0);
    private AtomicLong reused = new AtomicLong(0);

    private static final Log log = LogFactory.getLog(ReadRequestPool.class);

    public ReadRequestPool()
    {
    }

    public ReadRequest getReadRequest(long backendReadStart, long backendReadEnd, long actualReadStart, long actualReadEnd, byte[] destBuffer, int destBufferOffset, long backendFileSize)
    {
            ReadRequest readRequest;
            do {
                WeakReference ref = (WeakReference) queue.poll();
                if (ref == null) {
                    return allocate(backendReadStart, backendReadEnd, actualReadStart, actualReadEnd, destBuffer, destBufferOffset, backendFileSize);
                }

                readRequest = (ReadRequest) ref.get();
            }
            while (readRequest == null);

            logData(allocations.get(), freed.get(), reused.incrementAndGet());
            readRequest.setInfo(backendReadStart, backendReadEnd, actualReadStart, actualReadEnd, destBuffer, destBufferOffset, backendFileSize);
            return readRequest;
    }

    public void returnBuffer(List<WeakReference<ReadRequest>> readRequests)
    {
        logData(allocations.get(), freed.addAndGet(readRequests.size()), reused.get());
        queue.addAll(readRequests);

    }

    private ReadRequest allocate(long backendReadStart, long backendReadEnd, long actualReadStart, long actualReadEnd, byte[] destBuffer, int destBufferOffset, long backendFileSize)
    {
        logData(allocations.incrementAndGet(), freed.get(), reused.get());
        return new ReadRequest(backendReadStart, backendReadEnd, actualReadStart, actualReadEnd, destBuffer, destBufferOffset, backendFileSize);
    }

    private void logData(long allocation, long freed, long reused)
    {
        log.info(String.format("allocations=%d freed=%d reused=%d", allocation, freed, reused));
    }
}
