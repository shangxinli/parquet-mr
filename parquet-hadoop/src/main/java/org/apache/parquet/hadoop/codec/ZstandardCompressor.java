/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop.codec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.parquet.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;
import io.airlift.compress.zstd.ZstdCompressor;
import com.github.luben.zstd.Zstd;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This class is a wrapper around the snappy compressor. It always consumes the
 * entire input in setInput and compresses it as one compressed block.
 */
public class ZstandardCompressor implements Compressor {
  private static final Logger LOG = LoggerFactory.getLogger(ZstandardCompressor.class);

  // Buffer for compressed output. This buffer grows as necessary.
  private ByteBuffer outputBuffer = ByteBuffer.allocate(0);

  // Buffer for uncompressed input. This buffer grows as necessary.
  private ByteBuffer inputBuffer = ByteBuffer.allocate(0);

  private long bytesRead = 0L;
  private long bytesWritten = 0L;
  private boolean finishCalled = false;
  private int level = 3;
  //
  private Zstd compressor = new Zstd();
  //private io.airlift.compress.Compressor compressor = new io.airlift.compress.zstd.ZstdCompressor();

  public ZstandardCompressor(int level) {
    this.level = level;
  }

  /**
   * Fills specified buffer with compressed data. Returns actual number
   * of bytes of compressed data. A return value of 0 indicates that
   * needsInput() should be called in order to determine if more input
   * data is required.
   *
   * @param buffer   Buffer for the compressed data
   * @param off Start offset of the data
   * @param len Size of the buffer
   * @return The actual number of bytes of compressed data.
   */
  @Override
  public synchronized int compress(byte[] buffer, int off, int len) throws IOException {
    SnappyUtil.validateBuffer(buffer, off, len);

    if (needsInput()) {
      // No buffered output bytes and no input to consume, need more input
      return 0;
    }

    if (!outputBuffer.hasRemaining()) {
      // There is uncompressed input, compress it now
      int maxOutputSize = inputBuffer.position();
        Snappy.maxCompressedLength(inputBuffer.position());
      if (maxOutputSize > outputBuffer.capacity()) {
        outputBuffer = ByteBuffer.allocate(maxOutputSize);
      }
      // Reset the previous outputBuffer
      outputBuffer.clear();
      inputBuffer.limit(inputBuffer.position());
      inputBuffer.position(0);

      byte[] outputBytes = outputBuffer.array();
      byte[] inputBytes = inputBuffer.array();
      LOG.info("zstd level is " + level);
      long size = compressor.compressByteArray(outputBytes, 0, outputBuffer.capacity(), inputBytes, 0, inputBuffer.limit(), level);
      outputBuffer.limit((int)size);
      inputBuffer.limit(0);
      inputBuffer.rewind();
    }

    // Return compressed output up to 'len'
    int numBytes = Math.min(len, outputBuffer.remaining());
    outputBuffer.get(buffer, off, numBytes);    
    bytesWritten += numBytes;
    return numBytes;	    
  }

  @Override
  public synchronized void setInput(byte[] buffer, int off, int len) {  
    SnappyUtil.validateBuffer(buffer, off, len);
    
    Preconditions.checkArgument(!outputBuffer.hasRemaining(), 
        "Output buffer should be empty. Caller must call compress()");

    if (inputBuffer.capacity() - inputBuffer.position() < len) {
      ByteBuffer tmp = ByteBuffer.allocate(inputBuffer.position() + len);
      inputBuffer.rewind();
      tmp.put(inputBuffer);
      inputBuffer = tmp;
    } else {
      inputBuffer.limit(inputBuffer.position() + len);
    }

    // Append the current bytes to the input buffer
    inputBuffer.put(buffer, off, len);
    bytesRead += len;
  }

  @Override
  public void end() {
    // No-op		
  }

  @Override
  public synchronized void finish() {
    finishCalled = true;
  }

  @Override
  public synchronized boolean finished() {
    return finishCalled && inputBuffer.position() == 0 && !outputBuffer.hasRemaining();
  }

  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  @Override
  // We want to compress all the input in one go so we always need input until it is
  // all consumed.
  public synchronized boolean needsInput() {
    return !finishCalled;
  }

  @Override
  public void reinit(Configuration c) {
    reset();		
  }

  @Override
  public synchronized void reset() {
    finishCalled = false;
    bytesRead = bytesWritten = 0;
    inputBuffer.rewind();
    outputBuffer.rewind();
    inputBuffer.limit(0);
    outputBuffer.limit(0);
  }

  @Override
  public void setDictionary(byte[] dictionary, int off, int len) {
    // No-op		
  }
}
