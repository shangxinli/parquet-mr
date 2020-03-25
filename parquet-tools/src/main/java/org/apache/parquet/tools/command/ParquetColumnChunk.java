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
package org.apache.parquet.tools.command;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.column.page.Page;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Get ride of this class by looking at the ParquetFileReader.java
 */
public class ParquetColumnChunk
  extends ByteArrayInputStream
{

  public ParquetColumnChunk(
    byte[] data,
    int offset)
  {
    super(data);
    this.pos = offset;
  }


  protected PageHeader readPageHeader()
    throws IOException
  {
    return Util.readPageHeader(this);
  }

  public List<PageWithHeader> readAllPagesInOrder(long totalValue)
    throws IOException
  {
    List<PageWithHeader> pagesWithHeader = new ArrayList<>();
    DictionaryPage dictionaryPage = null;
    long valueCount = 0;
    while (valueCount < totalValue) {
      PageHeader pageHeader = readPageHeader();
      int uncompressedPageSize = pageHeader.getUncompressed_page_size();
      int compressedPageSize = pageHeader.getCompressed_page_size();

      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          if (dictionaryPage != null) {
            throw new IOException(  "has more than one dictionary page in column chunk");
          }
          pagesWithHeader.add(new PageWithHeader(pageHeader, getSlice(compressedPageSize).getBytes()));
          break;
        case DATA_PAGE:
          pagesWithHeader.add(new PageWithHeader(pageHeader, getSlice(compressedPageSize).getBytes()));
          valueCount += pageHeader.getData_page_header().getNum_values();
          break;
        case DATA_PAGE_V2:
          pagesWithHeader.add(new PageWithHeader(pageHeader, getSlice(compressedPageSize).getBytes()));
          valueCount += pageHeader.getData_page_header_v2().getNum_values();
          break;
        default:
          skip(compressedPageSize);
          break;
      }
    }
    return pagesWithHeader;
  }

  private Slice getSlice(int size)
  {
    Slice slice = Slices.wrappedBuffer(buf, pos, size);
    pos += size;
    return slice;
  }

}
