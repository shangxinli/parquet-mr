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

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.Page;
import org.apache.parquet.format.DataPageHeader;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.DictionaryPageHeader;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import io.airlift.slice.Slice;

public class PageWithHeader
{
  private PageHeader header;
  private boolean compressed;
  private byte[] pageLoad;

  public PageWithHeader(PageHeader header, byte[] pageLoad) {
    this.header = header;
    this.pageLoad = pageLoad;
    this.compressed = setCompressed(header);
  }

  public PageHeader getHeader() {
    return header;
  }

  public byte[] getPageLoad() {
    return pageLoad;
  }

  public boolean isCompressed() {
    return compressed;
  }

  private boolean setCompressed(PageHeader header) {
    //TODO: This is a hack
    return header.uncompressed_page_size != header.compressed_page_size;
  }
}
