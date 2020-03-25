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

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.parquet.tools.mask.Mask;

import java.io.DataInput;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.format.Util.writePageHeader;

public class TransCompressionCommand extends ArgsOnlyCommand {
  private static final Statistics<?> EMPTY_STATS = Statistics
      .getBuilderForReading(Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("test_binary")).build();

  public static final String[] USAGE = new String[] {
    "<input> <output> [<column, mask_method> ...]",

    "where <input> is the source parquet file",
    "    <output> is the destination parquet file," +
      "    [<column, mask_method> ...] are the columns in the case sensitive dot format and mask method sha256, md5 or null" +
      " for example 'file1, file2 col_a.b.c sha256 col_c.d null' is explained as 'mask column col_a.b.c with hash" +
      " sha256 values and mask column_c.d with null values in file1. Output file is saved as file2"
  };

  private static final int MAX_COL_NUM = 100;

  private ParquetFileWriter writer = null;
  private ParquetFileReader reader = null;
  protected CompressionCodecFactory codecFactory = HadoopCodecs.newFactory(0);


  public TransCompressionCommand() {
    super(4, 2 * MAX_COL_NUM + 2);
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public String getCommandDescription() {
    return "Mask one ore more columns with defined mask methods";
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);

    // TODO: Valid arguments
    List<String> args = options.getArgList();
    Configuration conf = new Configuration();
    Path inpath = new Path(args.get(0));
    Path outpath = new Path(args.get(1));

    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inpath, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();
    HadoopInputFile inputFile = HadoopInputFile.fromPath(inpath, conf);
    ParquetReadOptions readOptions = HadoopReadOptions.builder(conf).build();
    reader = new ParquetFileReader(inputFile, readOptions);
    writer = new ParquetFileWriter(conf, schema, outpath, ParquetFileWriter.Mode.CREATE);
    writer.start();
    processBlocks(metaData, schema);
    writer.end(metaData.getFileMetaData().getKeyValueMetaData());
    reader.close();
  }

  public void processBlocks(final ParquetMetadata meta, final MessageType schema) throws IOException {
    int blockIndex = 0;
    PageReadStore store = reader.readNextRowGroup();
    while (store != null) {
      writer.startBlock(store.getRowCount());

      BlockMetaData blockMetaData = meta.getBlocks().get(blockIndex);
      List<ColumnChunkMetaData> columnsInOrder = blockMetaData.getColumns();
      Map<ColumnPath, ColumnDescriptor> descriptorsMap = schema.getColumns().stream().collect(
        Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));

      ParquetFileWriter.StreamPara para = new ParquetFileWriter.StreamPara(-1, 0, 0);
      for (int i = 0; i < columnsInOrder.size(); i += 1) {
        ColumnChunkMetaData chunk = columnsInOrder.get(i);
        para.setSize(para.getSize() + chunk.getTotalUncompressedSize());

        ColumnReadStoreImpl crstore = new ColumnReadStoreImpl(store, new DumpGroupConverter(), schema, meta.getFileMetaData().getCreatedBy());
        ColumnDescriptor columnDescriptor = descriptorsMap.get(chunk.getPath());
        //TODO: creader already trigger decompress
        ColumnReader creader = crstore.getColumnReader(columnDescriptor);
        long totalValues = creader.getTotalValueCount();
        List<PageWithHeader> pagesWithHeader = getPagesWithHeader(chunk);
        //TODO: We hardcoded it
        CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
        List<PageWithHeader> transPagesWithHeader = transCompression(pagesWithHeader, chunk.getCodec(), compressionCodecName);

        writer.startColumn(columnDescriptor, totalValues, compressionCodecName);
        writeColumn(transPagesWithHeader);
        writer.endColumn();
      }

      writer.setCurrentBlockSize(para.getSize());
      writer.endBlock();

      store = reader.readNextRowGroup();
      blockIndex++;
    }
  }

  private List<PageWithHeader> getPagesWithHeader(ColumnChunkMetaData chunk)  throws IOException {
    SeekableInputStream f = reader.getInputStream();
    // TODO Cast loss
    byte[] columnBuffer = new byte[(int) chunk.getTotalSize()];
    f.seek(chunk.getStartingPos());
    f.readFully(columnBuffer);
    ParquetColumnChunk parquetColumnChunk = new ParquetColumnChunk(columnBuffer, 0);
    return parquetColumnChunk.readAllPagesInOrder(chunk.getValueCount());
  }

  private List<PageWithHeader> transCompression(List<PageWithHeader> pagesWithHeader, CompressionCodecName deCodecName, CompressionCodecName enCodecName) throws IOException {
    List<PageWithHeader> transPagesWithHeader = new ArrayList<>();
    CompressionCodecFactory.BytesInputDecompressor decompressor = codecFactory.getDecompressor(deCodecName);
    CompressionCodecFactory.BytesInputCompressor compressor = codecFactory.getCompressor(enCodecName);
    for (PageWithHeader page : pagesWithHeader) {
      byte[] data = page.getPageLoad();
      if (page.isCompressed()) {
        BytesInput input = BytesInput.from(data);
        int uncompressedSize = page.getHeader().getUncompressed_page_size();
        BytesInput rawData = decompressor.decompress(input, uncompressedSize);
        data = rawData.toByteArray();
      }
      BytesInput bytesInput = BytesInput.from(data);
      BytesInput newCompressedData = compressor.compress(bytesInput);
      transPagesWithHeader.add(new PageWithHeader(page.getHeader(), newCompressedData.toByteArray()));
    }
    return transPagesWithHeader;
  }

  private void writeColumn(List<PageWithHeader> pagesWithHeader) throws IOException {
    for (PageWithHeader page : pagesWithHeader) {
      int valueCount = 0;
      long rowCount = 0;
      Statistics statistics = EMPTY_STATS;
      //TODO: replace with converter.getEncoding
      Encoding encoding = null;
      switch (page.getHeader().type) {
        case DICTIONARY_PAGE:
          String codeName = page.getHeader().dictionary_page_header.getEncoding().name();
          encoding =  org.apache.parquet.column.Encoding.valueOf(codeName);
          writer.writeDictionaryPage(new DictionaryPage(BytesInput.from(page.getPageLoad()), page.getHeader().getCompressed_page_size(), encoding));
          break;
        case DATA_PAGE:
          codeName = page.getHeader().data_page_header.getEncoding().name();
          encoding =  org.apache.parquet.column.Encoding.valueOf(codeName);
          valueCount = page.getHeader().getData_page_header().getNum_values();
          //statistics = page.getHeader().getData_page_header().getStatistics();
          //TODO: WRONG WRONG
          rowCount = valueCount;
          writer.writeDataPage(valueCount, page.getHeader().uncompressed_page_size, BytesInput.from(page.getPageLoad()),
            statistics, rowCount, encoding, encoding, encoding);
          break;
        case DATA_PAGE_V2:
          codeName = page.getHeader().data_page_header_v2.getEncoding().name();
          encoding =  org.apache.parquet.column.Encoding.valueOf(codeName);
          valueCount = page.getHeader().getData_page_header_v2().getNum_values();
          //TODO:
          rowCount = valueCount;
          writer.writeDataPage(valueCount, page.getHeader().uncompressed_page_size, BytesInput.from(page.getPageLoad()),
            statistics, rowCount, encoding, encoding, encoding);
          break;
        default:
          throw new RuntimeException("blalla");
      }
    }
  }

  private static final class DumpGroupConverter extends GroupConverter {
      @Override public void start() { }
      @Override public void end() { }
      @Override public Converter getConverter(int fieldIndex) { return new DumpConverter(); }
  }

  private static final class DumpConverter extends PrimitiveConverter {
      @Override public GroupConverter asGroupConverter() { return new DumpGroupConverter(); }
  }
}
