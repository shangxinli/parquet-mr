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
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputCompressor;
import org.apache.parquet.compression.CompressionCodecFactory.BytesInputDecompressor;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;

public class TransCompressionCommand extends ArgsOnlyCommand {

  public static final String[] USAGE = new String[] {
    "<input> <output> <codec_name>",

    "where <input> is the source parquet file",
    "    <output> is the destination parquet file," +
    "    <new_codec_name> is the codec name in the case sensitive format to be translated to, e.g. SNAPPY, GZIP, ZSTD, LZO, LZ4, BROTLI, UNCOMPRESSED"
  };

  private static final Logger LOG = LoggerFactory.getLogger(TransCompressionCommand.class);

  private Configuration conf;

  public TransCompressionCommand() {
    super(3, 3);
    this.conf = new Configuration();
  }

  public TransCompressionCommand(Configuration conf) {
    super(3, 3);
    this.conf = conf;
  }

  @Override
  public String[] getUsageDescription() {
    return USAGE;
  }

  @Override
  public String getCommandDescription() {
    return "Translate the compression of a given Parquet file to a new compression one to a new Parquet file.";
  }

  @Override
  public void execute(CommandLine options) throws Exception {
    super.execute(options);
    List<String> args = options.getArgList();
    Path inPath = new Path(args.get(0));
    Path outPath = new Path(args.get(1));
    CompressionCodecName codecName = CompressionCodecName.valueOf(args.get(2));

    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inPath, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();

    try (ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(inPath, conf), HadoopReadOptions.builder(conf).build())) {
      ParquetFileWriter writer = new ParquetFileWriter(conf, schema, outPath, ParquetFileWriter.Mode.CREATE);
      writer.start();
      processBlocks(reader, writer, metaData, schema, metaData.getFileMetaData().getCreatedBy(), codecName);
      writer.end(metaData.getFileMetaData().getKeyValueMetaData());
    }
  }

  private void processBlocks(ParquetFileReader reader, ParquetFileWriter writer, ParquetMetadata meta, MessageType schema,
                             String createdBy, CompressionCodecName codecName) throws IOException {
    int blockIndex = 0;
    PageReadStore store = reader.readNextRowGroup();
    while (store != null) {
      writer.startBlock(store.getRowCount());
      BlockMetaData blockMetaData = meta.getBlocks().get(blockIndex);
      List<ColumnChunkMetaData> columnsInOrder = blockMetaData.getColumns();
      Map<ColumnPath, ColumnDescriptor> descriptorsMap = schema.getColumns().stream().collect(
        Collectors.toMap(x -> ColumnPath.get(x.getPath()), x -> x));
      long blockUncompressedSize = 0;
      for (int i = 0; i < columnsInOrder.size(); i += 1) {
        ColumnChunkMetaData chunk = columnsInOrder.get(i);
        blockUncompressedSize += chunk.getTotalUncompressedSize();
        ColumnReadStoreImpl crstore = new ColumnReadStoreImpl(store, new DumpGroupConverter(), schema, createdBy);
        ColumnDescriptor columnDescriptor = descriptorsMap.get(chunk.getPath());
        writer.startColumn(columnDescriptor, crstore.getColumnReader(columnDescriptor).getTotalValueCount(), codecName);
        translatePages(reader, writer, chunk, createdBy, codecName);
        writer.endColumn();
      }
      writer.setCurrentBlockSize(blockUncompressedSize);
      writer.endBlock();
      store = reader.readNextRowGroup();
      blockIndex++;
    }
  }

  private void translatePages(ParquetFileReader reader, ParquetFileWriter writer, ColumnChunkMetaData chunk,
                              String createdBy, CompressionCodecName codecName) throws IOException {
    CompressionCodecFactory codecFactory = HadoopCodecs.newFactory(0);
    BytesInputDecompressor decompressor = codecFactory.getDecompressor(chunk.getCodec());
    BytesInputCompressor compressor = codecFactory.getCompressor(codecName);
    ColumnIndex columnIndex = reader.readColumnIndex(chunk);

    SeekableInputStream f = reader.getInputStream();
    f.seek(chunk.getStartingPos());

    DictionaryPage dictionaryPage = null;
    long valueCount = 0;
    Statistics statistics = null;
    int pageIndex = 0;
    while (valueCount < chunk.getValueCount()) {
      PageHeader pageHeader = Util.readPageHeader(f);
      int compressedPageSize = pageHeader.getCompressed_page_size();
      byte[] data = new byte[compressedPageSize];
      f.readFully(data);
      byte[] newData;
      Encoding encoding;
      switch (pageHeader.type) {
        case DICTIONARY_PAGE:
          if (dictionaryPage != null) {
            throw new IOException(  "has more than one dictionary page in column chunk");
          }
          newData = translate(data, pageHeader, compressor, decompressor);
          encoding = Encoding.valueOf(pageHeader.dictionary_page_header.getEncoding().name());
          writer.writeDictionaryPage(new DictionaryPage(BytesInput.from(newData), pageHeader.getCompressed_page_size(), encoding));
          break;
        case DATA_PAGE:
          valueCount += pageHeader.getData_page_header().getNum_values();
          newData = translate(data, pageHeader, compressor, decompressor);
          encoding = Encoding.valueOf(pageHeader.data_page_header.getEncoding().name());
          statistics = convertStatistics(createdBy, chunk.getPrimitiveType(), pageHeader.data_page_header.getStatistics(), columnIndex, pageIndex);
          long rowCount = valueCount;
          writer.writeDataPage((int) valueCount, pageHeader.uncompressed_page_size, BytesInput.from(newData),
            statistics, rowCount, encoding, encoding, encoding);
          pageIndex++;
          break;
        case DATA_PAGE_V2:
          valueCount += pageHeader.getData_page_header_v2().getNum_values();
          newData = translate(data, pageHeader, compressor, decompressor);
          encoding = Encoding.valueOf(pageHeader.data_page_header_v2.getEncoding().name());
          statistics = convertStatistics(createdBy, chunk.getPrimitiveType(), pageHeader.data_page_header_v2.getStatistics(), columnIndex, pageIndex);
          writer.writeDataPage((int)valueCount, pageHeader.uncompressed_page_size, BytesInput.from(newData),
            statistics, valueCount, encoding, encoding, encoding);
          pageIndex++;
          break;
        case INDEX_PAGE:
          LOG.warn("Unrecognized page type: {}", pageHeader.type);
          break;
        default:
          // TODO: null pages??? should we increase pageIndex when it is null page
          // TODO: bloom filters //Index Page
          LOG.warn("Unrecognized page type: {}", pageHeader.type);
          break;
      }
    }
  }

  private Statistics convertStatistics(String createdBy, PrimitiveType type, org.apache.parquet.format.Statistics pageStatistics,
                                       ColumnIndex columnIndex, int pageIndex) throws IOException {
    ParquetMetadataConverter converter = new ParquetMetadataConverter();
    if (pageStatistics != null) {
      return converter.fromParquetStatistics(createdBy, pageStatistics, type);
    } else if (columnIndex != null) {
      if (columnIndex.getNullPages() == null) {
        throw new IOException("columnIndex has null variable 'nullPages' which indicates corrupted data for type: " +  type.getName());
      }
      if (pageIndex > columnIndex.getNullPages().size()) {
        throw new IOException("There are more pages " + pageIndex + " found in the column than in the columnIndex " + columnIndex.getNullPages().size());
      }
      org.apache.parquet.column.statistics.Statistics.Builder statsBuilder = org.apache.parquet.column.statistics.Statistics.getBuilderForReading(type);
      statsBuilder.withNumNulls(columnIndex.getNullCounts().get(pageIndex));

      if (!columnIndex.getNullPages().get(pageIndex)) {
        statsBuilder.withMin(columnIndex.getMinValues().get(pageIndex).array().clone());
        statsBuilder.withMax(columnIndex.getMaxValues().get(pageIndex).array().clone());
      }
      return statsBuilder.build();
    } else {
      return null;
    }
  }

  private byte[] translate(byte[] data, PageHeader header, BytesInputCompressor compressor, BytesInputDecompressor decompressor) throws IOException {
    if (isCompressed(header)) {
      BytesInput input = BytesInput.from(data);
      int uncompressedSize = header.getUncompressed_page_size();
      BytesInput rawData = decompressor.decompress(input, uncompressedSize);
      data = rawData.toByteArray();
    }
    BytesInput bytesInput = BytesInput.from(data);
    BytesInput newCompressedData = compressor.compress(bytesInput);
    return newCompressedData.toByteArray();
  }

  private boolean isCompressed(PageHeader header) {
    return header.uncompressed_page_size != header.compressed_page_size;
  }

  private static final class DumpGroupConverter extends GroupConverter {
      @Override public void start() {}
      @Override public void end() {}
      @Override public Converter getConverter(int fieldIndex) { return new DumpConverter(); }
  }

  private static final class DumpConverter extends PrimitiveConverter {
      @Override public GroupConverter asGroupConverter() { return new DumpGroupConverter(); }
  }
}
