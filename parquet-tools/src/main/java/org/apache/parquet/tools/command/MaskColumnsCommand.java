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
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
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

import java.io.IOException;
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

public class MaskColumnsCommand extends ArgsOnlyCommand {
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

  public MaskColumnsCommand() {
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
    Map<ColumnPath, Mask.Method> columnMaskMap = new HashMap<>();
    for (int i = 2; i < args.size(); i += 2) {
      columnMaskMap.put(ColumnPath.fromDotString(args.get(i)), Mask.Method.valueOf(args.get(i + 1)));
    }

    //Set<ColumnPath> maskPaths = convertToColumnPaths(cols);
    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inpath, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();
    HadoopInputFile inputFile = HadoopInputFile.fromPath(inpath, conf);
    ParquetReadOptions readOptions = HadoopReadOptions.builder(conf).build();
    try(ParquetFileReader reader = new ParquetFileReader(inputFile, readOptions)) {
      ParquetFileWriter writer = new ParquetFileWriter(conf, schema, outpath, ParquetFileWriter.Mode.CREATE);
      writer.start();
      mask(writer, reader, metaData, schema, columnMaskMap);
      writer.end(metaData.getFileMetaData().getKeyValueMetaData());
    }
  }

  public static void mask(final ParquetFileWriter writer, final ParquetFileReader reader, final ParquetMetadata meta,
                          final MessageType schema, final Map<ColumnPath, Mask.Method> columnMaskMap) throws IOException {
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
        ColumnPath path = chunk.getPath();
        para.setSize(para.getSize() + chunk.getTotalUncompressedSize());

        if (columnMaskMap.containsKey(path)) {
          maskColumnChunk(writer, store, meta, schema, descriptorsMap, path, chunk);
        } else {
          appendColumnChunk(writer, reader.getInputStream(), para, columnsInOrder, i);
        }
      }

      writer.setCurrentBlockSize(para.getSize());
      writer.endBlock();

      store = reader.readNextRowGroup();
      blockIndex++;
    }
  }

  private static void appendColumnChunk(final ParquetFileWriter writer, final SeekableInputStream from, final ParquetFileWriter.StreamPara para,
                                        final List<ColumnChunkMetaData> columnsInOrder, final int columnIndex) throws IOException {
    writer.appendColumnChunk(from, para, columnsInOrder, columnIndex);
  }

  private static void maskColumnChunk(final ParquetFileWriter writer,final PageReadStore store, final ParquetMetadata meta,
                                      final MessageType schema, final Map<ColumnPath, ColumnDescriptor> descriptorsMap,
                                      final ColumnPath path,  final ColumnChunkMetaData chunk,
                                      final Map<ColumnPath, Mask.Method> columnMaskMap) throws IOException  {
    ColumnReadStoreImpl crstore = new ColumnReadStoreImpl(store, new DumpGroupConverter(),
      schema, meta.getFileMetaData().getCreatedBy());
    ColumnDescriptor columnDescriptor = descriptorsMap.get(path);
    int dmax = columnDescriptor.getMaxDefinitionLevel();
    ColumnReader creader = crstore.getColumnReader(columnDescriptor);
    long totalValues = creader.getTotalValueCount();

    writer.startColumn(columnDescriptor, totalValues, chunk.getCodec());

    if (columnMaskMap.get(path).equals(Mask.Method.NULL)) {
      //TODO:
      //writer.writeDataPage(1, totalValues, null, EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
    } else {
      for (long j = 0; j < totalValues; ++j) {
        maskField(writer, creader, dmax, columnDescriptor);
        creader.consume();
      }
    }

    writer.endColumn();
  }

  private static void maskField(final ParquetFileWriter writer, final ColumnReader creader, final int dmax,
                                 final ColumnDescriptor columnDescriptor) throws IOException {
    int rlvl = creader.getCurrentRepetitionLevel();
    int dlvl = creader.getCurrentDefinitionLevel();
    int len = 0;

    if (dlvl == dmax) {
      switch (columnDescriptor.getType()) {
        case FIXED_LEN_BYTE_ARRAY:
        case INT96:
        case BINARY:
          //out.print(stringifier.stringify(creader.getBinary()));
          Binary data = creader.getBinary();
          writer.writeDataPage(1, data.length(), BytesInput.from(data.getBytes()), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
          break;
        case BOOLEAN:
          //out.print(stringifier.stringify(creader.getBoolean()));
          break;
        case DOUBLE:
          //out.print(stringifier.stringify(creader.getDouble()));
          break;
        case FLOAT:
          //out.print(stringifier.stringify(creader.getFloat()));
          break;
        case INT32:
          //out.print(stringifier.stringify(creader.getInteger()));
          break;
        case INT64:
          //out.print(stringifier.stringify(creader.getLong()));
          Long longData = creader.getLong();
          writer.writeDataPage(1, 8, BytesInput.from(BytesUtils.longToBytes(longData)), EMPTY_STATS, BIT_PACKED, BIT_PACKED, PLAIN);
          break;
      }
    } else {
    }
  }

  private Set<ColumnPath> convertToColumnPaths(List<String> cols) {
    Set<ColumnPath> prunePaths = new HashSet<>();
    for (String col : cols) {
      prunePaths.add(ColumnPath.fromDotString(col));
    }
    return prunePaths;
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
