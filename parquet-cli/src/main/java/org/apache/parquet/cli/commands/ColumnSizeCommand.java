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

package org.apache.parquet.cli.commands;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.avro.file.SeekableInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.util.Formats;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Parameters(commandDescription="Print the column sizes of a parquet file")
public class ColumnSizeCommand extends BaseCommand {

  public ColumnSizeCommand(Logger console) {
    super(console);
  }

  @Parameter(description = "<parquet path>")
  List<String> targets;

  @Override
  @SuppressWarnings("unchecked")
  public int run() throws IOException {
    Preconditions.checkArgument(targets != null && targets.size() >= 1,
      "A Parquet file is required.");

    Path inputFile = new Path(targets.get(0));

    Map<String, Long> columnSizes = getColumnSizeInBytes(inputFile);
    Map<String, Float> columnPercentage = getColumnPercentage(columnSizes);

    if (targets.size() > 1) {
      for (String inputColumn : targets.subList(1, targets.size())) {
        long size = 0;
        float percentage = 0;
        for (String column : columnSizes.keySet()) {
          if (column.startsWith(inputColumn)) {
            size += columnSizes.get(column);
            percentage += columnPercentage.get(column);
          }
        }
        console.info(inputColumn + "->" + " Size In Bytes: " + size + " Size In Percentage: " + percentage);
      }
    } else {
      for (String column : columnSizes.keySet()) {
        console.info(column + "->" + " Size In Bytes: " + columnSizes.get(column)
          + " Size In Percentage: " + columnPercentage.get(column));
      }
    }

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Print every column size in byte and percentage for a Parquet file",
        "sample.parquet",
        "sample.parquet col_1 col_2",
        "sample.parquet col_1 col_2.sub_col_a"
    );
  }

  // Make it public to allow some automation tools to call it
  public Map<String, Long> getColumnSizeInBytes(Path inputFile) throws IOException {
    Map<String, Long> colSizes = new HashMap<>();
    ParquetMetadata pmd = ParquetFileReader.readFooter(new Configuration(), inputFile, ParquetMetadataConverter.NO_FILTER);

    for (BlockMetaData block : pmd.getBlocks()) {
      for (ColumnChunkMetaData column : block.getColumns()) {
        String colName = column.getPath().toDotString();
        colSizes.put(colName, column.getTotalSize() + colSizes.getOrDefault(colName, 0L));
      }
    }

    return colSizes;
  }

  // Make it public to allow some automation tools to call it
  public Map<String, Float> getColumnPercentage(Map<String, Long> colSizes) {
    long totalSize = colSizes.values().stream().reduce(0L, Long::sum);
    Map<String, Float> colPercentage = new HashMap<>();

    for (Map.Entry<String, Long> entry : colSizes.entrySet()) {
      colPercentage.put(entry.getKey(), ((float) entry.getValue()) / ((float) totalSize));
    }

    return colPercentage;
  }
}