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
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.HadoopReadOptions;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTransCompressionCommand {
  
  private TransCompressionCommand command = new TransCompressionCommand();
  private Configuration conf = new Configuration();

  @Test
  public void testTransCompression() throws Exception {
    String[] codecs = {"UNCOMPRESSED", "SNAPPY", "GZIP", "ZSTD"};
    for (int i = 0; i < codecs.length; i++) {
      for (int j = 0; j <codecs.length; j++) {
        // Same codec for both are considered as valid test case
        testInternal(codecs[i], codecs[j]);
      }
    }
  }

  @Test
  public void testSpeed() throws Exception {
    compairWithRecordWrite("GZIP", "ZSTD");
  }

  @Test
  public void testLocalFile() throws Exception {
    String inputFile = "/Users/shangx/test_zstd_hudi.parquet";
    String outputFile = createTempFile("output_trans");
    String cargs[] = {inputFile, outputFile, "ZSTD"};
    executeCommandLine(cargs);
  }

  private void testInternal(String srcCodec, String destCodec) throws Exception {
    int numRecord = 1000;
    String inputFile = createParquetFile("input", srcCodec, numRecord);
    String outputFile = createTempFile("output_trans");
    String cargs[] = {inputFile, outputFile, destCodec};
    executeCommandLine(cargs);
    validateColumns(inputFile, numRecord);
    validMeta(inputFile, outputFile);
  }

  private void compairWithRecordWrite(String srcCodec, String destCodec) throws Exception {
    String inputFile = createParquetFile("input", srcCodec, 100000);
    String outputFile = createTempFile("output_trans");
    String cargs[] = {inputFile, outputFile, destCodec};

    long start = System.currentTimeMillis();
    executeCommandLine(cargs);
    long durationTrans = System.currentTimeMillis() - start;

    outputFile = createTempFile("output_record");
    start = System.currentTimeMillis();
    convertRowLevel(conf, CompressionCodecName.valueOf(destCodec), new Path(inputFile), new Path(outputFile));
    long durationRecord = System.currentTimeMillis() - start;

    Assert.assertTrue(durationTrans < durationRecord);
  }

  private void convertRowLevel(Configuration conf, CompressionCodecName codecName, Path inpath, Path outpath) throws Exception {
    ParquetMetadata metaData = ParquetFileReader.readFooter(conf, inpath, NO_FILTER);
    MessageType schema = metaData.getFileMetaData().getSchema();
    HadoopInputFile inputFile = HadoopInputFile.fromPath(inpath, conf);
    ParquetReadOptions readOptions = HadoopReadOptions.builder(conf).build();

    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(outpath).withConf(conf).withCompressionCodec(codecName);

    ParquetWriter parquetWriter = builder.build();

    PageReadStore pages;
    ParquetFileReader reader = new ParquetFileReader(inputFile, readOptions);

    while ((pages = reader.readNextRowGroup()) != null) {
      long rows = pages.getRowCount();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

      for (int i = 0; i < rows; i++) {
        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
        parquetWriter.write(simpleGroup);
      }
    }

    parquetWriter.close();
  }

  private void validateColumns(String inputFile, int numRecord) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(inputFile)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      assertTrue(group.getLong("DocId", 0) < 1000);
      assertEquals(group.getBinary("Name", 0).length(), 100);
      assertEquals(group.getBinary("Gender", 0).length(), 100);
      Group subGroup = group.getGroup("Links", 0);
      assertEquals(subGroup.getBinary("Backward", 0).length(), 100);
      assertEquals(subGroup.getBinary("Forward", 0).length(), 100);
    }
    reader.close();
  }

  private void validMeta(String inputFile, String outFile) throws Exception {
    ParquetMetadata inMetaData = ParquetFileReader.readFooter(conf, new Path(inputFile), NO_FILTER);
    ParquetMetadata outMetaData = ParquetFileReader.readFooter(conf, new Path(outFile), NO_FILTER);
    Assert.assertEquals(inMetaData.getFileMetaData().getSchema(), outMetaData.getFileMetaData().getSchema());
    Assert.assertEquals(inMetaData.getFileMetaData().getKeyValueMetaData(), outMetaData.getFileMetaData().getKeyValueMetaData());
  }

  private void executeCommandLine(String[] cargs) throws Exception {
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(new Options(), cargs, command.supportsExtraArgs());
    command.execute(cmd);
  }

  private String createParquetFile(String prefix, String codec, int numRecord) throws IOException {
    MessageType schema = new MessageType("schema",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "Name"),
      new PrimitiveType(REQUIRED, BINARY, "Gender"),
      new GroupType(OPTIONAL, "Links",
        new PrimitiveType(REPEATED, BINARY, "Backward"),
        new PrimitiveType(REPEATED, BINARY, "Forward")));

    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());

    String file = createTempFile(prefix);
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(file)).withConf(conf).withCompressionCodec(CompressionCodecName.valueOf(codec));
    try (ParquetWriter writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("DocId", getLong());
        g.add("Name", getString());
        g.add("Gender", getString());
        Group links = g.addGroup("Links");
        links.add(0, getString());
        links.add(1, getString());
        writer.write(g);
      }
    }

    return file;
  }

  private static long getLong() {
    return ThreadLocalRandom.current().nextLong(1000);
  }

  private static String getString() {
    char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'x', 'z', 'y'};
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      sb.append(chars[new Random().nextInt(10)]);
    }
    return sb.toString();
  }


  private static String createTempFile(String prefix) {
    try {
      return Files.createTempDirectory(prefix).toAbsolutePath().toString() + "/test.parquet";
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }
}
