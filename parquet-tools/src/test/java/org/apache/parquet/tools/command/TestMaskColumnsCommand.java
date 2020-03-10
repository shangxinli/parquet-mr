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
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.junit.Assert.assertEquals;

public class TestMaskColumnsCommand {

  private final int numRecord = 5;
  private MaskColumnsCommand command = new MaskColumnsCommand();
  private DumpCommand dumpCommand = new DumpCommand();
  private Configuration conf = new Configuration();

  @Test
  public void testMaskOneColumn() throws Exception {
    // Create Parquet file
    String inputFile = createParquetFile("input");
    String outputFile = createTempFile("output");

    // Remove column
    String cargs[] = {inputFile, outputFile, "DocId"};
    executeCommandLine(cargs);

    String dumpCargs[] = {outputFile};
    CommandLineParser parser = new PosixParser();
    CommandLine cmdLine = parser.parse(new Options(), dumpCargs, dumpCommand.supportsExtraArgs());
    dumpCommand.execute(cmdLine);
  }

  private void executeCommandLine(String[] cargs) throws Exception {
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(new Options(), cargs, command.supportsExtraArgs());
    command.execute(cmd);
  }

  private void validateColumns(String inputFile, List<String> prunePaths) throws IOException {
    ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), new Path(inputFile)).withConf(conf).build();
    for (int i = 0; i < numRecord; i++) {
      Group group = reader.read();
      if (!prunePaths.contains("DocId")) {
        assertEquals(1l, group.getLong("DocId", 0));
      }
      if (!prunePaths.contains("Name")) {
        assertEquals("foo", group.getBinary("Name", 0).toStringUsingUTF8());
      }

      /*
      if (!prunePaths.contains("Gender")) {
        assertEquals("male", group.getBinary("Gender", 0).toStringUsingUTF8());
      }

      if (!prunePaths.contains("Links")) {
        Group subGroup = group.getGroup("Links", 0);
        if (!prunePaths.contains("Links.Backward")) {
          assertEquals(2l, subGroup.getLong("Backward", 0));
        }
        if (!prunePaths.contains("Links.Forward")) {
          assertEquals(3l, subGroup.getLong("Forward", 0));
        }
      } */

    }
    reader.close();
  }

  private String createParquetFile(String prefix) throws IOException {
    MessageType schema = new MessageType("schema",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "Name")
      /*,
      new PrimitiveType(REQUIRED, BINARY, "Gender"),
      new GroupType(OPTIONAL, "Links",
        new PrimitiveType(REPEATED, INT64, "Backward"),
        new PrimitiveType(REPEATED, INT64, "Forward"))*/);

    conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());

    String file = createTempFile(prefix);
    ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(file)).withConf(conf);
    try (ParquetWriter writer = builder.build()) {
      for (int i = 0; i < numRecord; i++) {
        SimpleGroup g = new SimpleGroup(schema);
        g.add("DocId", 1l);
        g.add("Name", "foo");
/*        g.add("Gender", "male");
        Group links = g.addGroup("Links");
        links.add(0, 2l);
        links.add(1, 3l); */
        writer.write(g);
      }
    }

    return file;
  }

  private static String createTempFile(String prefix) {
    try {
      return Files.createTempDirectory(prefix).toAbsolutePath().toString() + "/test.parquet";
    } catch (IOException e) {
      throw new AssertionError("Unable to create temporary file", e);
    }
  }
}
