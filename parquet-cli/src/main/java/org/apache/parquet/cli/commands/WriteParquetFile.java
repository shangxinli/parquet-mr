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
import com.google.common.collect.Lists;


import org.apache.parquet.cli.BaseCommand;
import org.apache.parquet.cli.util.Codecs;
import org.apache.parquet.crypto.ColumnEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

@Parameters(commandDescription="Generate a parquet file")
public class WriteParquetFile extends BaseCommand {

  public WriteParquetFile(Logger console) {
    super(console);
  }

  static final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  static Random rnd = new Random();
  static     MessageDigest digest;

  static String randomString(int len ){
    StringBuilder sb = new StringBuilder( len );
    for( int i = 0; i < len; i++ ) 
      sb.append( AB.charAt( rnd.nextInt(AB.length()) ) );
    return sb.toString();
  }

  static String intSha256(int num, MessageDigest digest) {
    byte[] num_bytes = ByteBuffer.allocate(4).putInt(num).array();
    byte[] hash = digest.digest(num_bytes);
    return Base64.getEncoder().encodeToString(hash);
  }

  static class Car {
    int id;
    int drivers[];
    int speeding;
    String id_hash;
    String driver_id_hash;
    Car(int driver_id) {
      id = rnd.nextInt(10000000);
      drivers = new int[2];
      drivers[0] = driver_id;
      drivers[1] = rnd.nextInt(1000000000);
      speeding = rnd.nextInt(50);
      id_hash = intSha256(id, digest);
      driver_id_hash = intSha256(drivers[0], digest);
    }
  }


  @Parameter(
      names={"-o", "--output"},
      description="Output file path",
      required=true)
  String outputPath = null;


  @Parameter(names = {"--compression-codec"},
      description = "A compression codec name.")
  String compressionCodecName = "SNAPPY";

  @Parameter(names="--row-group-size", description="Target row group size")
  int rowGroupSize = ParquetWriter.DEFAULT_BLOCK_SIZE;

  @Parameter(names="--page-size", description="Target page size")
  int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE;

  @Parameter(names="--dictionary-size", description="Max dictionary page size")
  int dictionaryPageSize = ParquetWriter.DEFAULT_PAGE_SIZE;


  @Parameter(names={"-n", "--number-of-lines"}, description="N lines")
  int numLines = 1000;

  @Parameter(names={"-e", "--encrypted-file"},
      description="Convert to an encrypted Parquet file")
  boolean encrypt = false;

  @Parameter(names={"-u"},
      description="Uniform")
  boolean uniform = false;
  
  @Parameter(names={"-a"},
      description="Algorithm")
  String algo = "AES_GCM_V1";

  @Override
  public int run() throws IOException {
    try {
      digest = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      return -1;
    }
    
    console.info("Algo "+algo);


    int[] driver_ids = {237857628, 832743239, 987634975, 895723475, 239872389, 237462937, 231895723, 293873928, 546749783, 967348973};

    int num_cars = 10;
    Car cars[] = new Car[num_cars];
    for (int i=0; i < num_cars; i++) {
      cars[i] = new Car(driver_ids[i]);
    }
    cars[0].speeding = 0; // set careful driver


    FileEncryptionProperties eSetup = null;

    if (encrypt) {
      byte[] keyBytes = new byte[16];
      for (byte i=0; i < 16; i++) {keyBytes[i] = i;}
      HashMap<ColumnPath, ColumnEncryptionProperties> columnMD = null;

      if (uniform) {
        console.info("Uniform encryption");
      }
      else {
        console.info("Non-Uniform encryption");
        columnMD = new HashMap<ColumnPath, ColumnEncryptionProperties>();
        
        byte[] colKeyBytes = new byte[16]; 
        for (byte i=0; i < 16; i++) {colKeyBytes[i] = (byte)(i*2);}
        ColumnEncryptionProperties encCol = ColumnEncryptionProperties.builder("CarID").withKey(colKeyBytes).withKeyID("kc").build();
        columnMD.put(encCol.getPath(), encCol);
        
        byte[] colKeyBytes1 = new byte[16];
        for (byte i=0; i < 16; i++) {colKeyBytes1[i] = (byte)(i*3);}
        encCol = ColumnEncryptionProperties.builder("DriverID").withKey(colKeyBytes1).withKeyID("kc1").build();
        columnMD.put(encCol.getPath(), encCol);
      }

      byte[] aad = outputPath.getBytes(StandardCharsets.UTF_8);

      eSetup = FileEncryptionProperties.builder(keyBytes)
          .withAlgorithm(ParquetCipher.valueOf(algo))
          .withFooterKeyID("kf")
          .withAADPrefix(aad)
          .withEncryptedColumns(columnMD)
          .build();
    }

    Configuration conf = getConf();

    MessageType schema = parseMessageType(
        "message test { "
            + "required int32 CarID; "
            + "required int32 DriverID; "
            + "required int64 Timestamp; "
            + "required binary CarIDHash; "
            + "required binary DriverIDHash; "
            + "required double Latitude; "
            + "required double Longitude; "
            + "required int32 Speed; "
            + "required int32 SpeedLimit; "
            + "required int32 TransmissionGearPosition; "
            + "required int32 AcceleratorPedalPosition; "
            + "required boolean BrakePedalStatus; "
            + "required int64 Odometer; "
            + "required int32 FuelLevel; "
            + "required int32 EngineSpeed; "
            + "required boolean HeadlampStatus; "
            + "required boolean HighBeamStatus; "
            + "required int32 DoorStatus; "
            + "required boolean WindshieldWiperStatus; "
            + "required binary CarServiceMessage; "
            + "} ");
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory f = new SimpleGroupFactory(schema);

    Path file = new Path(outputPath);

    ParquetWriter<Group> writer = new ParquetWriter<Group>(
        file,
        new GroupWriteSupport(),
        Codecs.parquetCodec(compressionCodecName), 
        rowGroupSize, 
        pageSize, 
        dictionaryPageSize, 
        true, 
        false, 
        ParquetWriter.DEFAULT_WRITER_VERSION, conf, 
        eSetup);

    long timestamp_millis = Timestamp.valueOf("2018-09-01 00:00:00").getTime();

    for (int i = 0; i < numLines; i++) {
      timestamp_millis += 10;


      Car car = cars[rnd.nextInt(num_cars)];

      double latt = 50.0 * rnd.nextDouble();
      double longt = 50.0 * rnd.nextDouble();

      int speed_limit = 90;

      int speed = speed_limit + (int)(rnd.nextFloat() * car.speeding/100.0 * speed_limit);

      String gear_lever_position = "drive";

      int transmission_gear_position = rnd.nextInt(7);

      int accelerator_pedal_position = rnd.nextInt(100);

      long odometer = 5 * rnd.nextInt(1000000); // nextLong produces negatives

      int fuel_level = rnd.nextInt(100);

      int engine_speed = rnd.nextInt(8000);

      boolean headlamp_status = true;

      boolean high_beam_status = rnd.nextBoolean();

      int door_status = rnd.nextInt(7); // 0: all closed. 1: driver's open 5: baggage open. 6: more than 1 door open.

      boolean brake_pedal_status = rnd.nextBoolean();

      boolean parking_brake_status = false;

      boolean windshield_wiper_status = rnd.nextBoolean();

      String car_service_message = "ABCDjshfsdjihfsdhfbscnsdljsdkl"+i;


      writer.write(
          f.newGroup()
          .append("CarID", car.id)
          .append("DriverID", car.drivers[0])
          .append("Timestamp", timestamp_millis)
          .append("CarIDHash", car.id_hash)
          .append("DriverIDHash", car.driver_id_hash)
          .append("Latitude", latt)
          .append("Longitude", longt)
          .append("Speed", speed)
          .append("SpeedLimit", speed_limit)
          .append("TransmissionGearPosition", transmission_gear_position)
          .append("AcceleratorPedalPosition", accelerator_pedal_position)
          .append("BrakePedalStatus", brake_pedal_status)
          .append("Odometer", odometer)
          .append("FuelLevel", fuel_level)
          .append("EngineSpeed", engine_speed)
          .append("HeadlampStatus", headlamp_status)
          .append("HighBeamStatus", high_beam_status)
          .append("DoorStatus", door_status)
          .append("WindshieldWiperStatus", windshield_wiper_status)
          .append("CarServiceMessage", car_service_message)
          );
    }
    writer.close();

    return 0;
  }

  @Override
  public List<String> getExamples() {
    return Lists.newArrayList(
        "# Create a Parquet file"
        );
  }
}
