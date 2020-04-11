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
package org.apache.parquet.tools.mask;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public abstract class Mask {

  public enum Rule {
    FLAT("flat"),
    IDENTITY("identity");

    Rule(String name) {
      this.name = name;
    }

    private final String name;

    public String getName() {
      return name;
    }
  }

  public enum Method {
    NULL("null", Rule.FLAT, "Replace the field with null value"),
    SHA256("sha256", Rule.IDENTITY, "Replace the field with hash value with hash method SHA256"),
    MD5("md5", Rule.IDENTITY, "Replace the field with hash value with hash method MD5");

    private final String name;
    private final Rule rule;
    private final String notes;

    Method(String name, Rule rule, String notes) {
      this.name = name;
      this.rule = rule;
      this.notes = notes;
    }

    public String getName() {
      return this.name;
    }

    public Rule getRule() {
      return this.rule;
    }

    public String getNotes() {
      return this.notes;
    }
  }

  //TODO: make it singleton
  protected static MessageDigest sha256MD;
  protected static MessageDigest md5MD;

  public Mask() throws NoSuchAlgorithmException  {
    this.sha256MD = MessageDigest.getInstance("SHA-256");
    this.md5MD = MessageDigest.getInstance("MD5");
  }

  public abstract Object mask(Object original);
}
