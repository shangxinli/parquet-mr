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
