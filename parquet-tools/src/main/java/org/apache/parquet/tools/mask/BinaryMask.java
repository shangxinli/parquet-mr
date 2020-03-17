package org.apache.parquet.tools.mask;

import org.apache.parquet.io.api.Binary;

import javax.xml.bind.DatatypeConverter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class BinaryMask extends Mask {
  private final Mask.Method maskMethod;

  public BinaryMask(Mask.Method maskMethod) throws NoSuchAlgorithmException {
    super();
    this.maskMethod = maskMethod;
  }

  @Override
  public Object mask(Object original) {
    if (original instanceof Binary) {
      throw new RuntimeException("The value to be masked is not " + Binary.class.getName());
    }
    if (maskMethod.getRule().equals(Rule.FLAT)) {
      if (maskMethod.equals(Method.NULL)) {
        return null;
      } else {
        throw new RuntimeException("No such rule" + maskMethod.getRule());
      }
    } else {
      byte[] src = ((Binary) original).getBytes();
      MessageDigest md = maskMethod.equals(Method.SHA256) ? sha256MD : md5MD;
      // TODO: assume always start 0, and update in place. Deepcopy first?
      md.update(src, 0, src.length);
      byte[] hash = DatatypeConverter.printHexBinary(md.digest()).getBytes(StandardCharsets.UTF_8);
      return Binary.fromConstantByteArray(hash);
    }
  }
}
