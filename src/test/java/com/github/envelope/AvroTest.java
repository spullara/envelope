package com.github.envelope;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import static junit.framework.Assert.assertEquals;

/**
 * Experiment with Avro a bit.
 */
public class AvroTest {
  @Test
  public void testSize() throws IOException {
    EncoderFactory ef = new EncoderFactory();
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    BinaryEncoder be = ef.binaryEncoder(out, null);
    SpecificDatumWriter<Frame> frameWriter = new SpecificDatumWriter<>(Frame.class);
    frameWriter.write(new Frame(0l, 0, -1, new HashMap<CharSequence, CharSequence>(), ByteBuffer.allocate(0)), be);
    be.flush();
    assertEquals(5, out.toByteArray().length);
  }
}
