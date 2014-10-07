package org.apache.lucene.util.fst;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// TODO: can we use just ByteArrayDataInput...?  need to
// add a .skipBytes to DataInput.. hmm and .setPosition

import java.nio.ByteBuffer;

/** Reads from a single ByteBuffer. */
final class ForwardBytesReader extends FST.BytesReader {
  private final ByteBuffer buffer;

  public ForwardBytesReader(ByteBuffer buffer) {
    this.buffer = (ByteBuffer) buffer.asReadOnlyBuffer().position(0);
  }

  @Override
  public byte readByte() {
    return buffer.get();
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) {
    buffer.get(b, offset, len);
  }

  @Override
  public void skipBytes(long count) {
    buffer.position(buffer.position() + (int) count);
  }

  @Override
  public long getPosition() {
    return buffer.position();
  }

  @Override
  public void setPosition(long pos) {
    buffer.position((int) pos);
  }

  @Override
  public boolean reversed() {
    return false;
  }
}
