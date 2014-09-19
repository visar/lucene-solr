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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

// TODO: merge with PagedBytes, except PagedBytes doesn't
// let you read while writing which FST needs

class BytesStore extends DataOutput implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
        RamUsageEstimator.shallowSizeOfInstance(BytesStore.class)
      + RamUsageEstimator.shallowSizeOfInstance(ArrayList.class);

  private final List<ByteBuffer> blocks = new ArrayList<>();

  private final int blockSize;
  private final int blockBits;
  private final int blockMask;

  private ByteBuffer current;

  @SuppressWarnings("FieldCanBeLocal")
  private final boolean allocateDirect = true;

  public BytesStore(int blockBits) {
    this.blockBits = blockBits;
    blockSize = 1 << blockBits;
    blockMask = blockSize-1;
  }

  private void addBlock() {
    //noinspection ConstantConditions
    current = (allocateDirect ? ByteBuffer.allocateDirect(blockSize) : ByteBuffer.allocate(blockSize));
    blocks.add(current);
  }

  /** Pulls bytes from the provided IndexInput.  */
  public BytesStore(DataInput in, long numBytes, int maxBlockSize) throws IOException {
    int blockSize = 2;
    int blockBits = 1;
    while(blockSize < numBytes && blockSize < maxBlockSize) {
      blockSize *= 2;
      blockBits++;
    }
    this.blockBits = blockBits;
    this.blockSize = blockSize;
    this.blockMask = blockSize-1;
    long left = numBytes;
    while(left > 0) {
      final int chunk = (int) Math.min(blockSize, left);
      // todo: optimize this
      byte[] block = new byte[chunk];
      in.readBytes(block, 0, block.length);
      blocks.add(ByteBuffer.wrap(block));
      left -= chunk;
    }
    if (! blocks.isEmpty()) {
      current = blocks.get(blocks.size() - 1);
      current.position(current.limit());
    }
  }

  /** Absolute write byte; you must ensure dest is < max
   *  position written so far. */
  public void writeByte(int dest, byte b) {
    int blockIndex = dest >> blockBits;
    ByteBuffer block = blocks.get(blockIndex);
    block.put(dest & blockMask, b);
  }

  private int safePos(ByteBuffer buffer) {
    return (buffer == null ? blockSize : buffer.position());
  }

  @Override
  public void writeByte(byte b) {
    if (safePos(current) == blockSize) {
      addBlock();
    }
    current.put(b);
  }

  @Override
  public void writeBytes(byte[] b, int offset, int len) {
    while (len > 0) {
      int chunk = blockSize - safePos(current);
      if (len <= chunk) {
        current.put(b, offset, len);
        break;
      } else {
        if (chunk > 0) {
          current.put(b, offset, chunk);
          offset += chunk;
          len -= chunk;
        }
        addBlock();
      }
    }
  }

  int getBlockBits() {
    return blockBits;
  }

  /** Absolute writeBytes without changing the current
   *  position.  Note: this cannot "grow" the bytes, so you
   *  must only call it on already written parts. */
  void writeBytes(long dest, byte[] b, int offset, int len) {
    //System.out.println("  BS.writeBytes dest=" + dest + " offset=" + offset + " len=" + len);
    assert dest + len <= getPosition(): "dest=" + dest + " pos=" + getPosition() + " len=" + len;

    // Note: weird: must go "backwards" because copyBytes
    // calls us with overlapping src/dest.  If we
    // go forwards then we overwrite bytes before we can
    // copy them:

    final long end = dest + len;
    int blockIndex = (int) (end >> blockBits);
    int downTo = (int) (end & blockMask);
    if (downTo == 0) {
      blockIndex--;
      downTo = blockSize;
    }
    ByteBuffer block = blocks.get(blockIndex);

    int lastPos = (block == current ? current.position() : -1);
    try {
      while (len > 0) {
        //System.out.println("    cycle downTo=" + downTo + " len=" + len);
        if (len <= downTo) {
          //System.out.println("      final: offset=" + offset + " len=" + len + " dest=" + (downTo-len));
          block.position(downTo - len);
          block.put(b, offset, len);
          break;
        } else {
          len -= downTo;
          //System.out.println("      partial: offset=" + (offset + len) + " len=" + downTo + " dest=0");
          block.position(0);
          block.put(b, offset + len, downTo);
          blockIndex--;
          block = blocks.get(blockIndex);
          downTo = blockSize;
        }
      }
    } finally {
      if (lastPos >= 0) current.position(lastPos);
    }
  }

  private byte[] byteBufferToArray(ByteBuffer buffer) {
    byte[] copy = new byte[buffer.limit()];

    int curPos = (buffer == current ? buffer.position() : -1);
    try {
      buffer.position(0);
      buffer.get(copy);
    } finally {
      if (curPos >= 0) current.position(curPos);
    }

    return copy;
  }

  // todo: optimize this
  /** Absolute copy bytes self to self, without changing the
   *  position. Note: this cannot "grow" the bytes, so must
   *  only call it on already written parts. */
  public void copyBytes(long src, long dest, int len) {
    //System.out.println("BS.copyBytes src=" + src + " dest=" + dest + " len=" + len);
    assert src < dest;

    // Note: weird: must go "backwards" because copyBytes
    // calls us with overlapping src/dest.  If we
    // go forwards then we overwrite bytes before we can
    // copy them:

    long end = src + len;

    int blockIndex = (int) (end >> blockBits);
    int downTo = (int) (end & blockMask);
    if (downTo == 0) {
      blockIndex--;
      downTo = blockSize;
    }
    ByteBuffer block = blocks.get(blockIndex);

    while (len > 0) {
      //System.out.println("  cycle downTo=" + downTo);
      if (len <= downTo) {
        //System.out.println("    finish");
        writeBytes(dest, byteBufferToArray(block), downTo-len, len);
        break;
      } else {
        //System.out.println("    partial");
        len -= downTo;
        writeBytes(dest + len, byteBufferToArray(block), 0, downTo);
        blockIndex--;
        block = blocks.get(blockIndex);
        downTo = blockSize;
      }
    }
  }

  /** Writes an int at the absolute position without
   *  changing the current pointer. */
  public void writeInt(long pos, int value) {
    int blockIndex = (int) (pos >> blockBits);
    int upto = (int) (pos & blockMask);
    ByteBuffer block = blocks.get(blockIndex);
    int shift = 24;
    for(int i=0;i<4;i++) {
      block.put(upto++, (byte) (value >> shift));
      shift -= 8;
      if (upto == blockSize) {
        upto = 0;
        blockIndex++;
        block = blocks.get(blockIndex);
      }
    }
  }

  /** Reverse from srcPos, inclusive, to destPos, inclusive. */
  public void reverse(long srcPos, long destPos) {
    assert srcPos < destPos;
    assert destPos < getPosition();
    //System.out.println("reverse src=" + srcPos + " dest=" + destPos);

    int srcBlockIndex = (int) (srcPos >> blockBits);
    int src = (int) (srcPos & blockMask);
    ByteBuffer srcBlock = blocks.get(srcBlockIndex);

    int destBlockIndex = (int) (destPos >> blockBits);
    int dest = (int) (destPos & blockMask);
    ByteBuffer destBlock = blocks.get(destBlockIndex);
    //System.out.println("  srcBlock=" + srcBlockIndex + " destBlock=" + destBlockIndex);

    int limit = (int) (destPos - srcPos + 1)/2;
    for(int i=0;i<limit;i++) {
      //System.out.println("  cycle src=" + src + " dest=" + dest);
      byte b = srcBlock.get(src);
      srcBlock.put(src, destBlock.get(dest));
      destBlock.put(dest, b);
      src++;
      if (src == blockSize) {
        srcBlockIndex++;
        srcBlock = blocks.get(srcBlockIndex);
        //System.out.println("  set destBlock=" + destBlock + " srcBlock=" + srcBlock);
        src = 0;
      }

      dest--;
      if (dest == -1) {
        destBlockIndex--;
        destBlock = blocks.get(destBlockIndex);
        //System.out.println("  set destBlock=" + destBlock + " srcBlock=" + srcBlock);
        dest = blockSize-1;
      }
    }
  }

  public void skipBytes(int len) {
    while (len > 0) {
      int chunk = blockSize - safePos(current);
      if (len <= chunk) {
        current.position(current.position() + len);
        break;
      } else {
        len -= chunk;
        addBlock();
      }
    }
  }

  public long getPosition() {
    return ((long) blocks.size()-1) * blockSize + safePos(current);
  }

  /** Pos must be less than the max position written so far!
   *  Ie, you cannot "grow" the file with this! */
  public void truncate(long newLen) {
    assert newLen <= getPosition();
    assert newLen >= 0;
    int blockIndex = (int) (newLen >> blockBits);
    int nextWrite = (int) (newLen & blockMask);
    if (nextWrite == 0) {
      blockIndex--;
      nextWrite = blockSize;
    }
    blocks.subList(blockIndex+1, blocks.size()).clear();
    if (newLen == 0) {
      current = null;
    } else {
      current = blocks.get(blockIndex);
      current.position(nextWrite);
    }
    assert newLen == getPosition();
  }

  public void finish() {
    if (current != null) {
      // todo: is this needed?
      current.limit(current.position());
      current = null;
    }
  }

  /** Writes all of our bytes to the target {@link DataOutput}. */
  public void writeTo(DataOutput out) throws IOException {
    for(ByteBuffer block : blocks) {
      byte[] blockArray = byteBufferToArray(block);
      out.writeBytes(blockArray, 0, blockArray.length);
    }
  }

  public FST.BytesReader getForwardReader() {
    if (blocks.size() == 1) {
      return new ForwardBytesReader(blocks.get(0));
    }
    return new FST.BytesReader() {
      private ByteBuffer current;
      private int nextBuffer;

      @Override
      public byte readByte() {
        if (safePos(current) == blockSize) {
          current = blocks.get(nextBuffer++).asReadOnlyBuffer();
          current.position(0);
        }
        return current.get();
      }

      @Override
      public void skipBytes(long count) {
        setPosition(getPosition() + count);
      }

      @Override
      public void readBytes(byte[] b, int offset, int len) {
        while(len > 0) {
          int chunkLeft = blockSize - safePos(current);
          if (len <= chunkLeft) {
            current.get(b, offset, len);
            break;
          } else {
            if (chunkLeft > 0) {
              current.get(b, offset, chunkLeft);
              offset += chunkLeft;
              len -= chunkLeft;
            }
            current = blocks.get(nextBuffer++).asReadOnlyBuffer();
            current.position(0);
          }
        }
      }

      @Override
      public long getPosition() {
        return ((long) nextBuffer-1)*blockSize + safePos(current);
      }

      @Override
      public void setPosition(long pos) {
        int bufferIndex = (int) (pos >> blockBits);
        nextBuffer = bufferIndex+1;
        current = blocks.get(bufferIndex).asReadOnlyBuffer();
        current.position((int) (pos & blockMask));
        assert getPosition() == pos;
      }

      @Override
      public boolean reversed() {
        return false;
      }
    };
  }

  public FST.BytesReader getReverseReader() {
    return getReverseReader(true);
  }

  FST.BytesReader getReverseReader(boolean allowSingle) {
    if (allowSingle && blocks.size() == 1) {
      return new ReverseBytesReader(blocks.get(0));
    }
    return new FST.BytesReader() {
      private ByteBuffer current = blocks.size() == 0 ? null : blocks.get(0).asReadOnlyBuffer();
      private int nextBuffer = -1;
      private int nextRead = 0;

      @Override
      public byte readByte() {
        if (nextRead == -1) {
          current = blocks.get(nextBuffer--).asReadOnlyBuffer();
          nextRead = blockSize-1;
        }
        return current.get(nextRead--);
      }

      @Override
      public void skipBytes(long count) {
        setPosition(getPosition() - count);
      }

      @Override
      public void readBytes(byte[] b, int offset, int len) {
        for(int i=0;i<len;i++) {
          b[offset+i] = readByte();
        }
      }

      @Override
      public long getPosition() {
        return ((long) nextBuffer+1)*blockSize + nextRead;
      }

      @Override
      public void setPosition(long pos) {
        // NOTE: a little weird because if you
        // setPosition(0), the next byte you read is
        // bytes[0] ... but I would expect bytes[-1] (ie,
        // EOF)...?
        int bufferIndex = (int) (pos >> blockBits);
        nextBuffer = bufferIndex-1;
        current = blocks.get(bufferIndex).asReadOnlyBuffer();
        nextRead = (int) (pos & blockMask);
        assert getPosition() == pos: "pos=" + pos + " getPos()=" + getPosition();
      }

      @Override
      public boolean reversed() {
        return true;
      }
    };
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES_USED;
    for (ByteBuffer block : blocks) {
      size += block.capacity();
    }
    return size;
  }
  
  @Override
  public Iterable<? extends Accountable> getChildResources() {
    return Collections.emptyList();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(numBlocks=" + blocks.size() + ")";
  }
}
