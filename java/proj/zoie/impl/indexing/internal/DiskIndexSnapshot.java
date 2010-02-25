
package proj.zoie.impl.indexing.internal;
/**
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;

import proj.zoie.api.DirectoryManager;
import proj.zoie.api.impl.util.ChannelUtil;
import proj.zoie.impl.indexing.internal.ZoieIndexDeletionPolicy.Snapshot;

/**
 * @author ymatsuda
 *
 */
public class DiskIndexSnapshot
{
  private DirectoryManager _dirMgr;
  private IndexSignature _sig;
  private Snapshot _snapshot;
  
  public DiskIndexSnapshot(DirectoryManager dirMgr, IndexSignature sig, Snapshot snapshot)
  {
    _dirMgr = dirMgr;
    _sig = sig;
    _snapshot = snapshot;
  }
  
  public void close()
  {
    _snapshot.close();
  }
  
  public DirectoryManager getDirecotryManager()
  {
    return  _dirMgr;
  }
  
  public long writeTo(WritableByteChannel channel) throws IOException
  {
    // format:
    //   <format_version> <sig_len> <sig_data> { <idx_file_name_len> <idx_file_name> <idx_file_len> <idx_file_data> }...
    
    long amount = 0;
    
    // format version
    amount += ChannelUtil.writeInt(channel, 1);
    
    // index signature
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    _sig.save(baos);
    byte[] sigBytes = baos.toByteArray();
    
    amount += ChannelUtil.writeLong(channel, (long)sigBytes.length); // data length
    amount += channel.write(ByteBuffer.wrap(sigBytes)); // data

    // index files
    Collection<String> fileNames = _snapshot.getFileNames();
    amount += ChannelUtil.writeInt(channel, fileNames.size()); // number of files
    for(String fileName : fileNames)
    {
      amount += ChannelUtil.writeString(channel, fileName);
      amount += _dirMgr.transferFromFileToChannel(fileName, channel);
    }
    return amount;
  }
  
  public static void readSnapshot(ReadableByteChannel channel, DirectoryManager dirMgr) throws IOException
  {
    // format version
    int formatVersion = ChannelUtil.readInt(channel);
    if(formatVersion != 1)
    {
      throw new IOException("snapshot format version mismatch [" + formatVersion + "]");
    }
    
    // index signature
    if(!dirMgr.transferFromChannelToFile(channel, DirectoryManager.INDEX_DIRECTORY))
    {
      throw new IOException("bad snapshot file");
    }

    // index files
    int numFiles = ChannelUtil.readInt(channel); // number of files
    if(numFiles < 0)
    {
      throw new IOException("bad snapshot file");      
    }
    while(numFiles-- > 0)
    {
      String fileName = ChannelUtil.readString(channel);
      if(fileName == null)
      {
        throw new IOException("bad snapshot file");
      }
      if(!dirMgr.transferFromChannelToFile(channel, fileName))
      {
        throw new IOException("bad snapshot file");
      }
    }
  }
}
