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
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.log4j.Logger;

public class IndexSignature{
  private static Logger log = Logger.getLogger(IndexSignature.class);
  
    private String  _version;                     // current version
    public IndexSignature(String version){
      _version = version;
    }   
    
    public void updateVersion(String version)
    {
      _version = version;
    }

    public String getVersion()
    {
      return _version;
    }

    public void save(OutputStream out) throws IOException
    {
      BufferedWriter writer = null;
      try
      {
        writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
        //writer.write(String.valueOf(_version));
        //System.out.println("IndexSignature:save:_version" + _version);
        if(_version == null)
        {
          //System.out.println("IndexSignature:save:_version: write null");
          writer.write("null");
        }
        else
        {
          //System.out.println("IndexSignature:save:_version: write " + _version);
          writer.write(_version);          
        }
        writer.flush();
      } catch(IOException e)
      {
        log.error(e);
      } finally
      {
        if (writer!=null) writer.close();
      }
    }
    
  public static IndexSignature read(InputStream in) throws IOException
  {
    BufferedReader reader = new BufferedReader(new InputStreamReader(in, "UTF-8"));
    String line = null;
    try
    {
      line = reader.readLine();
    } catch (IOException e)
    {
      log.error(e);
    } finally
    {
      reader.close();
    }
    if (line != null && !line.equals("null"))
    {
      return new IndexSignature(line);
    }
    else
    {
      return new IndexSignature(null);
    }
  }
}
