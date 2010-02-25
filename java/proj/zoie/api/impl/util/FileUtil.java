package proj.zoie.api.impl.util;
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
import java.io.File;
import java.io.IOException;

public class FileUtil {
	/**
	   * utility method to delete a directory
	   * @param dir
	   * @throws IOException
	   */
	  private static void deleteDir(File dir)
	  {
	    if (dir == null) return;
	    
	    if (dir.isDirectory())
	    {
	      File[] files=dir.listFiles();
	      for (File file : files)
	      {
	        deleteDir(file);
	      }
	      dir.delete();
	    }
	    else
	    {
	      dir.delete();
	    }
	  }

	  /**
	   * Purges an index
	   */
	  public static void rmDir(File location)
	  {
	    String name=location.getName()+"-"+System.currentTimeMillis();
	    File parent=location.getParentFile();
	    File tobeDeleted=new File(parent,name);
	    location.renameTo(tobeDeleted);
	    // try to delete the files, ok if it fails, this is just for testing
	    deleteDir(tobeDeleted);
	  }
}
