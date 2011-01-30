package proj.zoie.impl.indexing;
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
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Index;
import org.apache.lucene.document.Field.Store;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexable;
import proj.zoie.api.indexing.ZoieIndexableInterpreter;

public class FileIndexableInterpreter implements ZoieIndexableInterpreter<File> 
{
	protected static int id = 0;
	private static final Logger log = Logger.getLogger(FileIndexableInterpreter.class);
	static ThreadLocal<StringBuilder> myStringBuilder = new ThreadLocal<StringBuilder>();
	static ThreadLocal<char[]> myCharBuffer = new ThreadLocal<char[]>();
	protected class FileIndexable extends AbstractZoieIndexable
	{
		private File _file;
		private int _uid;
		private FileIndexable(File file, int uid)
		{
			_file=file;
			_uid = uid;
		}
		
		public IndexingReq[] buildIndexingReqs(){
			IndexingReq req = new IndexingReq(buildDocument(),null);
			return new IndexingReq[]{req};
		}
		
		public Document buildDocument() 
		{
			Document doc=new Document();
			doc.add(new Field("foo","bar",Store.NO,Index.NOT_ANALYZED_NO_NORMS));
			StringBuilder sb= myStringBuilder.get();
			char[] cb = myCharBuffer.get();
			if (sb == null || cb==null)
			{
			  sb = new StringBuilder(15500);
			  sb.ensureCapacity(15500);
			  myStringBuilder.set(sb);
			  cb = new char[15500];
			  myCharBuffer.set(cb);
			}
			sb.append(_file.getAbsoluteFile()).append("\n");
			doc.add(new Field("path",_file.getAbsolutePath(),Store.YES,Index.ANALYZED));
			FileReader freader=null;
			try
			{
				freader=new FileReader(_file);
				BufferedReader br=new BufferedReader(freader);
				int len = br.read(cb, 0, cb.length);
				int start = 0;
				int end = 0;
				while(end<len)
				{
				  if (cb[end] == '\n' || cb[end] == '\r')
				  {
				    sb.append(cb, start, end-start).append('\n');
				    start = end;
				    while(start< len && (cb[start] == '\n' || cb[start] == '\r'))
				    {
				      start++;
				    }
				    end = start;
				  }
				  end ++;
				}
			}
			catch(Exception e)
			{
				log.error(e);
			}
			finally
			{
				if (freader!=null)
				{
					try {
						freader.close();
					} catch (IOException e) {
						log.error(e);
					}
				}
			}
			doc.add(new Field("content",sb.toString(),Store.YES,Index.ANALYZED));
			sb.setLength(0);
			return doc;
		}
		
		public boolean isSkip()
		{
			return false;
		}
		
		public boolean isDeleted()
		{
			return false;
		}

		public long getUID() 
		{
			return _uid;
		}
	}
	
	public ZoieIndexable convertAndInterpret(File src) {
		ZoieIndexable idxable = new FileIndexable(src, id);
		id++;
		return idxable;
	}
}
