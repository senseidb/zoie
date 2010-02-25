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
import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Stack;

import proj.zoie.api.DataConsumer.DataEvent;

public class FileDataProvider extends StreamDataProvider<File>
{
	private final File _dir;
	private long _currentVersion;
	private Stack<Iterator<File>> _stack;
	private Iterator<File> _currentIterator;
	private boolean _looping;
	
	public FileDataProvider(File dir)
	{
		super();
		if (!dir.exists())
			throw new IllegalArgumentException("dir: "+dir+" does not exist.");
		_dir=dir;
		_stack=new Stack<Iterator<File>>();
		_looping = false;
		reset();
	}
	
	public File getDir()
	{
		return _dir;
	}

	@Override
	public void reset()
	{
		_stack.clear();
		if (_dir.isFile())
		{
			_currentIterator=Arrays.asList(new File[]{_dir}).iterator();
		}
		else
		{
			_currentIterator=Arrays.asList(_dir.listFiles()).iterator();
		}
	}
	
	public void setLooping(boolean looping){
		_looping = looping;
	}
	
	@Override
	public DataEvent<File> next() {
		if(_currentIterator.hasNext())
		{
			File next=_currentIterator.next();
			if (next.isFile())
			{
				return new DataEvent<File>(_currentVersion++,next);
			}
			else
			{
				_stack.push(_currentIterator);
				_currentIterator=Arrays.asList(next.listFiles()).iterator();
				return next();
			}
		}
		else
		{
			if (_stack.isEmpty())
			{
				if (_looping){
					reset();
					return next();
				}
				else{
				  return null;
				}
			}
			else
			{
				_currentIterator=_stack.pop();
				return next();
			}
		}
	}
}
