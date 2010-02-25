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
import org.apache.log4j.Logger;

/**
 * The thread handling indexing in background. Such thread reports UncaughtExceptions automatically.
 */
public class IndexingThread extends Thread
{
  private static final Logger log = Logger.getLogger(IndexingThread.class);
  private static final Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler()
  {
    public void uncaughtException(Thread thread, Throwable t)
    {
      log.error(thread.getName() + " is abruptly terminated", t);
    }
  };
  
  public IndexingThread(String name)
  {
    super(name);
    this.setUncaughtExceptionHandler(exceptionHandler);
  }
}
